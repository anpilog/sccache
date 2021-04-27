// Copyright 2016 Shawn Hice <shawnhice@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::cache::{Cache, CacheRead, CacheWrite, Storage};
use crate::errors::*;
use futures_03::prelude::*;
use redis::{cmd, InfoDict, Client, RedisError, RedisResult, ConnectionLike};
use redis::aio::Connection;
use std::collections::HashMap;
use std::io::Cursor;
use std::time::{Duration, Instant};

// For displaying slices of String Vecs
use crate::config::{SliceDisplay};
use futures_03::future::{try_join_all};

/// An Alternative to `RedisClusterCache` that will implement Async `Redis::Client` in a thread pool
/// Each Client is resolved and printed independently
#[derive(Clone, Debug)]
pub struct RedisClientPool {
    urls: Vec<String>,
    client_map: HashMap<String, Client>,
    // this isn't really necessary but for repeated transactions life would be easier
    // connections: HashMap<String, Connection>
}

impl RedisClientPool {
    /// Creates a new `RedisClientPool` by constructing `redis::Client` objects from all URLs
    pub fn new(urls: &Vec<String>) -> Result<RedisClientPool> {
        Ok({
            let mut client_map: HashMap<String, Client> = HashMap::new();

            for url in urls {
                client_map.insert(url.to_owned(), Client::open(url.to_owned()).unwrap());
            }

            RedisClientPool {
                urls: urls.to_owned(),
                client_map: client_map.to_owned()
            }
        })
    }

    /// Connects to all of the various Clients being used
    async fn connect_all(self) -> core::result::Result<Vec<Connection>, RedisError> {
        // Cannot directly map from an iterator, this is because the impl trait in direct typing isn't enabled by default
        let mut future_objects = Vec::new();

        for client in self.client_map.values(){
            //debug!("Adding {} to connection list", _url);
            future_objects.push(client.get_async_connection());
        }

        return try_join_all(future_objects).await;
    }

    async fn connect_first(self) -> core::result::Result<(Connection, String), Error> {
        for (url, client) in self.client_map.iter(){
            match client.get_async_connection().await {
                Ok(c) => {
                    if client.to_owned().check_connection() {
                        debug!("Connected successfully to : {}", url);
                        return Ok((c, url.to_owned()));
                    }else{
                        error!("Connection to {} is open but cannot Ping - trying next", url);
                    }
                },
                Err(e) => {
                    debug!("Failed to connect to {} due to {:?}", url, e);
                }
            }
        }
        Err(Error::msg("Cannot find a valid connection in SCCACHE_REDIS_CLUSTER"))
    }
}

impl Storage for RedisClientPool {
    // copied over from the basic redis implementation for testing

    fn get(&self, key: &str) -> SFuture<Cache> {
        let key = key.to_owned();
        let me = self.clone();
        Box::new(
            Box::pin(async move {
                let mut conn = me.connect_first().await?;
                let d: RedisResult<Vec<u8>> = cmd("GET")
                    .arg(key.to_owned())
                    .query_async(&mut conn.0)
                    .await;

                match d {
                    Ok(res) => {
                        if res.is_empty() {
                            debug!("Cache miss on {} for {:?}", conn.1, key);
                            Ok(Cache::Miss)
                        } else {
                            CacheRead::from(Cursor::new(res)).map(Cache::Hit)
                        }
                    },
                    Err(e) => {
                        error!("Failed to get {} from {} because {}", key, conn.1, e);
                        Ok(Cache::Miss)
                    }
                }


            })
                .compat(),
        )
    }

    /// Open a connection and store a object in the cache.
    fn put(&self, key: &str, entry: CacheWrite) -> SFuture<Duration> {
        let key = key.to_owned();
        let me = self.clone();
        let start = Instant::now();
        Box::new(
            Box::pin(async move {
                let connections = me.connect_all().await?;
                let d = entry.finish()?;

                debug!("Putting values into reddis clusters : {}" , key);

                for mut conn in connections {
                    // could try to get errors for this - probably not worth it
                    cmd("SET")
                        .arg(key.to_owned())
                        .arg(d.to_owned())
                        .query_async(&mut conn)
                        .await?;
                }

                Ok(start.elapsed())
            })
                .compat(),
        )
    }

    /// Returns the cache location.
    fn location(&self) -> String {
        format!("Redis: {}", SliceDisplay(&self.urls))
    }

    /// Returns the current cache size. This value is aquired via
    /// the Redis INFO command (used_memory).
    // #[instrument]
    fn current_size(&self) -> SFuture<Option<u64>> {
        let me = self.clone(); // TODO Remove clone
        Box::new(
            Box::pin(async move {
                let mut c_ = me.connect_first().await?;
                let v : RedisResult<InfoDict> = cmd("INFO").query_async(&mut c_.0).await;
                match v {
                    Ok(dict_) => {
                        let size : Option<u64> = dict_.get("used_memory");
                        debug!("Current Size from {} : {:?}", c_.1, size);
                        Ok(size)
                    },
                    Err(e) => {
                        error!("Error while getting current_size from {} : {:?}", c_.1, e);
                        Ok(None)
                    }
                }

            })
                .compat(),
        )
    }

    /// Returns the maximum cache size. This value is read via
    /// the Redis CONFIG command (maxmemory). If the server has no
    /// configured limit, the result is None.
    fn max_size(&self) -> SFuture<Option<u64>> {
        let me = self.clone(); // TODO Remove clone
        Box::new(
            Box::pin(
                async move {
                    let mut c_ = me.connect_first().await?;

                    let result: redis::RedisResult<HashMap<String, usize>> = cmd("CONFIG")
                        .arg("GET")
                        .arg("maxmemory")
                        .query_async(&mut c_.0)
                        .await;

                    match result {
                        Ok(h) => {
                            let size: Option<&usize> = h.get("maxmemory");
                            debug!("Max Size from {} : {:?}", c_.1, size);
                            Ok(size.and_then(|&s| if s != 0 { Some(s as u64) } else { None }))
                        }
                        Err(e) => {
                            error!("Error occurred during max_size from {} : {:?}", c_.1, e);
                            Ok(None)
                        },
                    }
                }
            )
                .compat(),
        )
    }
}
