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
use redis::{cmd, InfoDict, Client, RedisResult};
use redis::cluster::{ClusterClient, ClusterConnection, cluster_pipe};
use redis::aio::Connection;
use std::collections::HashMap;
use std::io::Cursor;
use std::time::{Duration, Instant};

// For displaying slices of String Vecs
use crate::config::{SliceDisplay};
use futures_03::future::{try_join_all, ok, err};

/// A Redis Cluster Client Implementation
/// Unfortunately Cluster Connection does not implement aio:ConnectionLike - only sync
#[derive(Clone)]
pub struct RedisClusterCache {
    urls: Vec<String>,
    cluster_client: ClusterClient,
}

/// An Alternative to `RedisClusterCache` that will implement Async `Redis::Client` in a thread pool
/// Each Client is resolved and printed independently
pub struct RedisClientPool {
    urls: Vec<String>,
    client_map: HashMap<String, Client>,
    // this isn't really necessary but for repeated transactions life would be easier
    connections: HashMap<String, Connection>
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
                client_map: client_map.to_owned(),
                connections: HashMap::new(),
            }
        })
    }

    /// Connects to all of the various Clients being used
    async fn connect_all(self) -> Vec<Connection> {
        // Cannot directly map from an iterator, this is because the impl trait in direct typing isn't enabled by default
        let mut future_objects = Vec::new();

        for client in self.client_map.values(){
            future_objects.push(client.get_tokio_connection());
        }

        return try_join_all(future_objects).await.unwrap();
    }
}

impl RedisClusterCache {
    /// Create a new `RedisClusterCache`.
    pub fn new(urls: &Vec<String>) -> Result<RedisClusterCache> {
        // Initialize urls and cluster_client
        Ok(RedisClusterCache {
            urls: urls.to_owned(),
            // does a sanity check of the nodes and passwords
            cluster_client: ClusterClient::open(urls.to_owned())?,
        })
    }

    /*
    async fn connect(self) -> Result<Connection> {
        Ok(self.client.get_tokio_connection().await?)
    }
    */

    /// Returns a connection with configured read and write timeouts.
    /// This is a synchronous call instead of async, could however be made async if someone really wanted
    /// With a pool of regular Redis Clients
    ///
    /// TODO: this should only be called once and preserved with the chek_connection function
    pub fn sync_connect(self) -> Result<ClusterConnection> {
        Ok(
            // propagate the error up if this is a failure for some reason
            self.cluster_client.get_connection()?
        )
    }
}

impl Storage for RedisClusterCache {
    /// Open a connection and query for a key.
    fn get(&self, key: &str) -> SFuture<Cache> {
        let key = key.to_owned();
        let me = self.clone();
        Box::new(
            Box::pin(async move {
                // converted to sync for a cluster connection
                let mut c = me.sync_connect()?;
                // ClusterConnection implements a ConnectionLike which is already used by this
                // Oh good lord apparently it doesn't implement aio::ConnectionLike
                let d: Vec<u8> = cmd("GET").arg(key).query(&mut c)?;
                if d.is_empty() {
                    Ok(Cache::Miss)
                } else {
                    CacheRead::from(Cursor::new(d)).map(Cache::Hit)
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
                let mut c = me.sync_connect()?;
                let d = entry.finish()?;
                cmd("SET").arg(key).arg(d).query(&mut c)?;
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
    fn current_size(&self) -> SFuture<Option<u64>> {
        let me = self.clone(); // TODO Remove clone
        Box::new(
            Box::pin(async move {
                let mut c = me.sync_connect()?;
                let v: InfoDict = cmd("INFO").query(&mut c)?;
                Ok(v.get("used_memory"))
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
            Box::pin(async move {
                let mut c = me.sync_connect()?;
                // this should be using cluter_pipe() https://docs.rs/redis/0.20.0/redis/cluster/struct.ClusterPipeline.html
                // but it lacks support for pretty much every single command you use
                let result: redis::RedisResult<HashMap<String, usize>> = cmd("CONFIG")
                    .arg("GET")
                    .arg("maxmemory")
                    .query(&mut c);
                match result {
                    Ok(h) => {
                        Ok(h.get("maxmemory")
                            .and_then(|&s| if s != 0 { Some(s as u64) } else { None }))
                    }
                    Err(_) => Ok(None),
                }
            })
            .compat(),
        )
    }
}
