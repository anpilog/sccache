// Copyright 2016 Mozilla Foundation
// Copyright 2016 Felix Obenhuber <felix@obenhuber.de>
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
use redis::{cmd, Client, InfoDict};
use redis::cluster::{ClusterClient, ClusterConnection};
use std::collections::HashMap;
use std::io::Cursor;
use std::time::{Duration, Instant};

// futures 0.13
use futures_03::executor::ThreadPool;

use crate::config::{SliceDisplay};

/// A cache that stores entries in a Redis.
#[derive(Clone)]
pub struct RedisClusterCacheold {
    url: String,
    client: Client,
}

/// A Async Cache of Redis connections
#[derive(Clone)]
pub struct RedisAsyncCache {
    url: String,
    pool: ThreadPool,
}

#[derive(Clone)]
pub struct RedisClusterCache {
    urls: Vec<String>,
    cluster_client: ClusterClient,
}

impl RedisClusterCache {
    /// Create a new `RedisClusterCache`.
    pub fn new(url: &Vec<String>) -> Result<RedisClusterCache> {
        // Initialize urls and cluster_client
        Ok(RedisClusterCache {
            urls: url.to_owned(),
            // does a sanity check of the nodes and passwords
            cluster_client: ClusterClient::open(url.to_owned())?,
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
