/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.cache.alluxio;

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.cache.CacheFactory;
import com.facebook.presto.cache.CacheManager;
import com.facebook.presto.cache.CachingFileSystem;
import com.facebook.presto.cache.basic.BasicCachingFileSystem;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;

import static com.facebook.presto.cache.CacheType.ALLUXIO;

public class HiveCacheFactory
        implements CacheFactory
{
    @Override
    public CachingFileSystem createCachingFileSystem(Configuration configuration, URI uri,
            ExtendedFileSystem fileSystem, CacheManager cacheManager, CacheConfig cacheConfig)
    {
        if (cacheConfig.isCachingEnabled() && cacheConfig.getCacheType() == ALLUXIO) {
            return new AlluxioCachingFileSystem(configuration, uri, fileSystem, this);
        }
        return new BasicCachingFileSystem(
            uri,
            configuration,
            cacheManager,
            fileSystem,
            cacheConfig.isValidationEnabled());
    }
}
