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

import alluxio.conf.AlluxioConfiguration;

import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

public class HiveCacheFactory
        implements CacheFactory
{
    @Override
    public ExtendedFileSystem createCachingFileSystem(Configuration configuration, URI uri, FileSystem fileSystem)
    {
        return new AlluxioCachingFileSystem(configuration, uri, fileSystem, this);
    }

    @Override
    public AlluxioCachingClientFileSystem getAlluxioCachingClientFileSystem(FileSystem fileSystem, AlluxioConfiguration alluxioConfiguration)
    {
        return new AlluxioCachingClientFileSystem(fileSystem, alluxioConfiguration);
    }
}
