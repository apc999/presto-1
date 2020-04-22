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

import alluxio.AlluxioURI;
import alluxio.client.file.cache.LocalCacheFileSystem;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.hadoop.HadoopConfigurationUtils;
import alluxio.metrics.MetricsConfig;
import alluxio.metrics.MetricsSystem;
import alluxio.uri.MultiMasterAuthority;
import alluxio.uri.SingleMasterAuthority;
import alluxio.uri.ZookeeperAuthority;
import alluxio.util.ConfigurationUtils;

import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.HiveFileInfo;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AlluxioCachingFileSystem
        extends ExtendedFileSystem
{
    private alluxio.hadoop.FileSystem cachingFs;
    private final FileSystem fileSystem;
    private final Configuration configuration;
    private final CacheFactory cacheFactory;

    public AlluxioCachingFileSystem(Configuration configuration, URI uri, FileSystem fileSystem, CacheFactory cacheFactory)
    {
        this.fileSystem = fileSystem;
        this.configuration = configuration;
        this.cacheFactory = cacheFactory;
    }

    @Override
    public synchronized void initialize(URI uri, Configuration conf)
        throws IOException
    {
        super.initialize(uri, conf);

        // Take the URI properties, hadoop configuration, and given Alluxio configuration and merge
        // all three into a single object.
        AlluxioProperties alluxioProps = ConfigurationUtils.defaults();
        InstancedConfiguration newConf = HadoopConfigurationUtils.mergeHadoopConfiguration(conf,
                alluxioProps);
        // Connection details in the URI has the highest priority
        newConf.merge(getConfigurationFromUri(uri), Source.RUNTIME);

        // Handle metrics
        Properties metricsProps = new Properties();
        for (Map.Entry<String, String> e : conf) {
            metricsProps.setProperty(e.getKey(), e.getValue());
        }
        MetricsSystem.startSinksFromConfig(new MetricsConfig(metricsProps));
        LocalCacheFileSystem localCacheSystem =
            new LocalCacheFileSystem(cacheFactory.getAlluxioCachingClientFileSystem(fileSystem,
                newConf), newConf);

        this.cachingFs = new alluxio.hadoop.FileSystem(localCacheSystem);
        cachingFs.initialize(uri, conf);
    }

    private AlluxioCachingClientFileSystem getAlluxioCachingClientFileSystem(FileSystem fileSystem, AlluxioConfiguration alluxioConfiguration)
    {
        return new AlluxioCachingClientFileSystem(fileSystem, alluxioConfiguration);
    }

    @Override
    public String getScheme()
    {
        return cachingFs.getScheme();
    }

    @Override
    public URI getUri() {
        return cachingFs.getUri();
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        return cachingFs.open(f, bufferSize);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        return cachingFs.create(f, permission,
            overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        return cachingFs.append(f, bufferSize, progress);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return cachingFs.rename(src, dst);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        return cachingFs.delete(f, recursive);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        return cachingFs.listStatus(f);
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        cachingFs.setWorkingDirectory(new_dir);
    }

    @Override
    public Path getWorkingDirectory() {
        return cachingFs.getWorkingDirectory();
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        return cachingFs.mkdirs(f, permission);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        return cachingFs.getFileStatus(f);
    }

    static Map<String, Object> getConfigurationFromUri(URI uri)
    {
        AlluxioURI alluxioUri = new AlluxioURI(uri.toString());
        Map<String, Object> alluxioConfProperties = new HashMap<>();

        if (alluxioUri.getAuthority() instanceof ZookeeperAuthority) {
            ZookeeperAuthority authority = (ZookeeperAuthority) alluxioUri.getAuthority();
            alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ENABLED.getName(), true);
            alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ADDRESS.getName(),
                    authority.getZookeeperAddress());
        }
        else if (alluxioUri.getAuthority() instanceof SingleMasterAuthority) {
            SingleMasterAuthority authority = (SingleMasterAuthority) alluxioUri.getAuthority();
            alluxioConfProperties.put(PropertyKey.MASTER_HOSTNAME.getName(), authority.getHost());
            alluxioConfProperties.put(PropertyKey.MASTER_RPC_PORT.getName(), authority.getPort());
            alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ENABLED.getName(), false);
            alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ADDRESS.getName(), null);
            // Unset the embedded journal related configuration
            // to support alluxio URI has the highest priority
            alluxioConfProperties.put(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES.getName(), null);
            alluxioConfProperties.put(PropertyKey.MASTER_RPC_ADDRESSES.getName(), null);
        }
        else if (alluxioUri.getAuthority() instanceof MultiMasterAuthority) {
            MultiMasterAuthority authority = (MultiMasterAuthority) alluxioUri.getAuthority();
            alluxioConfProperties.put(PropertyKey.MASTER_RPC_ADDRESSES.getName(),
                    authority.getMasterAddresses());
            // Unset the zookeeper configuration to support alluxio URI has the highest priority
            alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ENABLED.getName(), false);
            alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ADDRESS.getName(), null);
        }
        return alluxioConfProperties;
    }

    @Override
    public FSDataInputStream openFile(Path path, HiveFileContext hiveFileContext) throws Exception {
        return cachingFs.open(path);
    }

    @Override
    public RemoteIterator<HiveFileInfo> listFiles(Path path) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listDirectory(Path path) throws IOException {
        throw new UnsupportedOperationException();
    }
}
