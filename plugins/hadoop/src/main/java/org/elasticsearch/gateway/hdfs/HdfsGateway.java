/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gateway.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.blobstore.hdfs.HdfsBlobStore;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.DynamicExecutors;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.gateway.blobstore.BlobStoreGateway;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * @author kimchy (shay.banon)
 */
public class HdfsGateway extends BlobStoreGateway {

    private final boolean closeFileSystem;

    private final FileSystem fileSystem;

    private final ExecutorService concurrentStreamPool;

    @Inject public HdfsGateway(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                               ClusterName clusterName) throws IOException {
        super(settings, threadPool, clusterService);

        this.closeFileSystem = componentSettings.getAsBoolean("close_fs", true);
        String uri = componentSettings.get("uri");
        if (uri == null) {
            throw new ElasticSearchIllegalArgumentException("hdfs gateway requires the 'uri' setting to be set");
        }
        String path = componentSettings.get("path");
        if (path == null) {
            throw new ElasticSearchIllegalArgumentException("hdfs gateway requires the 'path' path setting to be set");
        }
        Path hPath = new Path(new Path(path), clusterName.value());

        int concurrentStreams = componentSettings.getAsInt("concurrent_streams", 5);
        this.concurrentStreamPool = DynamicExecutors.newScalingThreadPool(1, concurrentStreams, TimeValue.timeValueSeconds(5).millis(), EsExecutors.daemonThreadFactory(settings, "[s3_stream]"));

        logger.debug("Using uri [{}], path [{}], concurrent_streams [{}]", uri, hPath, concurrentStreams);

        Configuration conf = new Configuration();
        Settings hdfsSettings = settings.getByPrefix("hdfs.conf.");
        for (Map.Entry<String, String> entry : hdfsSettings.getAsMap().entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }

        fileSystem = FileSystem.get(URI.create(uri), conf);

        initialize(new HdfsBlobStore(settings, fileSystem, concurrentStreamPool, hPath), clusterName, null);
    }

    @Override public String type() {
        return "hdfs";
    }

    @Override public Class<? extends Module> suggestIndexGateway() {
        return HdfsIndexGatewayModule.class;
    }

    @Override protected void doClose() throws ElasticSearchException {
        super.doClose();
        if (closeFileSystem) {
            try {
                fileSystem.close();
            } catch (IOException e) {
                // ignore
            }
        }
        concurrentStreamPool.shutdown();
    }
}
