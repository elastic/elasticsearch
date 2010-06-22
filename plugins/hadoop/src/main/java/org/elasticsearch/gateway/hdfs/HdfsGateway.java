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
import org.elasticsearch.common.blobstore.hdfs.HdfsBlobStore;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.blobstore.BlobStoreGateway;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class HdfsGateway extends BlobStoreGateway {

    private final boolean closeFileSystem;

    private final FileSystem fileSystem;

    @Inject public HdfsGateway(Settings settings, ClusterName clusterName) throws IOException {
        super(settings);

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

        logger.debug("Using uri [{}], path [{}]", uri, hPath);

        Configuration conf = new Configuration();
        Settings hdfsSettings = settings.getByPrefix("hdfs.conf.");
        for (Map.Entry<String, String> entry : hdfsSettings.getAsMap().entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }

        fileSystem = FileSystem.get(URI.create(uri), conf);

        initialize(new HdfsBlobStore(settings, fileSystem, hPath), clusterName);
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
    }
}
