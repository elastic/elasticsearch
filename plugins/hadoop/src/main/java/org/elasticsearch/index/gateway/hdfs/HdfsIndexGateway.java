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

package org.elasticsearch.index.gateway.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.gateway.hdfs.HdfsGateway;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.gateway.IndexGateway;
import org.elasticsearch.index.gateway.IndexShardGateway;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class HdfsIndexGateway extends AbstractIndexComponent implements IndexGateway {

    private final FileSystem fileSystem;

    private final Path indexPath;

    @Inject public HdfsIndexGateway(Index index, @IndexSettings Settings indexSettings, Gateway gateway) {
        super(index, indexSettings);

        Path path = null;
        String pathSetting = componentSettings.get("path");
        if (pathSetting != null) {
            path = new Path(pathSetting);
        }
        if (gateway instanceof HdfsGateway) {
            HdfsGateway hdfsGateway = (HdfsGateway) gateway;
            fileSystem = hdfsGateway.fileSystem();
            if (path == null) {
                path = hdfsGateway.path();
            }
        } else {
            throw new ElasticSearchIllegalArgumentException("Must configure an hdfs gateway to use index hdfs gateway");
        }
        this.indexPath = new Path(new Path(path, "indices"), index.name());
    }

    public FileSystem fileSystem() {
        return this.fileSystem;
    }

    public Path indexPath() {
        return this.indexPath;
    }

    @Override public Class<? extends IndexShardGateway> shardGatewayClass() {
        return HdfsIndexShardGateway.class;
    }

    @Override public void close(boolean delete) throws ElasticSearchException {
        if (delete) {
            try {
                fileSystem.delete(indexPath, true);
            } catch (IOException e) {
                logger.warn("Failed to delete [{}]", e, indexPath);
            }
        }
    }
}
