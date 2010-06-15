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

package org.elasticsearch.index.gateway.fs;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.gateway.fs.FsGateway;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexException;
import org.elasticsearch.index.gateway.IndexGateway;
import org.elasticsearch.index.gateway.IndexShardGateway;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.Strings;

import java.io.File;

import static org.elasticsearch.common.io.FileSystemUtils.*;

/**
 * @author kimchy (Shay Banon)
 */
public class FsIndexGateway extends AbstractIndexComponent implements IndexGateway {

    private final String location;

    private File indexGatewayHome;

    @Inject public FsIndexGateway(Index index, @IndexSettings Settings indexSettings, Environment environment, Gateway gateway) {
        super(index, indexSettings);

        String location = componentSettings.get("location");
        if (location == null) {
            if (gateway instanceof FsGateway) {
                indexGatewayHome = new File(new File(((FsGateway) gateway).gatewayHome(), "indices"), index.name());
            } else {
                indexGatewayHome = new File(new File(new File(environment.workWithClusterFile(), "gateway"), "indices"), index.name());
            }
            location = Strings.cleanPath(indexGatewayHome.getAbsolutePath());
        } else {
            indexGatewayHome = new File(new File(location), index.name());
        }
        this.location = location;

        if (!(indexGatewayHome.exists() && indexGatewayHome.isDirectory())) {
            boolean result;
            for (int i = 0; i < 5; i++) {
                result = indexGatewayHome.mkdirs();
                if (result) {
                    break;
                }
            }
        }
        if (!(indexGatewayHome.exists() && indexGatewayHome.isDirectory())) {
            throw new IndexException(index, "Failed to create index gateway at [" + indexGatewayHome + "]");
        }
    }

    @Override public Class<? extends IndexShardGateway> shardGatewayClass() {
        return FsIndexShardGateway.class;
    }

    @Override public void close(boolean delete) {
        if (!delete) {
            return;
        }
        try {
            String[] files = indexGatewayHome.list();
            if (files == null || files.length == 0) {
                deleteRecursively(indexGatewayHome, true);
            }
        } catch (Exception e) {
            // ignore
        }
    }

    public File indexGatewayHome() {
        return this.indexGatewayHome;
    }
}
