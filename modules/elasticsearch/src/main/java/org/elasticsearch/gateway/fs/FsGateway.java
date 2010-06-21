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

package org.elasticsearch.gateway.fs;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.blobstore.BlobStoreGateway;
import org.elasticsearch.index.gateway.fs.FsIndexGatewayModule;

import java.io.File;
import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class FsGateway extends BlobStoreGateway {

    @Inject public FsGateway(Settings settings, Environment environment, ClusterName clusterName) throws IOException {
        super(settings);

        File gatewayFile;
        String location = componentSettings.get("location");
        if (location == null) {
            logger.warn("using local fs location for gateway, should be changed to be a shared location across nodes");
            gatewayFile = new File(environment.workFile(), "gateway");
        } else {
            gatewayFile = new File(location);
        }
        initialize(new FsBlobStore(componentSettings, gatewayFile), clusterName);
    }

    @Override public String type() {
        return "fs";
    }

    @Override public Class<? extends Module> suggestIndexGateway() {
        return FsIndexGatewayModule.class;
    }
}
