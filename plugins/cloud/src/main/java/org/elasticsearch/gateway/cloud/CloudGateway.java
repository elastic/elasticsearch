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

package org.elasticsearch.gateway.cloud;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.cloud.blobstore.CloudBlobStore;
import org.elasticsearch.cloud.blobstore.CloudBlobStoreService;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.gateway.blobstore.BlobStoreGateway;
import org.elasticsearch.index.gateway.cloud.CloudIndexGatewayModule;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class CloudGateway extends BlobStoreGateway {

    @Inject public CloudGateway(Settings settings, ClusterName clusterName, CloudBlobStoreService blobStoreService) throws IOException {
        super(settings);

        String location = componentSettings.get("location");
        String container = componentSettings.get("container");
        if (container == null) {
            throw new ElasticSearchIllegalArgumentException("Cloud gateway requires 'container' setting");
        }

        initialize(new CloudBlobStore(settings, blobStoreService.context(), container, location), clusterName, new ByteSizeValue(100, ByteSizeUnit.MB));
    }

    @Override public String type() {
        return "cloud";
    }

    @Override public Class<? extends Module> suggestIndexGateway() {
        return CloudIndexGatewayModule.class;
    }
}

