/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.gateway;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.MetaDataUpgrader;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class Zen1GatewayMetaState extends GatewayMetaState implements ClusterStateApplier {

    private boolean incrementalWrite;

    public Zen1GatewayMetaState(Settings settings, NodeEnvironment nodeEnv, MetaStateService metaStateService,
                                MetaDataIndexUpgradeService metaDataIndexUpgradeService, MetaDataUpgrader metaDataUpgrader,
                                TransportService transportService) throws IOException {
        super(settings, nodeEnv, metaStateService, metaDataIndexUpgradeService, metaDataUpgrader, transportService);
        incrementalWrite = false;
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        if (isMasterOrDataNode() == false) {
            return;
        }

        if (event.state().blocks().disableStatePersistence()) {
            incrementalWrite = false;
            return;
        }

        try {
            updateClusterState(event.state(), event.previousState(), incrementalWrite);
            incrementalWrite = true;
        } catch (WriteStateException e) {
            logger.warn("Exception occurred when storing new meta data", e);
        }
    }


}
