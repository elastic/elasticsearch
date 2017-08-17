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
package org.elasticsearch.tribe;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.single.SingleNodeDiscovery;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.tribe.TribeService.BLOCKS_METADATA_SETTING;
import static org.elasticsearch.tribe.TribeService.BLOCKS_WRITE_SETTING;
import static org.elasticsearch.tribe.TribeService.TRIBE_METADATA_BLOCK;
import static org.elasticsearch.tribe.TribeService.TRIBE_WRITE_BLOCK;

/**
 * A {@link Discovery} implementation that is used by {@link TribeService}. This implementation
 * doesn't support any clustering features. Most notably {@link #startInitialJoin()} does nothing and
 * {@link #publish(ClusterChangedEvent, AckListener)} delegates state updates directly to the
 * {@link org.elasticsearch.cluster.service.ClusterApplierService}.
 */
public class TribeDiscovery extends SingleNodeDiscovery implements Discovery {

    @Inject
    public TribeDiscovery(Settings settings, TransportService transportService,
                          MasterService masterService, ClusterApplier clusterApplier) {
        super(settings, transportService, masterService, clusterApplier);
    }

    @Override
    protected ClusterState createInitialState(DiscoveryNode localNode) {
        ClusterBlocks.Builder clusterBlocks = ClusterBlocks.builder(); // don't add no_master / state recovery block
        if (BLOCKS_WRITE_SETTING.get(settings)) {
            clusterBlocks.addGlobalBlock(TRIBE_WRITE_BLOCK);
        }
        if (BLOCKS_METADATA_SETTING.get(settings)) {
            clusterBlocks.addGlobalBlock(TRIBE_METADATA_BLOCK);
        }
        return ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(settings))
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build())
            .blocks(clusterBlocks).build();
    }

    @Override
    public synchronized void startInitialJoin() {
        // no state recovery required as tribe nodes don't persist cluster state
    }
}
