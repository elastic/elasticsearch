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

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.MetadataUpgrader;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link GatewayMetaState} constructor accepts a lot of arguments.
 * It's not always easy / convenient to construct these dependencies.
 * This class constructor takes far fewer dependencies and constructs usable {@link GatewayMetaState} with 2 restrictions:
 * no metadata upgrade will be performed and no cluster state updaters will be run. This is sufficient for most of the tests.
 */
public class MockGatewayMetaState extends GatewayMetaState {
    private final DiscoveryNode localNode;

    public MockGatewayMetaState(DiscoveryNode localNode) {
        this.localNode = localNode;
    }

    @Override
    Metadata upgradeMetadataForNode(
            Metadata metadata, MetadataIndexUpgradeService metadataIndexUpgradeService,
            MetadataUpgrader metadataUpgrader) {
        // Metadata upgrade is tested in GatewayMetaStateTests, we override this method to NOP to make mocking easier
        return metadata;
    }

    @Override
    ClusterState prepareInitialClusterState(TransportService transportService, ClusterService clusterService, ClusterState clusterState) {
        // Just set localNode here, not to mess with ClusterService and IndicesService mocking
        return ClusterStateUpdaters.setLocalNode(clusterState, localNode);
    }

    public void start(Settings settings, NodeEnvironment nodeEnvironment, NamedXContentRegistry xContentRegistry) {
        final TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(mock(ThreadPool.class));
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings())
            .thenReturn(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        final MetaStateService metaStateService = mock(MetaStateService.class);
        try {
            when(metaStateService.loadFullState()).thenReturn(new Tuple<>(Manifest.empty(), Metadata.builder().build()));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        start(settings, transportService, clusterService, metaStateService,
            null, null, new PersistedClusterStateService(nodeEnvironment, xContentRegistry, BigArrays.NON_RECYCLING_INSTANCE,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L));
    }
}
