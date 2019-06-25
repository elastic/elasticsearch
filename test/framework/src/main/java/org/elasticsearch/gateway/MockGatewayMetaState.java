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

import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.MetaDataUpgrader;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

import static org.mockito.Mockito.mock;

/**
 * {@link GatewayMetaState} constructor accepts a lot of arguments.
 * It's not always easy / convenient to construct these dependencies.
 * This class constructor takes far fewer dependencies and constructs usable {@link GatewayMetaState} with 2 restrictions:
 * no metadata upgrade will be performed and no cluster state updaters will be run. This is sufficient for most of the tests.
 */
public class MockGatewayMetaState extends GatewayMetaState {
    private final DiscoveryNode localNode;

    public MockGatewayMetaState(Settings settings, NodeEnvironment nodeEnvironment,
                                NamedXContentRegistry xContentRegistry, DiscoveryNode localNode) throws IOException {
        super(settings, new MetaStateService(nodeEnvironment, xContentRegistry),
                mock(MetaDataIndexUpgradeService.class), mock(MetaDataUpgrader.class),
                mock(TransportService.class), mock(ClusterService.class));
        this.localNode = localNode;
    }

    @Override
    protected void upgradeMetaData(MetaDataIndexUpgradeService metaDataIndexUpgradeService, MetaDataUpgrader metaDataUpgrader) {
        // MetaData upgrade is tested in GatewayMetaStateTests, we override this method to NOP to make mocking easier
    }

    @Override
    public void applyClusterStateUpdaters() {
        // Just set localNode here, not to mess with ClusterService and IndicesService mocking
        previousClusterState = ClusterStateUpdaters.setLocalNode(previousClusterState, localNode);
    }
}
