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

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;

public class GatewayModuleTests extends ModuleTestCase {

    public void testCustomGateway() {
        GatewayModule gatewayModule = new GatewayModule(Settings.builder().put(GatewayModule.GATEWAY_TYPE_KEY, "mock").build());
        gatewayModule.registerGatewayType("mock", MockGateway.class);
        assertBinding(gatewayModule, Gateway.class, MockGateway.class);
    }

    public void testDefaultGateway() {
        GatewayModule gatewayModule = new GatewayModule(Settings.EMPTY);
        assertBinding(gatewayModule, Gateway.class, Gateway.class);
    }

    public static class MockGateway extends Gateway {

        @Inject
        public MockGateway(Settings settings, ClusterService clusterService, NodeEnvironment nodeEnv, GatewayMetaState metaState, TransportNodesListGatewayMetaState listGatewayMetaState, ClusterName clusterName) {
            super(settings, clusterService, nodeEnv, metaState, listGatewayMetaState, clusterName);
        }
    }
}
