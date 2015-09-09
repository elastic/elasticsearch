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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalTestCluster;

import static org.hamcrest.Matchers.instanceOf;

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

    protected Node buildNode(String gateway) {
        final Settings.Builder settings = Settings.builder()
                .put(ClusterName.SETTING, InternalTestCluster.clusterName("custom-gateway", randomLong()))
                .put("path.home", createTempDir())
                .put("path.shared_data", createTempDir().getParent())
                .put("node.name", "mock_gateway")
                .put(EsExecutors.PROCESSORS, 1) // limit the number of threads created
                .put("http.enabled", false)
                .put(InternalSettingsPreparer.IGNORE_SYSTEM_PROPERTIES_SETTING, true);
        if (gateway != null) {
            settings.put(GatewayModule.GATEWAY_TYPE_KEY, gateway);
            settings.putArray("plugin.types", TestPlugin.class.getName());
        }
        return NodeBuilder.nodeBuilder().local(true).data(true).settings(settings // make sure we get what we set :)
        ).build().start();
    }

    public static class TestPlugin extends Plugin {
        @Override
        public String name() {
            return "mock-gateway";
        }

        @Override
        public String description() {
            return "injecting custom MockGateway testing";
        }

        public void onModule(GatewayModule gatewayModule) {
            gatewayModule.registerGatewayType("mock", MockGateway.class);
        }

        @Override
        public Settings additionalSettings() {
            return Settings.builder().put(GatewayModule.GATEWAY_TYPE_KEY, "mock").build();
        }
    }

    public static class MockGateway extends Gateway {

        @Inject
        public MockGateway(Settings settings, ClusterService clusterService, NodeEnvironment nodeEnv, GatewayMetaState metaState, TransportNodesListGatewayMetaState listGatewayMetaState, ClusterName clusterName) {
            super(settings, clusterService, nodeEnv, metaState, listGatewayMetaState, clusterName);
        }
    }
}
