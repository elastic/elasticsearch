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

package org.elasticsearch.cloud.azure.classic;

import com.microsoft.windowsazure.management.compute.models.DeploymentSlot;
import com.microsoft.windowsazure.management.compute.models.DeploymentStatus;
import com.microsoft.windowsazure.management.compute.models.HostedServiceGetDetailedResponse;
import com.microsoft.windowsazure.management.compute.models.InstanceEndpoint;
import com.microsoft.windowsazure.management.compute.models.RoleInstance;
import com.microsoft.windowsazure.management.compute.models.RoleInstancePowerState;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.cloud.azure.classic.management.AzureComputeService;
import org.elasticsearch.cloud.azure.classic.management.AzureComputeService.Discovery;
import org.elasticsearch.cloud.azure.classic.management.AzureComputeService.Management;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.azure.classic.AzureUnicastHostsProvider;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.plugin.discovery.azure.classic.AzureDiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.common.util.CollectionUtils.newSingletonArrayList;

public abstract class AbstractAzureComputeServiceTestCase extends ESIntegTestCase {

    private static final Map<String, DiscoveryNode> nodes = new ConcurrentHashMap<>();

    @After
    public void clearAzureNodes() {
        nodes.clear();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("discovery.zen.hosts_provider", "azure");

        // Make the test run faster
        builder.put(ZenDiscovery.JOIN_TIMEOUT_SETTING.getKey(), "1s")
            .put(ZenDiscovery.PING_TIMEOUT_SETTING.getKey(), "500ms");

        // We add a fake subscription_id to start mock compute service
        builder.put(Management.SUBSCRIPTION_ID_SETTING.getKey(), "fake")
            .put(Discovery.REFRESH_SETTING.getKey(), "5s")
            .put(Management.KEYSTORE_PATH_SETTING.getKey(), "dummy")
            .put(Management.KEYSTORE_PASSWORD_SETTING.getKey(), "dummy")
            .put(Management.SERVICE_NAME_SETTING.getKey(), "dummy");
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestPlugin.class);
    }

    @Override
    protected boolean addTestZenDiscovery() {
        return false;
    }

    /**
     * Register an existing node as a Azure node, exposing its address and details htrough
     *
     * @param nodeName the name of the node
     */
    protected void registerAzureNode(final String nodeName) {
        TransportService transportService = internalCluster().getInstance(TransportService.class, nodeName);
        assertNotNull(transportService);
        DiscoveryNode discoveryNode = transportService.getLocalNode();
        assertNotNull(discoveryNode);
        if (nodes.put(discoveryNode.getName(), discoveryNode) != null) {
            throw new IllegalArgumentException("Node [" + discoveryNode.getName() + "] cannot be registered twice in Azure");
        }
    }

    protected void assertNumberOfNodes(int expected) {
        NodesInfoResponse nodeInfos = client().admin().cluster().prepareNodesInfo().clear().execute().actionGet();
        assertNotNull(nodeInfos);
        assertNotNull(nodeInfos.getNodes());
        assertEquals(expected, nodeInfos.getNodes().size());
    }

    /**
     * Test plugin that exposes internal test cluster nodes as if they were real Azure nodes.
     * Use {@link #registerAzureNode(String)} method to expose nodes in the tests.
     */
    public static class TestPlugin extends AzureDiscoveryPlugin {

        public TestPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected AzureComputeService createComputeService() {
            return () -> {
                final List<RoleInstance> instances = new ArrayList<>();
                for (Map.Entry<String, DiscoveryNode> node : nodes.entrySet()) {
                    final String name = node.getKey();
                    final DiscoveryNode discoveryNode = node.getValue();

                    RoleInstance instance = new RoleInstance();
                    instance.setInstanceName(name);
                    instance.setHostName(discoveryNode.getHostName());
                    instance.setPowerState(RoleInstancePowerState.Started);

                    // Set the private IP address
                    final TransportAddress transportAddress = discoveryNode.getAddress();
                    instance.setIPAddress(transportAddress.address().getAddress());

                    // Set the public IP address
                    final InstanceEndpoint endpoint = new InstanceEndpoint();
                    endpoint.setName(Discovery.ENDPOINT_NAME_SETTING.getDefault(Settings.EMPTY));
                    endpoint.setVirtualIPAddress(transportAddress.address().getAddress());
                    endpoint.setPort(transportAddress.address().getPort());
                    instance.setInstanceEndpoints(new ArrayList<>(Collections.singletonList(endpoint)));
                    instances.add(instance);
                }

                final HostedServiceGetDetailedResponse.Deployment deployment = new HostedServiceGetDetailedResponse.Deployment();
                deployment.setName("dummy");
                deployment.setDeploymentSlot(DeploymentSlot.Production);
                deployment.setStatus(DeploymentStatus.Running);
                deployment.setRoleInstances(new ArrayList<>(Collections.unmodifiableList(instances)));

                final HostedServiceGetDetailedResponse response = new HostedServiceGetDetailedResponse();
                response.setDeployments(newSingletonArrayList(deployment));

                return response;
            };
        }

        /**
         * Defines a {@link AzureUnicastHostsProvider} for testing purpose that is able to resolve
         * network addresses for Azure instances running on the same host but different ports.
         */
        @Override
        protected AzureUnicastHostsProvider createUnicastHostsProvider(final Settings settings,
                                                                       final AzureComputeService azureComputeService,
                                                                       final TransportService transportService,
                                                                       final NetworkService networkService) {
            return new AzureUnicastHostsProvider(settings, azureComputeService, transportService, networkService) {
                @Override
                protected String resolveInstanceAddress(final HostType hostType, final RoleInstance instance) {
                    if (hostType == HostType.PRIVATE_IP) {
                        DiscoveryNode discoveryNode = nodes.get(instance.getInstanceName());
                        if (discoveryNode != null) {
                            // Format the InetSocketAddress to a format that contains the port number
                            return NetworkAddress.format(discoveryNode.getAddress().address());
                        }
                    }
                    return super.resolveInstanceAddress(hostType, instance);
                }
            };
        }
    }
}
