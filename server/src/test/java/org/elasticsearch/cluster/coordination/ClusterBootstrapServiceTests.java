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
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.bootstrap.BootstrapClusterAction;
import org.elasticsearch.action.admin.cluster.bootstrap.BootstrapClusterRequest;
import org.elasticsearch.action.admin.cluster.bootstrap.BootstrapClusterResponse;
import org.elasticsearch.action.admin.cluster.bootstrap.BootstrapConfiguration.NodeDescription;
import org.elasticsearch.action.admin.cluster.bootstrap.GetDiscoveredNodesAction;
import org.elasticsearch.action.admin.cluster.bootstrap.GetDiscoveredNodesRequest;
import org.elasticsearch.action.admin.cluster.bootstrap.GetDiscoveredNodesResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNode.Role;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING;
import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.INITIAL_MASTER_NODE_COUNT_SETTING;
import static org.elasticsearch.common.settings.Settings.builder;
import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING;
import static org.elasticsearch.discovery.zen.SettingsBasedHostsProvider.DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class ClusterBootstrapServiceTests extends ESTestCase {

    private DiscoveryNode localNode, otherNode1, otherNode2;
    private DeterministicTaskQueue deterministicTaskQueue;
    private TransportService transportService;
    private ClusterBootstrapService clusterBootstrapService;

    @Before
    public void createServices() {
        localNode = newDiscoveryNode("local");
        otherNode1 = newDiscoveryNode("other1");
        otherNode2 = newDiscoveryNode("other2");

        deterministicTaskQueue = new DeterministicTaskQueue(builder().put(NODE_NAME_SETTING.getKey(), "node").build(), random());

        final MockTransport transport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                throw new AssertionError("unexpected " + action);
            }
        };

        transportService = transport.createTransportService(Settings.EMPTY, deterministicTaskQueue.getThreadPool(),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, boundTransportAddress -> localNode, null, emptySet());

        clusterBootstrapService = new ClusterBootstrapService(builder().put(INITIAL_MASTER_NODE_COUNT_SETTING.getKey(), 3).build(),
            transportService);

        final Settings settings;
        if (randomBoolean()) {
            settings = Settings.builder().put(INITIAL_MASTER_NODE_COUNT_SETTING.getKey(), 3).build();
        } else {
            settings = Settings.builder()
                .putList(INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getName(), otherNode1.getName(), otherNode2.getName()).build();
        }
        clusterBootstrapService = new ClusterBootstrapService(settings, transportService);
    }

    private DiscoveryNode newDiscoveryNode(String nodeName) {
        return new DiscoveryNode(nodeName, randomAlphaOfLength(10), buildNewFakeTransportAddress(), emptyMap(), singleton(Role.MASTER),
            Version.CURRENT);
    }

    private void startServices() {
        transportService.start();
        transportService.acceptIncomingRequests();
        clusterBootstrapService.start();
    }

    public void testDoesNothingOnNonMasterNodes() {
        localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        transportService.registerRequestHandler(GetDiscoveredNodesAction.NAME, Names.SAME, GetDiscoveredNodesRequest::new,
            (request, channel, task) -> {
                throw new AssertionError("should not make a discovery request");
            });

        startServices();
        deterministicTaskQueue.runAllTasks();
    }

    public void testDoesNothingByDefaultIfHostsProviderConfigured() {
        testConfiguredIfSettingSet(builder().putList(DISCOVERY_HOSTS_PROVIDER_SETTING.getKey()));
    }

    public void testDoesNothingByDefaultIfUnicastHostsConfigured() {
        testConfiguredIfSettingSet(builder().putList(DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.getKey()));
    }

    public void testDoesNothingByDefaultIfMasterNodeCountConfigured() {
        testConfiguredIfSettingSet(builder().put(INITIAL_MASTER_NODE_COUNT_SETTING.getKey(), 0));
    }

    public void testDoesNothingByDefaultIfMasterNodesConfigured() {
        testConfiguredIfSettingSet(builder().putList(INITIAL_MASTER_NODES_SETTING.getKey()));
    }

    private void testConfiguredIfSettingSet(Builder builder) {
        clusterBootstrapService = new ClusterBootstrapService(builder.build(), transportService);
        transportService.registerRequestHandler(GetDiscoveredNodesAction.NAME, Names.SAME, GetDiscoveredNodesRequest::new,
            (request, channel, task) -> {
                throw new AssertionError("should not make a discovery request");
            });
        startServices();
        deterministicTaskQueue.runAllTasks();
    }

    public void testBootstrapsAutomaticallyWithDefaultConfiguration() {
        clusterBootstrapService = new ClusterBootstrapService(Settings.EMPTY, transportService);

        final Set<DiscoveryNode> discoveredNodes = Stream.of(localNode, otherNode1, otherNode2).collect(Collectors.toSet());
        transportService.registerRequestHandler(GetDiscoveredNodesAction.NAME, Names.SAME, GetDiscoveredNodesRequest::new,
            (request, channel, task) -> channel.sendResponse(new GetDiscoveredNodesResponse(discoveredNodes)));

        final AtomicBoolean bootstrapped = new AtomicBoolean();
        transportService.registerRequestHandler(BootstrapClusterAction.NAME, Names.SAME, BootstrapClusterRequest::new,
            (request, channel, task) -> {
                assertThat(request.getBootstrapConfiguration().getNodeDescriptions().stream()
                        .map(NodeDescription::getId).collect(Collectors.toSet()),
                    equalTo(discoveredNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet())));

                channel.sendResponse(new BootstrapClusterResponse(randomBoolean()));
                assertTrue(bootstrapped.compareAndSet(false, true));
            });

        startServices();
        deterministicTaskQueue.runAllTasks();

        assertTrue(bootstrapped.get());
    }

    public void testDoesNotRetryOnDiscoveryFailure() {
        transportService.registerRequestHandler(GetDiscoveredNodesAction.NAME, Names.SAME, GetDiscoveredNodesRequest::new,
            new TransportRequestHandler<GetDiscoveredNodesRequest>() {
                private boolean called = false;

                @Override
                public void messageReceived(GetDiscoveredNodesRequest request, TransportChannel channel, Task task) {
                    assert called == false;
                    called = true;
                    throw new IllegalArgumentException("simulate failure of discovery request");
                }
            });

        startServices();
        deterministicTaskQueue.runAllTasks();
    }

    public void testBootstrapsOnDiscoverySuccess() {
        final AtomicBoolean discoveryAttempted = new AtomicBoolean();
        final Set<DiscoveryNode> discoveredNodes = Stream.of(localNode, otherNode1, otherNode2).collect(Collectors.toSet());
        transportService.registerRequestHandler(GetDiscoveredNodesAction.NAME, Names.SAME, GetDiscoveredNodesRequest::new,
            (request, channel, task) -> {
                assertTrue(discoveryAttempted.compareAndSet(false, true));
                channel.sendResponse(new GetDiscoveredNodesResponse(discoveredNodes));
            });

        final AtomicBoolean bootstrapAttempted = new AtomicBoolean();
        transportService.registerRequestHandler(BootstrapClusterAction.NAME, Names.SAME, BootstrapClusterRequest::new,
            (request, channel, task) -> {
                assertTrue(bootstrapAttempted.compareAndSet(false, true));
                channel.sendResponse(new BootstrapClusterResponse(false));
            });

        startServices();
        deterministicTaskQueue.runAllTasks();

        assertTrue(discoveryAttempted.get());
        assertTrue(bootstrapAttempted.get());
    }

    public void testRetriesOnBootstrapFailure() {
        final Set<DiscoveryNode> discoveredNodes = Stream.of(localNode, otherNode1, otherNode2).collect(Collectors.toSet());
        transportService.registerRequestHandler(GetDiscoveredNodesAction.NAME, Names.SAME, GetDiscoveredNodesRequest::new,
            (request, channel, task) -> channel.sendResponse(new GetDiscoveredNodesResponse(discoveredNodes)));

        AtomicLong callCount = new AtomicLong();
        transportService.registerRequestHandler(BootstrapClusterAction.NAME, Names.SAME, BootstrapClusterRequest::new,
            (request, channel, task) -> {
                callCount.incrementAndGet();
                channel.sendResponse(new ElasticsearchException("simulated exception"));
            });

        startServices();
        while (callCount.get() < 5) {
            if (deterministicTaskQueue.hasDeferredTasks()) {
                deterministicTaskQueue.advanceTime();
            }
            deterministicTaskQueue.runAllRunnableTasks();
        }
    }

    public void testStopsRetryingBootstrapWhenStopped() {
        final Set<DiscoveryNode> discoveredNodes = Stream.of(localNode, otherNode1, otherNode2).collect(Collectors.toSet());
        transportService.registerRequestHandler(GetDiscoveredNodesAction.NAME, Names.SAME, GetDiscoveredNodesRequest::new,
            (request, channel, task) -> channel.sendResponse(new GetDiscoveredNodesResponse(discoveredNodes)));

        transportService.registerRequestHandler(BootstrapClusterAction.NAME, Names.SAME, BootstrapClusterRequest::new,
            (request, channel, task) -> channel.sendResponse(new ElasticsearchException("simulated exception")));

        deterministicTaskQueue.scheduleAt(deterministicTaskQueue.getCurrentTimeMillis() + 200000, new Runnable() {
            @Override
            public void run() {
                clusterBootstrapService.stop();
            }

            @Override
            public String toString() {
                return "stop cluster bootstrap service";
            }
        });

        startServices();
        deterministicTaskQueue.runAllTasks();
        // termination means success
    }
}
