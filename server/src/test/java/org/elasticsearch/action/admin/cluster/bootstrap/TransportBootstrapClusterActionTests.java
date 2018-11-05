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
package org.elasticsearch.action.admin.cluster.bootstrap;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.bootstrap.BootstrapConfiguration.NodeDescription;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.InMemoryPersistedState;
import org.elasticsearch.cluster.coordination.NoOpClusterApplier;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNode.Role;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

public class TransportBootstrapClusterActionTests extends ESTestCase {
    private static BootstrapClusterRequest exampleRequest() {
        return new BootstrapClusterRequest(new BootstrapConfiguration(singletonList(new NodeDescription("id", "name"))));
    }

    public void testHandlesNonstandardDiscoveryImplementation() {
        final MockTransport transport = new MockTransport();
        final ThreadPool threadPool = new TestThreadPool("test", Settings.EMPTY);
        final DiscoveryNode discoveryNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
        final TransportService transportService = transport.createTransportService(Settings.EMPTY, threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, boundTransportAddress -> discoveryNode, null, emptySet());

        final Discovery discovery = mock(Discovery.class);
        verifyZeroInteractions(discovery);

        final TransportBootstrapClusterAction transportBootstrapClusterAction
            = new TransportBootstrapClusterAction(Settings.EMPTY, mock(ActionFilters.class), transportService, discovery);

        final ActionListener<AcknowledgedResponse> listener = new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                throw new AssertionError("should not be called");
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("should not be called");
            }
        };

        assertThat(expectThrows(IllegalStateException.class,
            () -> transportBootstrapClusterAction.doExecute(mock(Task.class), exampleRequest(), listener))
            .getMessage(), equalTo("cluster bootstrapping is not supported by this discovery type"));

        threadPool.shutdown();
    }

    public void testFailsOnNonMasterEligibleNodes() {
        final DiscoveryNode discoveryNode
            = new DiscoveryNode("local", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);

        final MockTransport transport = new MockTransport();
        final ThreadPool threadPool = new TestThreadPool("test", Settings.EMPTY);
        final TransportService transportService = transport.createTransportService(Settings.EMPTY, threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, boundTransportAddress -> discoveryNode, null, emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();

        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final ClusterState state = ClusterState.builder(new ClusterName("cluster")).build();
        final Coordinator coordinator = new Coordinator("local", Settings.EMPTY, clusterSettings, transportService,
            ESAllocationTestCase.createAllocationService(Settings.EMPTY),
            new MasterService("local", Settings.EMPTY, threadPool),
            () -> new InMemoryPersistedState(0, state), r -> emptyList(),
            new NoOpClusterApplier(), random());
        coordinator.start();

        final TransportBootstrapClusterAction transportBootstrapClusterAction
            = new TransportBootstrapClusterAction(Settings.EMPTY, mock(ActionFilters.class), transportService, coordinator);

        final ActionListener<AcknowledgedResponse> listener = new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                throw new AssertionError("should not be called");
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("should not be called");
            }
        };

        assertThat(expectThrows(ElasticsearchException.class,
            () -> transportBootstrapClusterAction.doExecute(mock(Task.class), exampleRequest(), listener)).getMessage(),
            equalTo("this node is not master-eligible"));

        threadPool.shutdown();
    }

    public void testSetsInitialConfiguration() {
        final DiscoveryNode discoveryNode
            = new DiscoveryNode("local", buildNewFakeTransportAddress(), emptyMap(), singleton(Role.MASTER), Version.CURRENT);

        final MockTransport transport = new MockTransport();
        final ThreadPool threadPool = new TestThreadPool("test", Settings.EMPTY);
        final TransportService transportService = transport.createTransportService(Settings.EMPTY, threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, boundTransportAddress -> discoveryNode, null, emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();

        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final ClusterState state = ClusterState.builder(new ClusterName("cluster")).build();
        final Coordinator coordinator = new Coordinator("local", Settings.EMPTY, clusterSettings, transportService,
            ESAllocationTestCase.createAllocationService(Settings.EMPTY),
            new MasterService("local", Settings.EMPTY, threadPool),
            () -> new InMemoryPersistedState(0, state), r -> emptyList(),
            new NoOpClusterApplier(), random());
        coordinator.start();
        coordinator.startInitialJoin();

        final TransportBootstrapClusterAction transportBootstrapClusterAction
            = new TransportBootstrapClusterAction(Settings.EMPTY, mock(ActionFilters.class), transportService, coordinator);

        final AtomicBoolean responseReceived = new AtomicBoolean();

        assertFalse(coordinator.isInitialConfigurationSet());

        final BootstrapClusterRequest request
            = new BootstrapClusterRequest(new BootstrapConfiguration(singletonList(new NodeDescription(discoveryNode))));

        transportBootstrapClusterAction.doExecute(mock(Task.class), request,
            new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    assertTrue(acknowledgedResponse.isAcknowledged());
                    responseReceived.set(true);
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError("should not be called");
                }
            });
        assertTrue(responseReceived.get());
        assertTrue(coordinator.isInitialConfigurationSet());

        responseReceived.set(false);
        transportBootstrapClusterAction.doExecute(mock(Task.class), request,
            new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    assertFalse(acknowledgedResponse.isAcknowledged());
                    responseReceived.set(true);
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError("should not be called");
                }
            });
        assertTrue(responseReceived.get());

        threadPool.shutdown();
    }
}
