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

import org.elasticsearch.Version;
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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

public class TransportBootstrapClusterActionTests extends ESTestCase {

    private final ActionFilters EMPTY_FILTERS = new ActionFilters(emptySet());

    private static BootstrapClusterRequest exampleRequest() {
        return new BootstrapClusterRequest(new BootstrapConfiguration(singletonList(new NodeDescription("id", "name"))));
    }

    public void testHandlesNonstandardDiscoveryImplementation() throws InterruptedException {
        final MockTransport transport = new MockTransport();
        final ThreadPool threadPool = new TestThreadPool("test", Settings.EMPTY);
        final DiscoveryNode discoveryNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
        final TransportService transportService = transport.createTransportService(Settings.EMPTY, threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, boundTransportAddress -> discoveryNode, null, emptySet());

        final Discovery discovery = mock(Discovery.class);
        verifyZeroInteractions(discovery);

        new TransportBootstrapClusterAction(Settings.EMPTY, EMPTY_FILTERS, transportService, discovery); // registers action
        transportService.start();
        transportService.acceptIncomingRequests();

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        transportService.sendRequest(discoveryNode, BootstrapClusterAction.NAME, exampleRequest(), new ResponseHandler() {
            @Override
            public void handleResponse(AcknowledgedResponse response) {
                throw new AssertionError("should not be called");
            }

            @Override
            public void handleException(TransportException exp) {
                assertThat(exp.getRootCause().getMessage(), equalTo("cluster bootstrapping is not supported by this discovery type"));
                countDownLatch.countDown();
            }
        });

        assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
        threadPool.shutdown();
    }

    public void testFailsOnNonMasterEligibleNodes() throws InterruptedException {
        final DiscoveryNode discoveryNode
            = new DiscoveryNode("local", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);

        final MockTransport transport = new MockTransport();
        final ThreadPool threadPool = new TestThreadPool("test", Settings.EMPTY);
        final TransportService transportService = transport.createTransportService(Settings.EMPTY, threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, boundTransportAddress -> discoveryNode, null, emptySet());

        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final ClusterState state = ClusterState.builder(new ClusterName("cluster")).build();
        final Coordinator coordinator = new Coordinator("local", Settings.EMPTY, clusterSettings, transportService,
            ESAllocationTestCase.createAllocationService(Settings.EMPTY),
            new MasterService("local", Settings.EMPTY, threadPool),
            () -> new InMemoryPersistedState(0, state), r -> emptyList(),
            new NoOpClusterApplier(), new Random(random().nextLong()));

        new TransportBootstrapClusterAction(Settings.EMPTY, EMPTY_FILTERS, transportService, coordinator); // registers action
        transportService.start();
        transportService.acceptIncomingRequests();
        coordinator.start();

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        transportService.sendRequest(discoveryNode, BootstrapClusterAction.NAME, exampleRequest(), new ResponseHandler() {
            @Override
            public void handleResponse(AcknowledgedResponse response) {
                throw new AssertionError("should not be called");
            }

            @Override
            public void handleException(TransportException exp) {
                assertThat(exp.getRootCause().getMessage(), equalTo("this node is not master-eligible"));
                countDownLatch.countDown();
            }
        });

        assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
        threadPool.shutdown();
    }

    public void testSetsInitialConfiguration() throws InterruptedException {
        final DiscoveryNode discoveryNode
            = new DiscoveryNode("local", buildNewFakeTransportAddress(), emptyMap(), singleton(Role.MASTER), Version.CURRENT);

        final MockTransport transport = new MockTransport();
        final ThreadPool threadPool = new TestThreadPool("test", Settings.EMPTY);
        final TransportService transportService = transport.createTransportService(Settings.EMPTY, threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, boundTransportAddress -> discoveryNode, null, emptySet());

        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final ClusterState state = ClusterState.builder(new ClusterName("cluster")).build();
        final Coordinator coordinator = new Coordinator("local", Settings.EMPTY, clusterSettings, transportService,
            ESAllocationTestCase.createAllocationService(Settings.EMPTY),
            new MasterService("local", Settings.EMPTY, threadPool),
            () -> new InMemoryPersistedState(0, state), r -> emptyList(),
            new NoOpClusterApplier(), new Random(random().nextLong()));

        new TransportBootstrapClusterAction(Settings.EMPTY, EMPTY_FILTERS, transportService, coordinator); // registers action
        transportService.start();
        transportService.acceptIncomingRequests();
        coordinator.start();
        coordinator.startInitialJoin();

        assertFalse(coordinator.isInitialConfigurationSet());

        final BootstrapClusterRequest request
            = new BootstrapClusterRequest(new BootstrapConfiguration(singletonList(new NodeDescription(discoveryNode))));

        {
            final CountDownLatch countDownLatch = new CountDownLatch(1);
            transportService.sendRequest(discoveryNode, BootstrapClusterAction.NAME, request, new ResponseHandler() {
                @Override
                public void handleResponse(AcknowledgedResponse response) {
                    assertTrue(response.isAcknowledged());
                    countDownLatch.countDown();
                }

                @Override
                public void handleException(TransportException exp) {
                    throw new AssertionError("should not be called", exp);
                }
            });

            assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
        }

        assertTrue(coordinator.isInitialConfigurationSet());

        {
            final CountDownLatch countDownLatch = new CountDownLatch(1);
            transportService.sendRequest(discoveryNode, BootstrapClusterAction.NAME, request, new ResponseHandler() {
                @Override
                public void handleResponse(AcknowledgedResponse response) {
                    assertFalse(response.isAcknowledged());
                    countDownLatch.countDown();
                }

                @Override
                public void handleException(TransportException exp) {
                    throw new AssertionError("should not be called", exp);
                }
            });

            assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
        }

        threadPool.shutdown();
    }

    private abstract class ResponseHandler implements TransportResponseHandler<AcknowledgedResponse> {
        @Override
        public String executor() {
            return Names.SAME;
        }

        @Override
        public AcknowledgedResponse read(StreamInput in) throws IOException {
            AcknowledgedResponse acknowledgedResponse = new AcknowledgedResponse();
            acknowledgedResponse.readFrom(in);
            return acknowledgedResponse;
        }
    }
}
