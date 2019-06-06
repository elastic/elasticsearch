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

package org.elasticsearch.discovery;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportService.HandshakeResponse;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING;
import static org.elasticsearch.discovery.HandshakingTransportAddressConnector.PROBE_HANDSHAKE_TIMEOUT_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class HandshakingTransportAddressConnectorTests extends ESTestCase {

    private DiscoveryNode remoteNode;
    private TransportService transportService;
    private ThreadPool threadPool;
    private String remoteClusterName;
    private HandshakingTransportAddressConnector handshakingTransportAddressConnector;
    private DiscoveryNode localNode;

    private boolean dropHandshake;

    @Before
    public void startServices() {
        localNode = new DiscoveryNode("local-node", buildNewFakeTransportAddress(), Version.CURRENT);
        final Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(CLUSTER_NAME_SETTING.getKey(), "local-cluster")
            .build();
        threadPool = new TestThreadPool("node", settings);

        remoteNode = null;
        remoteClusterName = null;
        dropHandshake = false;

        final MockTransport mockTransport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                super.onSendRequest(requestId, action, request, node);
                assertThat(action, equalTo(TransportService.HANDSHAKE_ACTION_NAME));
                assertEquals(remoteNode.getAddress(), node.getAddress());
                if (dropHandshake == false) {
                    handleResponse(requestId, new HandshakeResponse(remoteNode, new ClusterName(remoteClusterName), Version.CURRENT));
                }
            }
        };

        transportService = mockTransport.createTransportService(settings, threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, address -> localNode, null, emptySet());

        transportService.start();
        transportService.acceptIncomingRequests();

        handshakingTransportAddressConnector = new HandshakingTransportAddressConnector(settings, transportService);
    }

    @After
    public void stopServices() throws InterruptedException {
        transportService.stop();
        terminate(threadPool);
    }

    public void testConnectsToMasterNode() throws InterruptedException {
        final CountDownLatch completionLatch = new CountDownLatch(1);
        final SetOnce<DiscoveryNode> receivedNode = new SetOnce<>();

        remoteNode = new DiscoveryNode("remote-node", buildNewFakeTransportAddress(), Version.CURRENT);
        remoteClusterName = "local-cluster";

        handshakingTransportAddressConnector.connectToRemoteMasterNode(remoteNode.getAddress(), new ActionListener<DiscoveryNode>() {
            @Override
            public void onResponse(DiscoveryNode discoveryNode) {
                receivedNode.set(discoveryNode);
                completionLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });

        assertTrue(completionLatch.await(30, TimeUnit.SECONDS));
        assertEquals(remoteNode, receivedNode.get());
    }

    public void testDoesNotConnectToNonMasterNode() throws InterruptedException {
        remoteNode = new DiscoveryNode("remote-node", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        remoteClusterName = "local-cluster";

        FailureListener failureListener = new FailureListener();
        handshakingTransportAddressConnector.connectToRemoteMasterNode(remoteNode.getAddress(), failureListener);
        failureListener.assertFailure();
    }

    public void testDoesNotConnectToLocalNode() throws Exception {
        remoteNode = localNode;
        remoteClusterName = "local-cluster";

        FailureListener failureListener = new FailureListener();
        handshakingTransportAddressConnector.connectToRemoteMasterNode(remoteNode.getAddress(), failureListener);
        failureListener.assertFailure();
    }

    public void testDoesNotConnectToDifferentCluster() throws InterruptedException {
        remoteNode = new DiscoveryNode("remote-node", buildNewFakeTransportAddress(), Version.CURRENT);
        remoteClusterName = "another-cluster";

        FailureListener failureListener = new FailureListener();
        handshakingTransportAddressConnector.connectToRemoteMasterNode(remoteNode.getAddress(), failureListener);
        failureListener.assertFailure();
    }

    public void testHandshakeTimesOut() throws InterruptedException {
        remoteNode = new DiscoveryNode("remote-node", buildNewFakeTransportAddress(), Version.CURRENT);
        remoteClusterName = "local-cluster";
        dropHandshake = true;

        FailureListener failureListener = new FailureListener();
        handshakingTransportAddressConnector.connectToRemoteMasterNode(remoteNode.getAddress(), failureListener);
        Thread.sleep(PROBE_HANDSHAKE_TIMEOUT_SETTING.get(Settings.EMPTY).millis());
        failureListener.assertFailure();
    }

    private class FailureListener implements ActionListener<DiscoveryNode> {
        final CountDownLatch completionLatch = new CountDownLatch(1);

        @Override
        public void onResponse(DiscoveryNode discoveryNode) {
            fail(discoveryNode.toString());
        }

        @Override
        public void onFailure(Exception e) {
            completionLatch.countDown();
        }

        void assertFailure() throws InterruptedException {
            assertTrue(completionLatch.await(30, TimeUnit.SECONDS));
        }
    }
}
