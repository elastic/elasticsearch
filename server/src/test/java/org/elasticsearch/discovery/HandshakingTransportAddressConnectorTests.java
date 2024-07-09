/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery;

import org.apache.logging.log4j.Level;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportService.HandshakeResponse;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING;
import static org.elasticsearch.discovery.HandshakingTransportAddressConnector.PROBE_CONNECT_TIMEOUT_SETTING;
import static org.elasticsearch.discovery.HandshakingTransportAddressConnector.PROBE_HANDSHAKE_TIMEOUT_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;

public class HandshakingTransportAddressConnectorTests extends ESTestCase {

    private DiscoveryNode remoteNode;
    private TransportAddress discoveryAddress;
    private TransportService transportService;
    private ThreadPool threadPool;
    private String remoteClusterName;
    private HandshakingTransportAddressConnector handshakingTransportAddressConnector;
    private DiscoveryNode localNode;

    private boolean dropHandshake;
    @Nullable // unless we want the full connection to fail
    private TransportException fullConnectionFailure;

    @Before
    public void startServices() {
        localNode = DiscoveryNodeUtils.create("local-node");
        final Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(CLUSTER_NAME_SETTING.getKey(), "local-cluster")
            .put(PROBE_HANDSHAKE_TIMEOUT_SETTING.getKey(), "1s") // shorter than default for the sake of test speed
            .build();
        threadPool = new TestThreadPool("node", settings);

        remoteNode = null;
        discoveryAddress = null;
        remoteClusterName = null;
        dropHandshake = false;
        fullConnectionFailure = null;

        final MockTransport mockTransport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                super.onSendRequest(requestId, action, request, node);
                assertThat(action, equalTo(TransportService.HANDSHAKE_ACTION_NAME));
                assertThat(discoveryAddress, notNullValue());
                assertThat(node.getAddress(), oneOf(discoveryAddress, remoteNode.getAddress()));
                if (dropHandshake == false) {
                    if (fullConnectionFailure != null && node.getAddress().equals(remoteNode.getAddress())) {
                        handleError(requestId, fullConnectionFailure);
                    } else {
                        handleResponse(
                            requestId,
                            new HandshakeResponse(Version.CURRENT, Build.current().hash(), remoteNode, new ClusterName(remoteClusterName))
                        );
                    }
                }
            }
        };

        transportService = mockTransport.createTransportService(
            settings,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            address -> localNode,
            null,
            emptySet()
        );

        transportService.start();
        transportService.acceptIncomingRequests();

        handshakingTransportAddressConnector = new HandshakingTransportAddressConnector(settings, transportService);
    }

    @After
    public void stopServices() {
        transportService.stop();
        terminate(threadPool);
    }

    public void testConnectsToMasterNode() throws InterruptedException {
        final CountDownLatch completionLatch = new CountDownLatch(1);
        final SetOnce<DiscoveryNode> receivedNode = new SetOnce<>();

        remoteNode = DiscoveryNodeUtils.create("remote-node");
        remoteClusterName = "local-cluster";
        discoveryAddress = getDiscoveryAddress();

        handshakingTransportAddressConnector.connectToRemoteMasterNode(
            discoveryAddress,
            ActionTestUtils.assertNoFailureListener(connectResult -> {
                receivedNode.set(connectResult.getDiscoveryNode());
                completionLatch.countDown();
            })
        );

        assertTrue(completionLatch.await(30, TimeUnit.SECONDS));
        assertEquals(remoteNode, receivedNode.get());
    }

    @TestLogging(reason = "ensure logging happens", value = "org.elasticsearch.discovery.HandshakingTransportAddressConnector:INFO")
    public void testLogsFullConnectionFailureAfterSuccessfulHandshake() throws Exception {

        final var remoteNodeAddress = buildNewFakeTransportAddress();
        remoteNode = DiscoveryNodeUtils.create("remote-node", remoteNodeAddress);
        remoteClusterName = "local-cluster";
        discoveryAddress = buildNewFakeTransportAddress();

        fullConnectionFailure = new ConnectTransportException(remoteNode, "simulated", new ElasticsearchException("root cause"));

        FailureListener failureListener = new FailureListener();

        try (var mockLog = MockLog.capture(HandshakingTransportAddressConnector.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "message",
                    HandshakingTransportAddressConnector.class.getCanonicalName(),
                    Level.WARN,
                    Strings.format(
                        """
                            Successfully discovered master-eligible node [%s] at address [%s] but could not connect to it at its publish \
                            address of [%s]. Each node in a cluster must be accessible at its publish address by all other nodes in the \
                            cluster. See %s for more information.""",
                        remoteNode.descriptionWithoutAttributes(),
                        discoveryAddress,
                        remoteNodeAddress,
                        ReferenceDocs.NETWORK_BINDING_AND_PUBLISHING
                    )
                )
            );

            handshakingTransportAddressConnector.connectToRemoteMasterNode(discoveryAddress, failureListener);
            assertThat(failureListener.getFailureMessage(), containsString("simulated"));
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testDoesNotConnectToNonMasterNode() throws InterruptedException {
        remoteNode = DiscoveryNodeUtils.builder("remote-node").roles(emptySet()).build();
        discoveryAddress = getDiscoveryAddress();
        remoteClusterName = "local-cluster";

        FailureListener failureListener = new FailureListener();
        handshakingTransportAddressConnector.connectToRemoteMasterNode(discoveryAddress, failureListener);
        assertThat(
            failureListener.getFailureMessage(),
            allOf(
                containsString("successfully discovered master-ineligible node"),
                containsString(remoteNode.descriptionWithoutAttributes()),
                containsString("to suppress this message"),
                containsString("remove address [" + discoveryAddress + "] from your discovery configuration"),
                containsString("ensure that traffic to this address is routed only to master-eligible nodes")
            )
        );
    }

    public void testDoesNotConnectToLocalNode() throws Exception {
        remoteNode = localNode;
        discoveryAddress = getDiscoveryAddress();
        remoteClusterName = "local-cluster";

        FailureListener failureListener = new FailureListener();
        handshakingTransportAddressConnector.connectToRemoteMasterNode(discoveryAddress, failureListener);
        assertThat(failureListener.getFailureMessage(), containsString("successfully discovered local node"));
    }

    public void testDoesNotConnectToDifferentCluster() throws InterruptedException {
        remoteNode = DiscoveryNodeUtils.create("remote-node");
        discoveryAddress = getDiscoveryAddress();
        remoteClusterName = "another-cluster";

        FailureListener failureListener = new FailureListener();
        handshakingTransportAddressConnector.connectToRemoteMasterNode(discoveryAddress, failureListener);
        assertThat(
            failureListener.getFailureMessage(),
            containsString("remote cluster name [another-cluster] does not match local cluster name [local-cluster]")
        );
    }

    public void testTimeoutDefaults() {
        assertThat(PROBE_HANDSHAKE_TIMEOUT_SETTING.get(Settings.EMPTY), equalTo(TimeValue.timeValueSeconds(30)));
        assertThat(PROBE_CONNECT_TIMEOUT_SETTING.get(Settings.EMPTY), equalTo(TimeValue.timeValueSeconds(30)));
    }

    public void testHandshakeTimesOut() throws InterruptedException {
        remoteNode = DiscoveryNodeUtils.create("remote-node");
        discoveryAddress = getDiscoveryAddress();
        remoteClusterName = "local-cluster";
        dropHandshake = true;

        FailureListener failureListener = new FailureListener();
        handshakingTransportAddressConnector.connectToRemoteMasterNode(discoveryAddress, failureListener);
        assertThat(failureListener.getFailureMessage(), containsString("timed out"));
    }

    private TransportAddress getDiscoveryAddress() {
        return randomBoolean() ? remoteNode.getAddress() : buildNewFakeTransportAddress();
    }

    private static class FailureListener implements ActionListener<ProbeConnectionResult> {
        final CountDownLatch completionLatch = new CountDownLatch(1);
        String message;

        @Override
        public void onResponse(ProbeConnectionResult connectResult) {
            fail(connectResult.getDiscoveryNode().toString());
        }

        @Override
        public void onFailure(Exception e) {
            message = e.getMessage();
            completionLatch.countDown();
        }

        String getFailureMessage() throws InterruptedException {
            assertTrue("timed out waiting for listener to complete", completionLatch.await(15, TimeUnit.SECONDS));
            return message;
        }
    }
}
