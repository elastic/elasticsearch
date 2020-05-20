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

package org.elasticsearch.transport;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.nio.MockNioTransport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class TransportServiceHandshakeTests extends ESTestCase {

    private static ThreadPool threadPool;
    private static final long timeout = Long.MAX_VALUE;

    private volatile boolean corruptInboundHandshakes;

    @BeforeClass
    public static void startThreadPool() {
        threadPool = new TestThreadPool(TransportServiceHandshakeTests.class.getSimpleName());
    }

    private List<TransportService> transportServices = new ArrayList<>();

    private NetworkHandle startServices(String nodeNameAndId, Settings settings, Version version) {
        return startServices(nodeNameAndId, settings, version, version);
    }

    private NetworkHandle startServices(String nodeNameAndId, Settings settings,
                                        Version transportVersion, Version transportServiceVersion) {
        MockNioTransport transport =
                new MockNioTransport(settings, transportVersion, threadPool, new NetworkService(Collections.emptyList()),
                    PageCacheRecycler.NON_RECYCLING_INSTANCE, new NamedWriteableRegistry(Collections.emptyList()),
                    new NoneCircuitBreakerService()) {

                    @Override
                    protected InboundMessage adjustInboundMessage(InboundMessage message) {
                        if (corruptInboundHandshakes && message.getHeader().isHandshake()) {
                            return unreadableHandshake(message);
                        } else {
                            return message;
                        }
                    }
         };
        TransportService transportService = new MockTransportService(settings, transport, threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, (boundAddress) -> new DiscoveryNode(
            nodeNameAndId,
            nodeNameAndId,
            boundAddress.publishAddress(),
            emptyMap(),
            emptySet(),
            transportServiceVersion), null, Collections.emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();
        transportServices.add(transportService);
        return new NetworkHandle(transportService, transportService.getLocalNode());
    }

    @After
    public void tearDown() throws Exception {
        for (TransportService transportService : transportServices) {
            transportService.close();
        }
        super.tearDown();
    }

    @AfterClass
    public static void terminateThreadPool() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        // since static must set to null to be eligible for collection
        threadPool = null;
    }

    public void testConnectToNodeLight() {
        Settings settings = Settings.builder().put("cluster.name", "test").build();

        NetworkHandle handleA = startServices("TS_A", settings, Version.CURRENT);
        NetworkHandle handleB = startServices("TS_B", settings, randomCompatibleVersion());
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "",
            handleB.discoveryNode.getAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT.minimumCompatibilityVersion());
        try (Transport.Connection connection =
                 AbstractSimpleTransportTestCase.openConnection(handleA.transportService, discoveryNode, TestProfiles.LIGHT_PROFILE)) {
            DiscoveryNode connectedNode = PlainActionFuture.get(fut -> handleA.transportService.handshake(connection, timeout, fut));
            assertNotNull(connectedNode);
            // the name and version should be updated
            assertEquals(connectedNode.getName(), "TS_B");
            assertEquals(connectedNode.getVersion(), handleB.discoveryNode.getVersion());
            assertFalse(handleA.transportService.nodeConnected(discoveryNode));
        }
    }

    public void testMismatchedClusterName() {

        NetworkHandle handleA = startServices("TS_A", Settings.builder().put("cluster.name", "a").build(), Version.CURRENT);
        NetworkHandle handleB = startServices("TS_B", Settings.builder().put("cluster.name", "b").build(), Version.CURRENT);
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "",
            handleB.discoveryNode.getAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT.minimumCompatibilityVersion());
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> {
            try (Transport.Connection connection =
                     AbstractSimpleTransportTestCase.openConnection(handleA.transportService, discoveryNode, TestProfiles.LIGHT_PROFILE)) {
                PlainActionFuture.get(fut -> handleA.transportService.handshake(connection, timeout, ActionListener.map(fut, x -> null)));
            }
        });
        assertThat(ex.getMessage(), containsString("handshake with [" + discoveryNode +
            "] failed: remote cluster name [b] does not match local cluster name [a]"));
        assertFalse(handleA.transportService.nodeConnected(discoveryNode));
    }

    public void testIncompatibleTransportServiceVersions() {
        Settings settings = Settings.builder().put("cluster.name", "test").build();
        NetworkHandle handleA = startServices("TS_A", settings, Version.CURRENT);
        // use a compatible version for the transport so the TCP handshake succeeds
        NetworkHandle handleB = startServices("TS_B", settings, randomCompatibleVersion(),
            VersionUtils.getPreviousVersion(Version.CURRENT.minimumCompatibilityVersion()));
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "",
            handleB.discoveryNode.getAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT.minimumCompatibilityVersion());
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> {
            try (Transport.Connection connection =
                     AbstractSimpleTransportTestCase.openConnection(handleA.transportService, discoveryNode, TestProfiles.LIGHT_PROFILE)) {
                PlainActionFuture.get(fut -> handleA.transportService.handshake(connection, timeout, ActionListener.map(fut, x -> null)));
            }
        });
        assertThat(ex.getMessage(), containsString("handshake with [" + discoveryNode +
            "] failed: remote node version [" + handleB.discoveryNode.getVersion() + "] is incompatible with local node version [" +
            Version.CURRENT + "]"));
        assertFalse(handleA.transportService.nodeConnected(discoveryNode));
    }

    public void testIncompatibleTransportVersions() {
        Settings settings = Settings.builder().put("cluster.name", "test").build();
        NetworkHandle handleA = startServices("TS_A", settings, Version.CURRENT);
        NetworkHandle handleB = startServices("TS_B", settings,
            VersionUtils.getPreviousVersion(Version.CURRENT.minimumCompatibilityVersion()));
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "",
            handleB.discoveryNode.getAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT.minimumCompatibilityVersion());
        ConnectTransportException connectTransportException = expectThrows(ConnectTransportException.class, () -> {
            try (Transport.Connection ignored =
                     AbstractSimpleTransportTestCase.openConnection(handleA.transportService, discoveryNode, TestProfiles.LIGHT_PROFILE)) {
                fail("connection should have failed");
            }
        });
        assertThat(connectTransportException.getCause(), instanceOf(IllegalStateException.class));
        final IllegalStateException illegalStateException = (IllegalStateException) connectTransportException.getCause();
        assertThat(illegalStateException.getMessage(), containsString("Received message from unsupported version: [" +
            handleB.discoveryNode.getVersion() + "] minimal compatible version is: [" +
            Version.CURRENT.minimumCompatibilityVersion() + "]"));
        assertFalse(handleA.transportService.nodeConnected(discoveryNode));
    }

    public void testReturnsExceptionToSameMajorVersion() {
        // Nodes use their minimum compatibility version for the TCP handshake, so a node from v(major-1).x will report its version
        // as v(major-2).last in the TCP handshake, with which we are not really compatible. We put extra effort into making sure that if
        // successful we can respond correctly in a format this old, but we do not guarantee that we can respond correctly with an
        // error response. However if the two nodes are from the same major version then we do guarantee compatibility of error responses.
        Settings settings = Settings.builder().put("cluster.name", "test").build();
        NetworkHandle currentVersionHandle = startServices("CURRENT_VERSION", settings, Version.CURRENT);
        Version sameMajor
            = VersionUtils.randomVersionBetween(random(), Version.fromId(Version.CURRENT.major * 1000000 + 99), Version.CURRENT);
        NetworkHandle compatVersionHandle = startServices("COMPATIBLE_VERSION", settings, sameMajor);
        ConnectTransportException connectTransportException = expectThrows(ConnectTransportException.class, () -> {
            corruptInboundHandshakes = true;
            try (Transport.Connection ignored = AbstractSimpleTransportTestCase.openConnection(compatVersionHandle.transportService,
                currentVersionHandle.discoveryNode, TestProfiles.LIGHT_PROFILE)) {
                fail("connection should have failed");
            }
        });
        final Optional<Throwable> cause = ExceptionsHelper.unwrapCausesAndSuppressed(connectTransportException, t -> t.getCause() == null);
        assertTrue(cause.isPresent());
        assertThat(cause.get(), instanceOf(ElasticsearchException.class));
        assertThat(cause.get().getMessage(), containsString("unreadable handshake"));
        assertFalse(compatVersionHandle.transportService.nodeConnected(currentVersionHandle.discoveryNode));
    }

    @TestLogging(value = "org.elasticsearch.transport.InboundHandler:WARN", reason = "checking that a warning is logged")
    public void testDoesNotReturnExceptionToPreviousMajorVersion() throws IllegalAccessException {
        // Nodes use their minimum compatibility version for the TCP handshake, so a node from v(major-1).x will report its version
        // as v(major-2).last in the TCP handshake, with which we are not really compatible. We put extra effort into making sure that if
        // successful we can respond correctly in a format this old, but we do not guarantee that we can respond correctly with an
        // error response so we must just close the connection on an error.

        MockLogAppender mockAppender = new MockLogAppender();
        Settings settings = Settings.builder().put("cluster.name", "test").build();
        NetworkHandle currentVersionHandle = startServices("CURRENT_VERSION", settings, Version.CURRENT);
        Version previousMajor = Version.CURRENT.minimumCompatibilityVersion();
        NetworkHandle compatVersionHandle = startServices("COMPATIBLE_VERSION", settings, previousMajor);

        ConnectTransportException connectTransportException = expectThrows(ConnectTransportException.class, () -> {
            corruptInboundHandshakes = true;
            mockAppender.start();
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "expected message",
                    InboundHandler.class.getCanonicalName(),
                    Level.WARN,
                    "could not send error response to handshake"));
            Logger inboundHandlerLogger = LogManager.getLogger(InboundHandler.class);
            Loggers.addAppender(inboundHandlerLogger, mockAppender);

            try (Transport.Connection ignored = AbstractSimpleTransportTestCase.openConnection(compatVersionHandle.transportService,
                currentVersionHandle.discoveryNode, TestProfiles.LIGHT_PROFILE)) {
                fail("connection should have failed");
            } finally {
                Loggers.removeAppender(inboundHandlerLogger, mockAppender);
                mockAppender.stop();
            }
        });

        final Optional<Throwable> cause = ExceptionsHelper.unwrapCausesAndSuppressed(connectTransportException, t -> t.getCause() == null);
        assertTrue(cause.isPresent());
        assertThat(cause.get(), instanceOf(ElasticsearchException.class));
        assertThat(cause.get().getMessage(), not(containsString("unreadable handshake")));
        assertThat(cause.get().getMessage(), containsString("handshake failed because connection reset"));
        assertFalse(compatVersionHandle.transportService.nodeConnected(currentVersionHandle.discoveryNode));
        mockAppender.assertAllExpectationsMatched();
    }

    public void testNodeConnectWithDifferentNodeId() {
        Settings settings = Settings.builder().put("cluster.name", "test").build();
        NetworkHandle handleA = startServices("TS_A", settings, Version.CURRENT);
        NetworkHandle handleB = startServices("TS_B", settings, Version.CURRENT);
        DiscoveryNode discoveryNode = new DiscoveryNode(
            randomAlphaOfLength(10),
            handleB.discoveryNode.getAddress(),
            emptyMap(),
            emptySet(),
            handleB.discoveryNode.getVersion());
        ConnectTransportException ex = expectThrows(ConnectTransportException.class, () ->
            AbstractSimpleTransportTestCase.connectToNode(handleA.transportService, discoveryNode, TestProfiles.LIGHT_PROFILE));
        assertThat(ex.getMessage(), containsString("unexpected remote node"));
        assertFalse(handleA.transportService.nodeConnected(discoveryNode));
    }

    private static class NetworkHandle {
        private TransportService transportService;
        private DiscoveryNode discoveryNode;

        NetworkHandle(TransportService transportService, DiscoveryNode discoveryNode) {
            this.transportService = transportService;
            this.discoveryNode = discoveryNode;
        }
    }

    private static Version randomCompatibleVersion() {
        return VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(), Version.CURRENT);
    }

    private static InboundMessage unreadableHandshake(InboundMessage message) {
        return new InboundMessage(message.getHeader(), false) {
            @Override
            public int getContentLength() {
                return message.getContentLength();
            }

            @Override
            public StreamInput openOrGetStreamInput() {
                return new StreamInput() {

                    {
                        setVersion(message.getHeader().getVersion());
                    }

                    @Override
                    public byte readByte() {
                        throw unreadableHandshakeException();
                    }

                    @Override
                    public void readBytes(byte[] b, int offset, int len) {
                        throw unreadableHandshakeException();
                    }

                    @Override
                    public void close() {
                    }

                    @Override
                    public int available() {
                        return message.getContentLength();
                    }

                    @Override
                    protected void ensureCanReadBytes(int length) {
                    }

                    @Override
                    public int read() {
                        throw unreadableHandshakeException();
                    }
                };
            }

            private ElasticsearchException unreadableHandshakeException() {
                return new ElasticsearchException("unreadable handshake");
            }
        };
    }

}
