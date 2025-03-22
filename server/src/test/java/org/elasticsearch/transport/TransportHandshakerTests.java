/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.transport;

import org.apache.logging.log4j.Level;
import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.TestThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransportHandshakerTests extends ESTestCase {

    private TransportHandshaker handshaker;
    private DiscoveryNode node;
    private TcpChannel channel;
    private TestThreadPool threadPool;
    private TransportHandshaker.HandshakeRequestSender requestSender;

    @UpdateForV10(owner = UpdateForV10.Owner.CORE_INFRA) // new handshake version required in v10
    private static final TransportVersion HANDSHAKE_REQUEST_VERSION = TransportHandshaker.V9_HANDSHAKE_VERSION;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        String nodeId = "node-id";
        channel = mock(TcpChannel.class);
        requestSender = mock(TransportHandshaker.HandshakeRequestSender.class);
        node = DiscoveryNodeUtils.builder(nodeId)
            .name(nodeId)
            .ephemeralId(nodeId)
            .address("host", "host_address", buildNewFakeTransportAddress())
            .roles(Collections.emptySet())
            .build();
        threadPool = new TestThreadPool(getTestName());
        handshaker = new TransportHandshaker(TransportVersion.current(), threadPool, requestSender, false);
    }

    @Override
    public void tearDown() throws Exception {
        threadPool.shutdown();
        super.tearDown();
    }

    public void testHandshakeRequestAndResponse() throws IOException {
        PlainActionFuture<TransportVersion> versionFuture = new PlainActionFuture<>();
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, node, channel, new TimeValue(30, TimeUnit.SECONDS), versionFuture);

        verify(requestSender).sendRequest(node, channel, reqId, HANDSHAKE_REQUEST_VERSION);

        assertFalse(versionFuture.isDone());

        TransportHandshaker.HandshakeRequest handshakeRequest = new TransportHandshaker.HandshakeRequest(
            TransportVersion.current(),
            randomIdentifier()
        );
        BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
        bytesStreamOutput.setTransportVersion(HANDSHAKE_REQUEST_VERSION);
        handshakeRequest.writeTo(bytesStreamOutput);
        StreamInput input = bytesStreamOutput.bytes().streamInput();
        input.setTransportVersion(HANDSHAKE_REQUEST_VERSION);
        final PlainActionFuture<TransportResponse> responseFuture = new PlainActionFuture<>();
        final TestTransportChannel channel = new TestTransportChannel(responseFuture);
        handshaker.handleHandshake(channel, reqId, input);

        TransportResponseHandler<TransportHandshaker.HandshakeResponse> handler = handshaker.removeHandlerForHandshake(reqId);
        handler.handleResponse((TransportHandshaker.HandshakeResponse) responseFuture.actionGet());

        assertTrue(versionFuture.isDone());
        assertEquals(TransportVersion.current(), versionFuture.actionGet());
    }

    @TestLogging(reason = "testing WARN logging", value = "org.elasticsearch.transport.TransportHandshaker:WARN")
    public void testIncompatibleHandshakeRequest() throws Exception {
        TransportHandshaker.HandshakeRequest handshakeRequest = new TransportHandshaker.HandshakeRequest(
            getRandomIncompatibleTransportVersion(),
            randomIdentifier()
        );
        BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
        bytesStreamOutput.setTransportVersion(HANDSHAKE_REQUEST_VERSION);
        handshakeRequest.writeTo(bytesStreamOutput);
        StreamInput input = bytesStreamOutput.bytes().streamInput();
        input.setTransportVersion(HANDSHAKE_REQUEST_VERSION);

        if (handshakeRequest.transportVersion.onOrAfter(TransportVersions.MINIMUM_COMPATIBLE)) {

            final PlainActionFuture<TransportResponse> responseFuture = new PlainActionFuture<>();
            final TestTransportChannel channel = new TestTransportChannel(responseFuture);

            // we fall back to the best known version
            MockLog.assertThatLogger(() -> {
                try {
                    handshaker.handleHandshake(channel, randomNonNegativeLong(), input);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            },
                TransportHandshaker.class,
                new MockLog.SeenEventExpectation(
                    "warning",
                    TransportHandshaker.class.getCanonicalName(),
                    Level.WARN,
                    Strings.format(
                        """
                            Negotiating transport handshake with remote node with version [%s/%s] received on [*] which appears to be from \
                            a chronologically-older release with a numerically-newer version compared to this node's version [%s/%s]. \
                            Upgrading to a chronologically-older release may not work reliably and is not recommended. Falling back to \
                            transport protocol version [%s].""",
                        handshakeRequest.releaseVersion,
                        handshakeRequest.transportVersion,
                        Build.current().version(),
                        TransportVersion.current(),
                        handshakeRequest.transportVersion.bestKnownVersion()
                    )
                )
            );

            assertTrue(responseFuture.isDone());
            assertEquals(
                handshakeRequest.transportVersion.bestKnownVersion(),
                asInstanceOf(TransportHandshaker.HandshakeResponse.class, responseFuture.result()).getTransportVersion()
            );

        } else {
            final TestTransportChannel channel = new TestTransportChannel(ActionListener.running(() -> fail("should not complete")));

            MockLog.assertThatLogger(
                () -> assertThat(
                    expectThrows(IllegalStateException.class, () -> handshaker.handleHandshake(channel, randomNonNegativeLong(), input))
                        .getMessage(),
                    allOf(
                        containsString("Rejecting unreadable transport handshake"),
                        containsString("[" + handshakeRequest.releaseVersion + "/" + handshakeRequest.transportVersion + "]"),
                        containsString("[" + Build.current().version() + "/" + TransportVersion.current() + "]"),
                        containsString("which has an incompatible wire format")
                    )
                ),
                TransportHandshaker.class,
                new MockLog.SeenEventExpectation(
                    "warning",
                    TransportHandshaker.class.getCanonicalName(),
                    Level.WARN,
                    "Rejecting unreadable transport handshake * incompatible wire format."
                )
            );
        }
    }

    public void testHandshakeResponseFromOlderNode() throws Exception {
        final PlainActionFuture<TransportVersion> versionFuture = new PlainActionFuture<>();
        final long reqId = randomNonNegativeLong();
        handshaker.sendHandshake(reqId, node, channel, SAFE_AWAIT_TIMEOUT, versionFuture);
        TransportResponseHandler<TransportHandshaker.HandshakeResponse> handler = handshaker.removeHandlerForHandshake(reqId);

        assertFalse(versionFuture.isDone());

        final var remoteVersion = TransportVersionUtils.randomCompatibleVersion(random());
        handler.handleResponse(new TransportHandshaker.HandshakeResponse(remoteVersion, randomIdentifier()));

        assertTrue(versionFuture.isDone());
        assertEquals(remoteVersion, versionFuture.result());
    }

    @TestLogging(reason = "testing WARN logging", value = "org.elasticsearch.transport.TransportHandshaker:WARN")
    public void testHandshakeResponseFromOlderNodeWithPatchedProtocol() throws Exception {
        final PlainActionFuture<TransportVersion> versionFuture = new PlainActionFuture<>();
        final long reqId = randomNonNegativeLong();
        handshaker.sendHandshake(reqId, node, channel, SAFE_AWAIT_TIMEOUT, versionFuture);
        TransportResponseHandler<TransportHandshaker.HandshakeResponse> handler = handshaker.removeHandlerForHandshake(reqId);

        assertFalse(versionFuture.isDone());

        final var randomIncompatibleTransportVersion = getRandomIncompatibleTransportVersion();
        final var handshakeResponse = new TransportHandshaker.HandshakeResponse(randomIncompatibleTransportVersion, randomIdentifier());

        if (randomIncompatibleTransportVersion.onOrAfter(TransportVersions.MINIMUM_COMPATIBLE)) {
            // we fall back to the best known version
            MockLog.assertThatLogger(
                () -> handler.handleResponse(handshakeResponse),
                TransportHandshaker.class,
                new MockLog.SeenEventExpectation(
                    "warning",
                    TransportHandshaker.class.getCanonicalName(),
                    Level.WARN,
                    Strings.format(
                        """
                            Negotiating transport handshake with remote node with version [%s/%s] received on [*] which appears to be from \
                            a chronologically-older release with a numerically-newer version compared to this node's version [%s/%s]. \
                            Upgrading to a chronologically-older release may not work reliably and is not recommended. Falling back to \
                            transport protocol version [%s].""",
                        handshakeResponse.getReleaseVersion(),
                        handshakeResponse.getTransportVersion(),
                        Build.current().version(),
                        TransportVersion.current(),
                        randomIncompatibleTransportVersion.bestKnownVersion()
                    )
                )
            );

            assertTrue(versionFuture.isDone());
            assertEquals(randomIncompatibleTransportVersion.bestKnownVersion(), versionFuture.result());
        } else {
            MockLog.assertThatLogger(
                () -> handler.handleResponse(handshakeResponse),
                TransportHandshaker.class,
                new MockLog.SeenEventExpectation(
                    "warning",
                    TransportHandshaker.class.getCanonicalName(),
                    Level.WARN,
                    "Rejecting unreadable transport handshake * incompatible wire format."
                )
            );

            assertTrue(versionFuture.isDone());
            assertThat(
                expectThrows(ExecutionException.class, IllegalStateException.class, versionFuture::result).getMessage(),
                allOf(
                    containsString("Rejecting unreadable transport handshake"),
                    containsString("[" + handshakeResponse.getReleaseVersion() + "/" + handshakeResponse.getTransportVersion() + "]"),
                    containsString("[" + Build.current().version() + "/" + TransportVersion.current() + "]"),
                    containsString("which has an incompatible wire format")
                )
            );
        }
    }

    private static TransportVersion getRandomIncompatibleTransportVersion() {
        return randomBoolean()
            // either older than MINIMUM_COMPATIBLE
            ? new TransportVersion(between(1, TransportVersions.MINIMUM_COMPATIBLE.id() - 1))
            // or between MINIMUM_COMPATIBLE and current but not known
            : randomValueOtherThanMany(
                TransportVersion::isKnown,
                () -> new TransportVersion(between(TransportVersions.MINIMUM_COMPATIBLE.id(), TransportVersion.current().id()))
            );
    }

    public void testHandshakeResponseFromNewerNode() throws Exception {
        final PlainActionFuture<TransportVersion> versionFuture = new PlainActionFuture<>();
        final long reqId = randomNonNegativeLong();
        handshaker.sendHandshake(reqId, node, channel, SAFE_AWAIT_TIMEOUT, versionFuture);
        TransportResponseHandler<TransportHandshaker.HandshakeResponse> handler = handshaker.removeHandlerForHandshake(reqId);

        assertFalse(versionFuture.isDone());

        handler.handleResponse(
            new TransportHandshaker.HandshakeResponse(
                TransportVersion.fromId(TransportVersion.current().id() + between(0, 10)),
                randomIdentifier()
            )
        );

        assertTrue(versionFuture.isDone());
        assertEquals(TransportVersion.current(), versionFuture.result());
    }

    public void testHandshakeRequestFutureVersionsCompatibility() throws IOException {
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, node, channel, new TimeValue(30, TimeUnit.SECONDS), new PlainActionFuture<>());

        verify(requestSender).sendRequest(node, channel, reqId, HANDSHAKE_REQUEST_VERSION);

        final var buildVersion = randomIdentifier();
        final var handshakeRequest = new TransportHandshaker.HandshakeRequest(TransportVersion.current(), buildVersion);
        BytesStreamOutput currentHandshakeBytes = new BytesStreamOutput();
        currentHandshakeBytes.setTransportVersion(HANDSHAKE_REQUEST_VERSION);
        handshakeRequest.writeTo(currentHandshakeBytes);

        BytesStreamOutput lengthCheckingHandshake = new BytesStreamOutput();
        BytesStreamOutput futureHandshake = new BytesStreamOutput();
        TaskId.EMPTY_TASK_ID.writeTo(lengthCheckingHandshake);
        TaskId.EMPTY_TASK_ID.writeTo(futureHandshake);
        final var extraDataSize = between(0, 1024);
        try (BytesStreamOutput internalMessage = new BytesStreamOutput()) {
            final var futureTransportVersionId = TransportVersion.current().id() + between(0, 100);
            internalMessage.writeVInt(futureTransportVersionId);
            internalMessage.writeString(buildVersion);
            lengthCheckingHandshake.writeBytesReference(internalMessage.bytes());
            internalMessage.write(new byte[extraDataSize]);
            futureHandshake.writeBytesReference(internalMessage.bytes());
        }
        StreamInput futureHandshakeStream = futureHandshake.bytes().streamInput();
        // We check that the handshake we serialize for this test equals the actual request.
        // Otherwise, we need to update the test.
        assertEquals(currentHandshakeBytes.bytes().length(), lengthCheckingHandshake.bytes().length());
        final var expectedInternalMessageSize = 4 /* transport version id */
            + (1 + buildVersion.length()) /* length prefixed release version string */
            + extraDataSize;
        assertEquals(
            1 /* EMPTY_TASK_ID */
                + (expectedInternalMessageSize < 0x80 ? 1 : 2) /* internalMessage size vInt */
                + expectedInternalMessageSize /* internalMessage */,
            futureHandshakeStream.available()
        );
        final PlainActionFuture<TransportResponse> responseFuture = new PlainActionFuture<>();
        final TestTransportChannel channel = new TestTransportChannel(responseFuture);
        handshaker.handleHandshake(channel, reqId, futureHandshakeStream);
        assertEquals(0, futureHandshakeStream.available());

        TransportHandshaker.HandshakeResponse response = (TransportHandshaker.HandshakeResponse) responseFuture.actionGet();

        assertEquals(TransportVersion.current(), response.getTransportVersion());
    }

    public void testReadV8HandshakeRequest() throws IOException {
        final var transportVersion = TransportVersionUtils.randomCompatibleVersion(random());

        final var requestPayloadStreamOutput = new BytesStreamOutput();
        requestPayloadStreamOutput.setTransportVersion(TransportHandshaker.V8_HANDSHAKE_VERSION);
        requestPayloadStreamOutput.writeVInt(transportVersion.id());

        final var requestBytesStreamOutput = new BytesStreamOutput();
        requestBytesStreamOutput.setTransportVersion(TransportHandshaker.V8_HANDSHAKE_VERSION);
        TaskId.EMPTY_TASK_ID.writeTo(requestBytesStreamOutput);
        requestBytesStreamOutput.writeBytesReference(requestPayloadStreamOutput.bytes());

        final var requestBytesStream = requestBytesStreamOutput.bytes().streamInput();
        requestBytesStream.setTransportVersion(TransportHandshaker.V8_HANDSHAKE_VERSION);
        final var handshakeRequest = new TransportHandshaker.HandshakeRequest(requestBytesStream);

        assertEquals(transportVersion, handshakeRequest.transportVersion);
        assertEquals(transportVersion.toReleaseVersion(), handshakeRequest.releaseVersion);
    }

    public void testReadV8HandshakeResponse() throws IOException {
        final var transportVersion = TransportVersionUtils.randomCompatibleVersion(random());

        final var responseBytesStreamOutput = new BytesStreamOutput();
        responseBytesStreamOutput.setTransportVersion(TransportHandshaker.V8_HANDSHAKE_VERSION);
        responseBytesStreamOutput.writeVInt(transportVersion.id());

        final var responseBytesStream = responseBytesStreamOutput.bytes().streamInput();
        responseBytesStream.setTransportVersion(TransportHandshaker.V8_HANDSHAKE_VERSION);
        final var handshakeResponse = new TransportHandshaker.HandshakeResponse(responseBytesStream);

        assertEquals(transportVersion, handshakeResponse.getTransportVersion());
        assertEquals(transportVersion.toReleaseVersion(), handshakeResponse.getReleaseVersion());
    }

    public void testReadV9HandshakeRequest() throws IOException {
        final var transportVersion = TransportVersionUtils.randomCompatibleVersion(random());
        final var releaseVersion = randomIdentifier();

        final var requestPayloadStreamOutput = new BytesStreamOutput();
        requestPayloadStreamOutput.setTransportVersion(TransportHandshaker.V9_HANDSHAKE_VERSION);
        requestPayloadStreamOutput.writeVInt(transportVersion.id());
        requestPayloadStreamOutput.writeString(releaseVersion);

        final var requestBytesStreamOutput = new BytesStreamOutput();
        requestBytesStreamOutput.setTransportVersion(TransportHandshaker.V9_HANDSHAKE_VERSION);
        TaskId.EMPTY_TASK_ID.writeTo(requestBytesStreamOutput);
        requestBytesStreamOutput.writeBytesReference(requestPayloadStreamOutput.bytes());

        final var requestBytesStream = requestBytesStreamOutput.bytes().streamInput();
        requestBytesStream.setTransportVersion(TransportHandshaker.V9_HANDSHAKE_VERSION);
        final var handshakeRequest = new TransportHandshaker.HandshakeRequest(requestBytesStream);

        assertEquals(transportVersion, handshakeRequest.transportVersion);
        assertEquals(releaseVersion, handshakeRequest.releaseVersion);
    }

    public void testReadV9HandshakeResponse() throws IOException {
        final var transportVersion = TransportVersionUtils.randomCompatibleVersion(random());
        final var releaseVersion = randomIdentifier();

        final var responseBytesStreamOutput = new BytesStreamOutput();
        responseBytesStreamOutput.setTransportVersion(TransportHandshaker.V9_HANDSHAKE_VERSION);
        responseBytesStreamOutput.writeVInt(transportVersion.id());
        responseBytesStreamOutput.writeString(releaseVersion);

        final var responseBytesStream = responseBytesStreamOutput.bytes().streamInput();
        responseBytesStream.setTransportVersion(TransportHandshaker.V9_HANDSHAKE_VERSION);
        final var handshakeResponse = new TransportHandshaker.HandshakeResponse(responseBytesStream);

        assertEquals(transportVersion, handshakeResponse.getTransportVersion());
        assertEquals(releaseVersion, handshakeResponse.getReleaseVersion());
    }

    public void testHandshakeError() throws IOException {
        PlainActionFuture<TransportVersion> versionFuture = new PlainActionFuture<>();
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, node, channel, new TimeValue(30, TimeUnit.SECONDS), versionFuture);

        verify(requestSender).sendRequest(node, channel, reqId, HANDSHAKE_REQUEST_VERSION);

        assertFalse(versionFuture.isDone());

        TransportResponseHandler<TransportHandshaker.HandshakeResponse> handler = handshaker.removeHandlerForHandshake(reqId);
        handler.handleException(new TransportException("failed"));

        assertTrue(versionFuture.isDone());
        IllegalStateException ise = expectThrows(IllegalStateException.class, versionFuture::actionGet);
        assertThat(ise.getMessage(), containsString("handshake failed"));
    }

    public void testSendRequestThrowsException() throws IOException {
        PlainActionFuture<TransportVersion> versionFuture = new PlainActionFuture<>();
        long reqId = randomLongBetween(1, 10);
        doThrow(new IOException("boom")).when(requestSender).sendRequest(node, channel, reqId, HANDSHAKE_REQUEST_VERSION);

        handshaker.sendHandshake(reqId, node, channel, new TimeValue(30, TimeUnit.SECONDS), versionFuture);

        assertTrue(versionFuture.isDone());
        ConnectTransportException cte = expectThrows(ConnectTransportException.class, versionFuture::actionGet);
        assertThat(cte.getMessage(), containsString("failure to send internal:tcp/handshake"));
        assertNull(handshaker.removeHandlerForHandshake(reqId));
    }

    public void testHandshakeTimeout() throws IOException {
        PlainActionFuture<TransportVersion> versionFuture = new PlainActionFuture<>();
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, node, channel, new TimeValue(100, TimeUnit.MILLISECONDS), versionFuture);

        verify(requestSender).sendRequest(node, channel, reqId, HANDSHAKE_REQUEST_VERSION);

        ConnectTransportException cte = expectThrows(ConnectTransportException.class, versionFuture::actionGet);
        assertThat(cte.getMessage(), containsString("handshake_timeout"));

        assertNull(handshaker.removeHandlerForHandshake(reqId));
    }
}
