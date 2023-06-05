/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

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

    @Override
    public void setUp() throws Exception {
        super.setUp();
        String nodeId = "node-id";
        channel = mock(TcpChannel.class);
        requestSender = mock(TransportHandshaker.HandshakeRequestSender.class);
        node = new DiscoveryNode(
            nodeId,
            nodeId,
            nodeId,
            "host",
            "host_address",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            null
        );
        threadPool = new TestThreadPool("thread-poll");
        handshaker = new TransportHandshaker(TransportVersion.CURRENT, threadPool, requestSender, false);
    }

    @Override
    public void tearDown() throws Exception {
        threadPool.shutdown();
        super.tearDown();
    }

    public void testHandshakeRequestAndResponse() throws IOException {
        PlainActionFuture<TransportVersion> versionFuture = PlainActionFuture.newFuture();
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, node, channel, new TimeValue(30, TimeUnit.SECONDS), versionFuture);

        verify(requestSender).sendRequest(node, channel, reqId, TransportHandshaker.REQUEST_HANDSHAKE_VERSION);

        assertFalse(versionFuture.isDone());

        TransportHandshaker.HandshakeRequest handshakeRequest = new TransportHandshaker.HandshakeRequest(TransportVersion.CURRENT);
        BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
        handshakeRequest.writeTo(bytesStreamOutput);
        StreamInput input = bytesStreamOutput.bytes().streamInput();
        final PlainActionFuture<TransportResponse> responseFuture = PlainActionFuture.newFuture();
        final TestTransportChannel channel = new TestTransportChannel(responseFuture);
        handshaker.handleHandshake(channel, reqId, input);

        TransportResponseHandler<TransportHandshaker.HandshakeResponse> handler = handshaker.removeHandlerForHandshake(reqId);
        handler.handleResponse((TransportHandshaker.HandshakeResponse) responseFuture.actionGet());

        assertTrue(versionFuture.isDone());
        assertEquals(TransportVersion.CURRENT, versionFuture.actionGet());
    }

    public void testHandshakeRequestFutureVersionsCompatibility() throws IOException {
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, node, channel, new TimeValue(30, TimeUnit.SECONDS), PlainActionFuture.newFuture());

        verify(requestSender).sendRequest(node, channel, reqId, TransportHandshaker.REQUEST_HANDSHAKE_VERSION);

        TransportHandshaker.HandshakeRequest handshakeRequest = new TransportHandshaker.HandshakeRequest(TransportVersion.CURRENT);
        BytesStreamOutput currentHandshakeBytes = new BytesStreamOutput();
        handshakeRequest.writeTo(currentHandshakeBytes);

        BytesStreamOutput lengthCheckingHandshake = new BytesStreamOutput();
        BytesStreamOutput futureHandshake = new BytesStreamOutput();
        TaskId.EMPTY_TASK_ID.writeTo(lengthCheckingHandshake);
        TaskId.EMPTY_TASK_ID.writeTo(futureHandshake);
        try (BytesStreamOutput internalMessage = new BytesStreamOutput()) {
            Version.writeVersion(Version.CURRENT, internalMessage);
            lengthCheckingHandshake.writeBytesReference(internalMessage.bytes());
            internalMessage.write(new byte[1024]);
            futureHandshake.writeBytesReference(internalMessage.bytes());
        }
        StreamInput futureHandshakeStream = futureHandshake.bytes().streamInput();
        // We check that the handshake we serialize for this test equals the actual request.
        // Otherwise, we need to update the test.
        assertEquals(currentHandshakeBytes.bytes().length(), lengthCheckingHandshake.bytes().length());
        assertEquals(1031, futureHandshakeStream.available());
        final PlainActionFuture<TransportResponse> responseFuture = PlainActionFuture.newFuture();
        final TestTransportChannel channel = new TestTransportChannel(responseFuture);
        handshaker.handleHandshake(channel, reqId, futureHandshakeStream);
        assertEquals(0, futureHandshakeStream.available());

        TransportHandshaker.HandshakeResponse response = (TransportHandshaker.HandshakeResponse) responseFuture.actionGet();

        assertEquals(TransportVersion.CURRENT, response.getResponseVersion());
    }

    public void testHandshakeError() throws IOException {
        PlainActionFuture<TransportVersion> versionFuture = PlainActionFuture.newFuture();
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, node, channel, new TimeValue(30, TimeUnit.SECONDS), versionFuture);

        verify(requestSender).sendRequest(node, channel, reqId, TransportHandshaker.REQUEST_HANDSHAKE_VERSION);

        assertFalse(versionFuture.isDone());

        TransportResponseHandler<TransportHandshaker.HandshakeResponse> handler = handshaker.removeHandlerForHandshake(reqId);
        handler.handleException(new TransportException("failed"));

        assertTrue(versionFuture.isDone());
        IllegalStateException ise = expectThrows(IllegalStateException.class, versionFuture::actionGet);
        assertThat(ise.getMessage(), containsString("handshake failed"));
    }

    public void testSendRequestThrowsException() throws IOException {
        PlainActionFuture<TransportVersion> versionFuture = PlainActionFuture.newFuture();
        long reqId = randomLongBetween(1, 10);
        doThrow(new IOException("boom")).when(requestSender)
            .sendRequest(node, channel, reqId, TransportHandshaker.REQUEST_HANDSHAKE_VERSION);

        handshaker.sendHandshake(reqId, node, channel, new TimeValue(30, TimeUnit.SECONDS), versionFuture);

        assertTrue(versionFuture.isDone());
        ConnectTransportException cte = expectThrows(ConnectTransportException.class, versionFuture::actionGet);
        assertThat(cte.getMessage(), containsString("failure to send internal:tcp/handshake"));
        assertNull(handshaker.removeHandlerForHandshake(reqId));
    }

    public void testHandshakeTimeout() throws IOException {
        PlainActionFuture<TransportVersion> versionFuture = PlainActionFuture.newFuture();
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, node, channel, new TimeValue(100, TimeUnit.MILLISECONDS), versionFuture);

        verify(requestSender).sendRequest(node, channel, reqId, TransportHandshaker.REQUEST_HANDSHAKE_VERSION);

        ConnectTransportException cte = expectThrows(ConnectTransportException.class, versionFuture::actionGet);
        assertThat(cte.getMessage(), containsString("handshake_timeout"));

        assertNull(handshaker.removeHandlerForHandshake(reqId));
    }
}
