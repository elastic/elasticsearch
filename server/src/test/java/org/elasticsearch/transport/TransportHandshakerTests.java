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

import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransportHandshakerTests extends ESTestCase {

    private TransportHandshaker handshaker;
    private DiscoveryNode remoteNode;
    private TcpChannel channel;
    private TestThreadPool threadPool;
    private TransportHandshaker.HandshakeRequestSender requestSender;
    private TransportHandshaker.HandshakeResponseSender responseSender;
    private ClusterName clusterName;
    private DiscoveryNode localNode;

    @Override
    @SuppressForbidden(reason = "Allow accessing localhost")
    public void setUp() throws Exception {
        super.setUp();
        String nodeId = "remote-node-id";
        channel = mock(TcpChannel.class);
        requestSender = mock(TransportHandshaker.HandshakeRequestSender.class);
        responseSender = mock(TransportHandshaker.HandshakeResponseSender.class);
        remoteNode = new DiscoveryNode(nodeId, nodeId, nodeId, "host", "host_address", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);
        threadPool = new TestThreadPool("thread-poll");
        clusterName = new ClusterName("cluster");
        localNode = new DiscoveryNode("local-node-id", new TransportAddress(InetAddress.getLocalHost(), 0), Version.CURRENT);
        handshaker = new TransportHandshaker(clusterName, Version.CURRENT, threadPool, requestSender, responseSender);
        handshaker.setLocalNode(localNode);
    }

    @Override
    public void tearDown() throws Exception {
        threadPool.shutdown();
        super.tearDown();
    }

    public void testHandshakeRequestAndResponse() throws IOException {
        PlainActionFuture<Version> versionFuture = PlainActionFuture.newFuture();
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, remoteNode, channel, new TimeValue(30, TimeUnit.SECONDS), versionFuture);

        verify(requestSender).sendRequest(remoteNode, channel, reqId, Version.CURRENT.minimumCompatibilityVersion());

        assertFalse(versionFuture.isDone());

        TcpChannel mockChannel = mock(TcpChannel.class);
        TransportHandshaker.HandshakeRequest handshakeRequest = new TransportHandshaker.HandshakeRequest(Version.CURRENT);
        BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
        handshakeRequest.writeTo(bytesStreamOutput);
        StreamInput input = bytesStreamOutput.bytes().streamInput();
        handshaker.handleHandshake(Version.CURRENT, mockChannel, reqId, input);


        ArgumentCaptor<TransportResponse> responseCaptor = ArgumentCaptor.forClass(TransportResponse.class);
        verify(responseSender).sendResponse(eq(Version.CURRENT),  eq(mockChannel), responseCaptor.capture(), eq(reqId));

        TransportResponseHandler<TransportHandshaker.HandshakeResponse> handler = handshaker.removeHandlerForHandshake(reqId);
        handler.handleResponse((TransportHandshaker.HandshakeResponse) responseCaptor.getValue());

        assertTrue(versionFuture.isDone());
        assertEquals(Version.CURRENT, versionFuture.actionGet());
        TransportHandshaker.HandshakeResponse response = (TransportHandshaker.HandshakeResponse) responseCaptor.getValue();
        assertEquals(Version.CURRENT, response.getVersion());
        assertEquals(clusterName, response.getClusterName());
        assertEquals(localNode, response.getDiscoveryNode());
    }

    public void testHandshakeRequestAndResponsePreV7_6() throws IOException {
        PlainActionFuture<Version> versionFuture = PlainActionFuture.newFuture();
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, remoteNode, channel, new TimeValue(30, TimeUnit.SECONDS), versionFuture);

        TransportResponseHandler<TransportHandshaker.HandshakeResponse> handler = handshaker.removeHandlerForHandshake(reqId);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            new TransportHandshaker.HandshakeResponse(Version.V_7_5_0, Version.V_7_5_0, clusterName, localNode).writeTo(out);
            TransportHandshaker.HandshakeResponse response = handler.read(out.bytes().streamInput());
            assertEquals(Version.V_7_5_0, response.getVersion());
            // When writing or reading a 6.6 stream, these are not serialized
            assertNull(response.getDiscoveryNode());
            assertNull(response.getClusterName());
        }
    }

    public void testHandshakeRequestFutureVersionsCompatibility() throws IOException {
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, remoteNode, channel, new TimeValue(30, TimeUnit.SECONDS), PlainActionFuture.newFuture());

        verify(requestSender).sendRequest(remoteNode, channel, reqId, Version.CURRENT.minimumCompatibilityVersion());

        TcpChannel mockChannel = mock(TcpChannel.class);
        TransportHandshaker.HandshakeRequest handshakeRequest = new TransportHandshaker.HandshakeRequest(Version.CURRENT);
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
        handshaker.handleHandshake(Version.CURRENT, mockChannel, reqId, futureHandshakeStream);
        assertEquals(0, futureHandshakeStream.available());


        ArgumentCaptor<TransportResponse> responseCaptor = ArgumentCaptor.forClass(TransportResponse.class);
        verify(responseSender).sendResponse(eq(Version.CURRENT), eq(mockChannel), responseCaptor.capture(), eq(reqId));

        TransportHandshaker.HandshakeResponse response = (TransportHandshaker.HandshakeResponse) responseCaptor.getValue();

        assertEquals(Version.CURRENT, response.getVersion());
    }

    public void testHandshakeError() throws IOException {
        PlainActionFuture<Version> versionFuture = PlainActionFuture.newFuture();
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, remoteNode, channel, new TimeValue(30, TimeUnit.SECONDS), versionFuture);

        verify(requestSender).sendRequest(remoteNode, channel, reqId, Version.CURRENT.minimumCompatibilityVersion());

        assertFalse(versionFuture.isDone());

        TransportResponseHandler<TransportHandshaker.HandshakeResponse> handler = handshaker.removeHandlerForHandshake(reqId);
        handler.handleException(new TransportException("failed"));

        assertTrue(versionFuture.isDone());
        IllegalStateException ise = expectThrows(IllegalStateException.class, versionFuture::actionGet);
        assertThat(ise.getMessage(), containsString("handshake failed"));
    }

    public void testSendRequestThrowsException() throws IOException {
        PlainActionFuture<Version> versionFuture = PlainActionFuture.newFuture();
        long reqId = randomLongBetween(1, 10);
        Version compatibilityVersion = Version.CURRENT.minimumCompatibilityVersion();
        doThrow(new IOException("boom")).when(requestSender).sendRequest(remoteNode, channel, reqId, compatibilityVersion);

        handshaker.sendHandshake(reqId, remoteNode, channel, new TimeValue(30, TimeUnit.SECONDS), versionFuture);

        assertTrue(versionFuture.isDone());
        ConnectTransportException cte = expectThrows(ConnectTransportException.class, versionFuture::actionGet);
        assertThat(cte.getMessage(), containsString("failure to send internal:tcp/handshake"));
        assertNull(handshaker.removeHandlerForHandshake(reqId));
    }

    public void testHandshakeTimeout() throws IOException {
        PlainActionFuture<Version> versionFuture = PlainActionFuture.newFuture();
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, remoteNode, channel, new TimeValue(100, TimeUnit.MILLISECONDS), versionFuture);

        verify(requestSender).sendRequest(remoteNode, channel, reqId, Version.CURRENT.minimumCompatibilityVersion());

        ConnectTransportException cte = expectThrows(ConnectTransportException.class, versionFuture::actionGet);
        assertThat(cte.getMessage(), containsString("handshake_timeout"));

        assertNull(handshaker.removeHandlerForHandshake(reqId));
    }
}
