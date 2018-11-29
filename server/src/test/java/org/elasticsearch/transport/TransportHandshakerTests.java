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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransportHandshakerTests extends ESTestCase {

    private TcpTransportHandshaker handshaker;
    private DiscoveryNode node;
    private TcpChannel channel;
    private TestThreadPool threadPool;
    private TcpTransportHandshaker.HandshakeRequestSender requestSender;
    private TcpTransportHandshaker.HandshakeResponseSender responseSender;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        String nodeId = "node-id";
        channel = mock(TcpChannel.class);
        requestSender = mock(TcpTransportHandshaker.HandshakeRequestSender.class);
        responseSender = mock(TcpTransportHandshaker.HandshakeResponseSender.class);
        node = new DiscoveryNode(nodeId, nodeId, nodeId, "host", "host_address", buildNewFakeTransportAddress(), Collections.emptyMap(),
            Collections.emptySet(), Version.CURRENT);
        threadPool = new TestThreadPool("thread-poll");
        handshaker = new TcpTransportHandshaker(Version.CURRENT, threadPool, requestSender, responseSender);
    }

    @Override
    public void tearDown() throws Exception {
        threadPool.shutdown();
        super.tearDown();
    }

    public void testHandshakeRequestAndResponse() throws IOException {
        PlainActionFuture<Version> versionFuture = PlainActionFuture.newFuture();
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, node, channel, new TimeValue(30, TimeUnit.SECONDS), versionFuture);

        verify(requestSender).sendRequest(node, channel, reqId, Version.CURRENT.minimumCompatibilityVersion());

        assertFalse(versionFuture.isDone());

        TcpChannel mockChannel = mock(TcpChannel.class);
        handshaker.handleHandshake(Version.CURRENT, Collections.emptySet(), mockChannel, reqId);


        ArgumentCaptor<TransportResponse> responseCaptor = ArgumentCaptor.forClass(TransportResponse.class);
        verify(responseSender).sendResponse(eq(Version.CURRENT), eq(Collections.emptySet()), eq(mockChannel), responseCaptor.capture(),
            eq(reqId));

        TransportResponseHandler<TcpTransportHandshaker.VersionHandshakeResponse> handler = handshaker.removeHandlerForHandshake(reqId);
        handler.handleResponse((TcpTransportHandshaker.VersionHandshakeResponse) responseCaptor.getValue());

        assertTrue(versionFuture.isDone());
        assertEquals(Version.CURRENT, versionFuture.actionGet());
    }

    public void testHandshakeError() throws IOException {
        PlainActionFuture<Version> versionFuture = PlainActionFuture.newFuture();
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, node, channel, new TimeValue(30, TimeUnit.SECONDS), versionFuture);

        verify(requestSender).sendRequest(node, channel, reqId, Version.CURRENT.minimumCompatibilityVersion());

        assertFalse(versionFuture.isDone());

        TransportResponseHandler<TcpTransportHandshaker.VersionHandshakeResponse> handler = handshaker.removeHandlerForHandshake(reqId);
        handler.handleException(new TransportException("failed"));

        assertTrue(versionFuture.isDone());
        IllegalStateException ise = expectThrows(IllegalStateException.class, versionFuture::actionGet);
        assertThat(ise.getMessage(), containsString("handshake failed"));
    }

    public void testSendRequestThrowsException() throws IOException {
        PlainActionFuture<Version> versionFuture = PlainActionFuture.newFuture();
        long reqId = randomLongBetween(1, 10);
        Version compatibilityVersion = Version.CURRENT.minimumCompatibilityVersion();
        doThrow(new IOException("boom")).when(requestSender).sendRequest(node, channel, reqId, compatibilityVersion);

        handshaker.sendHandshake(reqId, node, channel, new TimeValue(30, TimeUnit.SECONDS), versionFuture);


        assertTrue(versionFuture.isDone());
        ConnectTransportException cte = expectThrows(ConnectTransportException.class, versionFuture::actionGet);
        assertThat(cte.getMessage(), containsString("failure to send internal:tcp/handshake"));
        assertNull(handshaker.removeHandlerForHandshake(reqId));
    }

    public void testHandshakeTimeout() throws IOException {
        PlainActionFuture<Version> versionFuture = PlainActionFuture.newFuture();
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, node, channel, new TimeValue(100, TimeUnit.MILLISECONDS), versionFuture);

        verify(requestSender).sendRequest(node, channel, reqId, Version.CURRENT.minimumCompatibilityVersion());

        ConnectTransportException cte = expectThrows(ConnectTransportException.class, versionFuture::actionGet);
        assertThat(cte.getMessage(), containsString("handshake_timeout"));

        assertNull(handshaker.removeHandlerForHandshake(reqId));
    }
}
