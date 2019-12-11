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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;

public class OutboundHandlerTests extends ESTestCase {

    private final TestThreadPool threadPool = new TestThreadPool(getClass().getName());
    private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
    private final TransportRequestOptions options = TransportRequestOptions.EMPTY;
    private OutboundHandler handler;
    private FakeTcpChannel channel;
    private DiscoveryNode node;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        channel = new FakeTcpChannel(randomBoolean(), buildNewFakeTransportAddress().address(), buildNewFakeTransportAddress().address());
        TransportAddress transportAddress = buildNewFakeTransportAddress();
        node = new DiscoveryNode("", transportAddress, Version.CURRENT);
        handler = new OutboundHandler("node", Version.CURRENT, threadPool, BigArrays.NON_RECYCLING_INSTANCE);
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testSendRawBytes() {
        BytesArray bytesArray = new BytesArray("message".getBytes(StandardCharsets.UTF_8));

        AtomicBoolean isSuccess = new AtomicBoolean(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<Void> listener = ActionListener.wrap((v) -> isSuccess.set(true), exception::set);
        handler.sendBytes(channel, bytesArray, listener);

        BytesReference reference = channel.getMessageCaptor().get();
        ActionListener<Void> sendListener  = channel.getListenerCaptor().get();
        if (randomBoolean()) {
            sendListener.onResponse(null);
            assertTrue(isSuccess.get());
            assertNull(exception.get());
        } else {
            IOException e = new IOException("failed");
            sendListener.onFailure(e);
            assertFalse(isSuccess.get());
            assertSame(e, exception.get());
        }

        assertEquals(bytesArray, reference);
    }

    public void testSendRequest() throws IOException {
        ThreadContext threadContext = threadPool.getThreadContext();
        Version version = randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion());
        String action = "handshake";
        long requestId = randomLongBetween(0, 300);
        boolean isHandshake = randomBoolean();
        boolean compress = randomBoolean();
        String value = "message";
        threadContext.putHeader("header", "header_value");
        Request request = new Request(value);

        AtomicReference<DiscoveryNode> nodeRef = new AtomicReference<>();
        AtomicLong requestIdRef = new AtomicLong();
        AtomicReference<String> actionRef = new AtomicReference<>();
        AtomicReference<TransportRequest> requestRef = new AtomicReference<>();
        handler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onRequestSent(DiscoveryNode node, long requestId, String action, TransportRequest request,
                                      TransportRequestOptions options) {
                nodeRef.set(node);
                requestIdRef.set(requestId);
                actionRef.set(action);
                requestRef.set(request);
            }
        });
        handler.sendRequest(node, channel, requestId, action, request, options, version, compress, isHandshake);

        BytesReference reference = channel.getMessageCaptor().get();
        ActionListener<Void> sendListener  = channel.getListenerCaptor().get();
        if (randomBoolean()) {
            sendListener.onResponse(null);
        } else {
            sendListener.onFailure(new IOException("failed"));
        }
        assertEquals(node, nodeRef.get());
        assertEquals(requestId, requestIdRef.get());
        assertEquals(action, actionRef.get());
        assertEquals(request, requestRef.get());

        InboundMessage.Reader reader = new InboundMessage.Reader(Version.CURRENT, namedWriteableRegistry, threadPool.getThreadContext());
        try (InboundMessage inboundMessage = reader.deserialize(reference.slice(6, reference.length() - 6))) {
            assertEquals(version, inboundMessage.getVersion());
            assertEquals(requestId, inboundMessage.getRequestId());
            assertTrue(inboundMessage.isRequest());
            assertFalse(inboundMessage.isResponse());
            if (isHandshake) {
                assertTrue(inboundMessage.isHandshake());
            } else {
                assertFalse(inboundMessage.isHandshake());
            }
            if (compress) {
                assertTrue(inboundMessage.isCompress());
            } else {
                assertFalse(inboundMessage.isCompress());
            }
            InboundMessage.Request inboundRequest = (InboundMessage.Request) inboundMessage;

            Request readMessage = new Request(inboundMessage.getStreamInput());
            assertEquals(value, readMessage.value);

            try (ThreadContext.StoredContext existing = threadContext.stashContext()) {
                ThreadContext.StoredContext storedContext = inboundMessage.getStoredContext();
                assertNull(threadContext.getHeader("header"));
                storedContext.restore();
                assertEquals("header_value", threadContext.getHeader("header"));
            }
        }
    }

    public void testSendResponse() throws IOException {
        ThreadContext threadContext = threadPool.getThreadContext();
        Version version = randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion());
        String action = "handshake";
        long requestId = randomLongBetween(0, 300);
        boolean isHandshake = randomBoolean();
        boolean compress = randomBoolean();
        String value = "message";
        threadContext.putHeader("header", "header_value");
        Response response = new Response(value);

        AtomicLong requestIdRef = new AtomicLong();
        AtomicReference<String> actionRef = new AtomicReference<>();
        AtomicReference<TransportResponse> responseRef = new AtomicReference<>();
        handler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onResponseSent(long requestId, String action, TransportResponse response) {
                requestIdRef.set(requestId);
                actionRef.set(action);
                responseRef.set(response);
            }
        });
        handler.sendResponse(version, channel, requestId, action, response, compress, isHandshake);

        BytesReference reference = channel.getMessageCaptor().get();
        ActionListener<Void> sendListener  = channel.getListenerCaptor().get();
        if (randomBoolean()) {
            sendListener.onResponse(null);
        } else {
            sendListener.onFailure(new IOException("failed"));
        }
        assertEquals(requestId, requestIdRef.get());
        assertEquals(action, actionRef.get());
        assertEquals(response, responseRef.get());

        InboundMessage.Reader reader = new InboundMessage.Reader(Version.CURRENT, namedWriteableRegistry, threadPool.getThreadContext());
        try (InboundMessage inboundMessage = reader.deserialize(reference.slice(6, reference.length() - 6))) {
            assertEquals(version, inboundMessage.getVersion());
            assertEquals(requestId, inboundMessage.getRequestId());
            assertFalse(inboundMessage.isRequest());
            assertTrue(inboundMessage.isResponse());
            if (isHandshake) {
                assertTrue(inboundMessage.isHandshake());
            } else {
                assertFalse(inboundMessage.isHandshake());
            }
            if (compress) {
                assertTrue(inboundMessage.isCompress());
            } else {
                assertFalse(inboundMessage.isCompress());
            }

            InboundMessage.Response inboundResponse = (InboundMessage.Response) inboundMessage;
            assertFalse(inboundResponse.isError());

            Response readMessage = new Response(inboundMessage.getStreamInput());
            assertEquals(value, readMessage.value);

            try (ThreadContext.StoredContext existing = threadContext.stashContext()) {
                ThreadContext.StoredContext storedContext = inboundMessage.getStoredContext();
                assertNull(threadContext.getHeader("header"));
                storedContext.restore();
                assertEquals("header_value", threadContext.getHeader("header"));
            }
        }
    }

    public void testErrorResponse() throws IOException {
        ThreadContext threadContext = threadPool.getThreadContext();
        Version version = randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion());
        String action = "handshake";
        long requestId = randomLongBetween(0, 300);
        threadContext.putHeader("header", "header_value");
        ElasticsearchException error = new ElasticsearchException("boom");

        AtomicLong requestIdRef = new AtomicLong();
        AtomicReference<String> actionRef = new AtomicReference<>();
        AtomicReference<Exception> responseRef = new AtomicReference<>();
        handler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onResponseSent(long requestId, String action, Exception error) {
                requestIdRef.set(requestId);
                actionRef.set(action);
                responseRef.set(error);
            }
        });
        handler.sendErrorResponse(version, channel, requestId, action, error);

        BytesReference reference = channel.getMessageCaptor().get();
        ActionListener<Void> sendListener  = channel.getListenerCaptor().get();
        if (randomBoolean()) {
            sendListener.onResponse(null);
        } else {
            sendListener.onFailure(new IOException("failed"));
        }
        assertEquals(requestId, requestIdRef.get());
        assertEquals(action, actionRef.get());
        assertEquals(error, responseRef.get());

        InboundMessage.Reader reader = new InboundMessage.Reader(Version.CURRENT, namedWriteableRegistry, threadPool.getThreadContext());
        try (InboundMessage inboundMessage = reader.deserialize(reference.slice(6, reference.length() - 6))) {
            assertEquals(version, inboundMessage.getVersion());
            assertEquals(requestId, inboundMessage.getRequestId());
            assertFalse(inboundMessage.isRequest());
            assertTrue(inboundMessage.isResponse());
            assertFalse(inboundMessage.isCompress());
            assertFalse(inboundMessage.isHandshake());

            InboundMessage.Response inboundResponse = (InboundMessage.Response) inboundMessage;
            assertTrue(inboundResponse.isError());

            RemoteTransportException remoteException = inboundMessage.getStreamInput().readException();
            assertThat(remoteException.getCause(), instanceOf(ElasticsearchException.class));
            assertEquals(remoteException.getCause().getMessage(), "boom");
            assertEquals(action, remoteException.action());
            assertEquals(channel.getLocalAddress(), remoteException.address().address());

            try (ThreadContext.StoredContext existing = threadContext.stashContext()) {
                ThreadContext.StoredContext storedContext = inboundMessage.getStoredContext();
                assertNull(threadContext.getHeader("header"));
                storedContext.restore();
                assertEquals("header_value", threadContext.getHeader("header"));
            }
        }
    }

    private static final class Request extends TransportRequest {

        public String value;

        private Request(StreamInput in) throws IOException {
            value = in.readString();
        }

        private Request(String value) {
            this.value = value;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(value);
        }
    }

    private static final class Response extends TransportResponse {

        public String value;

        private Response(StreamInput in) throws IOException {
            super(in);
            value = in.readString();
        }

        private Response(String value) {
            this.value = value;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(value);
        }
    }
}
