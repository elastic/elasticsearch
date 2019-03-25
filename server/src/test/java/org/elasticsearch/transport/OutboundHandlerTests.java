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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
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
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class OutboundHandlerTests extends ESTestCase {

    private final TestThreadPool threadPool = new TestThreadPool(getClass().getName());
    private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
    private OutboundHandler handler;
    private FakeTcpChannel fakeTcpChannel;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        TransportLogger transportLogger = new TransportLogger();
        fakeTcpChannel = new FakeTcpChannel(randomBoolean());
        handler = new OutboundHandler(threadPool, BigArrays.NON_RECYCLING_INSTANCE, transportLogger);
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
        handler.sendBytes(fakeTcpChannel, bytesArray, listener);

        BytesReference reference = fakeTcpChannel.getMessageCaptor().get();
        ActionListener<Void> sendListener  = fakeTcpChannel.getListenerCaptor().get();
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

    public void testSendMessage() throws IOException {
        OutboundMessage message;
        ThreadContext threadContext = threadPool.getThreadContext();
        Version version = Version.CURRENT;
        String actionName = "handshake";
        long requestId = randomLongBetween(0, 300);
        boolean isHandshake = randomBoolean();
        boolean compress = randomBoolean();
        String value = "message";
        threadContext.putHeader("header", "header_value");
        Writeable writeable = new Message(value);

        boolean isRequest = randomBoolean();
        if (isRequest) {
            message = new OutboundMessage.Request(threadContext, new String[0], writeable, version, actionName, requestId, isHandshake,
                compress);
        } else {
            message = new OutboundMessage.Response(threadContext, new HashSet<>(), writeable, version, requestId, isHandshake, compress);
        }

        AtomicBoolean isSuccess = new AtomicBoolean(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<Void> listener = ActionListener.wrap((v) -> isSuccess.set(true), exception::set);
        handler.sendMessage(fakeTcpChannel, message, listener);

        BytesReference reference = fakeTcpChannel.getMessageCaptor().get();
        ActionListener<Void> sendListener  = fakeTcpChannel.getListenerCaptor().get();
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

        InboundMessage.Reader reader = new InboundMessage.Reader(Version.CURRENT, namedWriteableRegistry, threadPool.getThreadContext());
        try (InboundMessage inboundMessage = reader.deserialize(reference.slice(6, reference.length() - 6))) {
            assertEquals(version, inboundMessage.getVersion());
            assertEquals(requestId, inboundMessage.getRequestId());
            if (isRequest) {
                assertTrue(inboundMessage.isRequest());
                assertFalse(inboundMessage.isResponse());
            } else {
                assertTrue(inboundMessage.isResponse());
                assertFalse(inboundMessage.isRequest());
            }
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
            Message readMessage = new Message();
            readMessage.readFrom(inboundMessage.getStreamInput());
            assertEquals(value, readMessage.value);

            try (ThreadContext.StoredContext existing = threadContext.stashContext()) {
                ThreadContext.StoredContext storedContext = inboundMessage.getStoredContext();
                assertNull(threadContext.getHeader("header"));
                storedContext.restore();
                assertEquals("header_value", threadContext.getHeader("header"));
            }
        }
    }

    private static final class Message extends TransportMessage {

        public String value;

        private Message() {
        }

        private Message(String value) {
            this.value = value;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            value = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(value);
        }
    }
}
