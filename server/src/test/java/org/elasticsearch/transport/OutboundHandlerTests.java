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
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class OutboundHandlerTests extends ESTestCase {

    private TestThreadPool threadPool = new TestThreadPool(getClass().getName());;
    private AtomicReference<Supplier<BytesReference>> messageCaptor = new AtomicReference<>();
    private OutboundHandler handler;
    private FakeTcpChannel fakeTcpChannel;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        TransportLogger transportLogger = new TransportLogger();
        fakeTcpChannel = new FakeTcpChannel(randomBoolean(), messageCaptor);
        handler = new OutboundHandler(threadPool, BigArrays.NON_RECYCLING_INSTANCE, transportLogger);
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    @SuppressWarnings("unchecked")
    public void testSendRawBytes() {
        BytesArray bytesArray = new BytesArray("message".getBytes(StandardCharsets.UTF_8));

        AtomicBoolean isSuccess = new AtomicBoolean(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<Void> listener = ActionListener.wrap((v) -> isSuccess.set(true), exception::set);
        handler.sendBytes(fakeTcpChannel, bytesArray, listener);

        ActionListener<Void> sendContext = (ActionListener<Void>) messageCaptor.get();
        BytesReference reference = messageCaptor.get().get();
        if (randomBoolean()) {
            sendContext.onResponse(null);
            assertTrue(isSuccess.get());
            assertNull(exception.get());
        } else {
            IOException e = new IOException("failed");
            sendContext.onFailure(e);
            assertFalse(isSuccess.get());
            assertSame(e, exception.get());
        }

        assertEquals(bytesArray, reference);
    }

    @SuppressWarnings("unchecked")
    public void testSendMessage() throws IOException {
        OutboundMessage message;
        ThreadContext threadContext = threadPool.getThreadContext();
        Version version = Version.CURRENT;
        String actionName = "handshake";
        long requestId = randomLongBetween(0, 300);
        boolean isHandshake = randomBoolean();
        boolean compress = randomBoolean();
        Writeable writeable = new Message("message");

        if (randomBoolean()) {
            message = new OutboundMessage.Request(threadContext, new String[0], writeable, version, actionName, requestId, isHandshake,
                compress);
        } else {
            message = new OutboundMessage.Response(threadContext, new HashSet<>(), writeable, version, requestId, isHandshake, compress);
        }

        AtomicBoolean isSuccess = new AtomicBoolean(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<Void> listener = ActionListener.wrap((v) -> isSuccess.set(true), exception::set);
        handler.sendMessage(fakeTcpChannel, message, listener);

        ActionListener<Void> sendContext = (ActionListener<Void>) messageCaptor.get();
        BytesReference reference = messageCaptor.get().get();
        if (randomBoolean()) {
            sendContext.onResponse(null);
            assertTrue(isSuccess.get());
            assertNull(exception.get());
        } else {
            IOException e = new IOException("failed");
            sendContext.onFailure(e);
            assertFalse(isSuccess.get());
            assertSame(e, exception.get());
        }

        OutboundMessage deserialized = OutboundMessage.deserialize(reference.streamInput());
        // TODO: Implement test
        assertEquals(null, deserialized);
    }

    private static final class Message extends TransportRequest {

        public String value;

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
