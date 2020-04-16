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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;

public class InboundHandlerTests extends ESTestCase {

    private final TestThreadPool threadPool = new TestThreadPool(getClass().getName());
    private final Version version = Version.CURRENT;

    private TaskManager taskManager;
    private Transport.ResponseHandlers responseHandlers;
    private Transport.RequestHandlers requestHandlers;
    private InboundHandler handler;
    private FakeTcpChannel channel;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        taskManager = new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet());
        channel = new FakeTcpChannel(randomBoolean(), buildNewFakeTransportAddress().address(), buildNewFakeTransportAddress().address());
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        TransportHandshaker handshaker = new TransportHandshaker(version, threadPool, (n, c, r, v) -> {});
        TransportKeepAlive keepAlive = new TransportKeepAlive(threadPool, TcpChannel::sendMessage);
        OutboundHandler outboundHandler = new OutboundHandler("node", version, new StatsTracker(), threadPool,
            BigArrays.NON_RECYCLING_INSTANCE);
        requestHandlers = new Transport.RequestHandlers();
        responseHandlers = new Transport.ResponseHandlers();
        handler = new InboundHandler(threadPool, outboundHandler, namedWriteableRegistry, handshaker, keepAlive, requestHandlers,
            responseHandlers);
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testPing() throws Exception {
        AtomicReference<TransportChannel> channelCaptor = new AtomicReference<>();
        RequestHandlerRegistry<TestRequest> registry = new RequestHandlerRegistry<>("test-request", TestRequest::new, taskManager,
            (request, channel, task) -> channelCaptor.set(channel), ThreadPool.Names.SAME, false, true);
        requestHandlers.registerHandler(registry);

        handler.inboundMessage(channel, new InboundMessage(null, true));
        if (channel.isServerChannel()) {
            BytesReference ping = channel.getMessageCaptor().get();
            assertEquals('E', ping.get(0));
            assertEquals(6, ping.length());
        }
    }

    public void testRequestAndResponse() throws Exception {
        String action = "test-request";
        int headerSize = TcpHeader.headerSize(version);
        boolean isError = randomBoolean();
        AtomicReference<TestRequest> requestCaptor = new AtomicReference<>();
        AtomicReference<TestResponse> responseCaptor = new AtomicReference<>();
        AtomicReference<Exception> exceptionCaptor = new AtomicReference<>();
        AtomicReference<TransportChannel> channelCaptor = new AtomicReference<>();

        long requestId = responseHandlers.add(new Transport.ResponseContext<>(new TransportResponseHandler<TestResponse>() {
            @Override
            public void handleResponse(TestResponse response) {
                responseCaptor.set(response);
            }

            @Override
            public void handleException(TransportException exp) {
                exceptionCaptor.set(exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public TestResponse read(StreamInput in) throws IOException {
                return new TestResponse(in);
            }
        }, null, action));
        RequestHandlerRegistry<TestRequest> registry = new RequestHandlerRegistry<>(action, TestRequest::new, taskManager,
            (request, channel, task) -> {
                channelCaptor.set(channel);
                requestCaptor.set(request);
            }, ThreadPool.Names.SAME, false, true);
        requestHandlers.registerHandler(registry);
        String requestValue = randomAlphaOfLength(10);
        OutboundMessage.Request request = new OutboundMessage.Request(threadPool.getThreadContext(),
            new TestRequest(requestValue), version, action, requestId, false, false);

        BytesReference fullRequestBytes = request.serialize(new BytesStreamOutput());
        BytesReference requestContent = fullRequestBytes.slice(headerSize, fullRequestBytes.length() - headerSize);
        Header requestHeader = new Header(fullRequestBytes.length() - 6, requestId, TransportStatus.setRequest((byte) 0), version);
        InboundMessage requestMessage = new InboundMessage(requestHeader, ReleasableBytesReference.wrap(requestContent), () -> {});
        requestHeader.finishParsingHeader(requestMessage.openOrGetStreamInput());
        handler.inboundMessage(channel, requestMessage);

        TransportChannel transportChannel = channelCaptor.get();
        assertEquals(Version.CURRENT, transportChannel.getVersion());
        assertEquals("transport", transportChannel.getChannelType());
        assertEquals(requestValue, requestCaptor.get().value);

        String responseValue = randomAlphaOfLength(10);
        byte responseStatus = TransportStatus.setResponse((byte) 0);
        if (isError) {
            responseStatus = TransportStatus.setError(responseStatus);
            transportChannel.sendResponse(new ElasticsearchException("boom"));
        } else {
            transportChannel.sendResponse(new TestResponse(responseValue));
        }

        BytesReference fullResponseBytes = channel.getMessageCaptor().get();
        BytesReference responseContent = fullResponseBytes.slice(headerSize, fullResponseBytes.length() - headerSize);
        Header responseHeader = new Header(fullRequestBytes.length() - 6, requestId, responseStatus, version);
        InboundMessage responseMessage = new InboundMessage(responseHeader, ReleasableBytesReference.wrap(responseContent), () -> {});
        responseHeader.finishParsingHeader(responseMessage.openOrGetStreamInput());
        handler.inboundMessage(channel, responseMessage);

        if (isError) {
            assertThat(exceptionCaptor.get(), instanceOf(RemoteTransportException.class));
            assertThat(exceptionCaptor.get().getCause(), instanceOf(ElasticsearchException.class));
            assertEquals("boom", exceptionCaptor.get().getCause().getMessage());
        } else {
            assertEquals(responseValue, responseCaptor.get().value);
        }
    }
}
