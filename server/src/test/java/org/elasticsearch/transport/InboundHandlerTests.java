/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Map;
import org.elasticsearch.core.Set;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
        OutboundHandler outboundHandler = new OutboundHandler("node", version, new String[0], new StatsTracker(), threadPool,
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
        OutboundMessage.Request request = new OutboundMessage.Request(threadPool.getThreadContext(), new String[0],
            new TestRequest(requestValue), version, action, requestId, false, null);

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

    public void testSendsErrorResponseToHandshakeFromCompatibleVersion() throws Exception {
        // Nodes use their minimum compatibility version for the TCP handshake, so a node from v(major-1).x will report its version as
        // v(major-2).last in the TCP handshake, with which we are not really compatible. We put extra effort into making sure that if
        // successful we can respond correctly in a format this old, but we do not guarantee that we can respond correctly with an error
        // response. However if the two nodes are from the same major version then we do guarantee compatibility of error responses.

        final Version remoteVersion = VersionUtils.randomCompatibleVersion(random(), version);
        final long requestId = randomNonNegativeLong();
        final Header requestHeader = new Header(between(0, 100), requestId,
            TransportStatus.setRequest(TransportStatus.setHandshake((byte) 0)), remoteVersion);
        final InboundMessage requestMessage = unreadableInboundHandshake(remoteVersion, requestHeader);
        requestHeader.actionName = TransportHandshaker.HANDSHAKE_ACTION_NAME;
        requestHeader.headers = Tuple.tuple(Map.of(), Map.of());
        requestHeader.features = Set.of();
        handler.inboundMessage(channel, requestMessage);

        final BytesReference responseBytesReference = channel.getMessageCaptor().get();
        final Header responseHeader = InboundDecoder.readHeader(remoteVersion, responseBytesReference.length(), responseBytesReference);
        assertTrue(responseHeader.isResponse());
        assertTrue(responseHeader.isError());
    }


    public void testClosesChannelOnErrorInHandshakeWithIncompatibleVersion() throws Exception {
        // Nodes use their minimum compatibility version for the TCP handshake, so a node from v(major-1).x will report its version as
        // v(major-2).last in the TCP handshake, with which we are not really compatible. We put extra effort into making sure that if
        // successful we can respond correctly in a format this old, but we do not guarantee that we can respond correctly with an error
        // response so we must just close the connection on an error. To avoid the failure disappearing into a black hole we at least log
        // it.

        final MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "expected message",
                InboundHandler.class.getCanonicalName(),
                Level.WARN,
                "could not send error response to handshake"));
        final Logger inboundHandlerLogger = LogManager.getLogger(InboundHandler.class);
        Loggers.addAppender(inboundHandlerLogger, mockAppender);

        try {
            final AtomicBoolean isClosed = new AtomicBoolean();
            channel.addCloseListener(ActionListener.wrap(() -> assertTrue(isClosed.compareAndSet(false, true))));

            final Version remoteVersion = Version.fromId(randomIntBetween(0, version.minimumCompatibilityVersion().id - 1));
            final long requestId = randomNonNegativeLong();
            final Header requestHeader = new Header(between(0, 100), requestId,
                TransportStatus.setRequest(TransportStatus.setHandshake((byte) 0)), remoteVersion);
            final InboundMessage requestMessage = unreadableInboundHandshake(remoteVersion, requestHeader);
            requestHeader.actionName = TransportHandshaker.HANDSHAKE_ACTION_NAME;
            requestHeader.headers = Tuple.tuple(Map.of(), Map.of());
            requestHeader.features = Set.of();
            handler.inboundMessage(channel, requestMessage);
            assertTrue(isClosed.get());
            assertNull(channel.getMessageCaptor().get());
            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(inboundHandlerLogger, mockAppender);
            mockAppender.stop();
        }
    }

    public void testLogsSlowInboundProcessing() throws Exception {
        final MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                        "expected message",
                        InboundHandler.class.getCanonicalName(),
                        Level.WARN,
                        "handling inbound transport message "));
        final Logger inboundHandlerLogger = LogManager.getLogger(InboundHandler.class);
        Loggers.addAppender(inboundHandlerLogger, mockAppender);

        handler.setSlowLogThreshold(TimeValue.timeValueMillis(5L));
        try {
            final Version remoteVersion = Version.CURRENT;
            final long requestId = randomNonNegativeLong();
            final Header requestHeader = new Header(between(0, 100), requestId,
                    TransportStatus.setRequest(TransportStatus.setHandshake((byte) 0)), remoteVersion);
            final InboundMessage requestMessage =
                    new InboundMessage(requestHeader, ReleasableBytesReference.wrap(BytesArray.EMPTY), () -> {
                        try {
                            TimeUnit.SECONDS.sleep(1L);
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                    });
            requestHeader.actionName = TransportHandshaker.HANDSHAKE_ACTION_NAME;
            requestHeader.headers = Tuple.tuple(Collections.emptyMap(), Collections.emptyMap());
            requestHeader.features = Set.of();
            handler.inboundMessage(channel, requestMessage);
            assertNotNull(channel.getMessageCaptor().get());
            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(inboundHandlerLogger, mockAppender);
            mockAppender.stop();
        }
    }

    private static InboundMessage unreadableInboundHandshake(Version remoteVersion, Header requestHeader) {
        return new InboundMessage(requestHeader, ReleasableBytesReference.wrap(BytesArray.EMPTY), () -> { }) {
            @Override
            public StreamInput openOrGetStreamInput() {
                final StreamInput streamInput = new InputStreamStreamInput(new InputStream() {
                    @Override
                    public int read() {
                        throw new ElasticsearchException("unreadable handshake");
                    }
                });
                streamInput.setVersion(remoteVersion);
                return streamInput;
            }
        };
    }

}
