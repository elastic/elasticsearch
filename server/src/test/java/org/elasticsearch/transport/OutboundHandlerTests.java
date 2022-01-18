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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.HandlingTimeTracker;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.core.internal.io.Streams;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class OutboundHandlerTests extends ESTestCase {

    private final TestThreadPool threadPool = new TestThreadPool(getClass().getName());
    private final TransportRequestOptions options = TransportRequestOptions.EMPTY;
    private final AtomicReference<Tuple<Header, BytesReference>> message = new AtomicReference<>();
    private final BytesRefRecycler recycler = new BytesRefRecycler(PageCacheRecycler.NON_RECYCLING_INSTANCE);
    private InboundPipeline pipeline;
    private OutboundHandler handler;
    private FakeTcpChannel channel;
    private DiscoveryNode node;
    private Compression.Scheme compressionScheme;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        channel = new FakeTcpChannel(randomBoolean(), buildNewFakeTransportAddress().address(), buildNewFakeTransportAddress().address());
        TransportAddress transportAddress = buildNewFakeTransportAddress();
        node = new DiscoveryNode("", transportAddress, Version.CURRENT);
        StatsTracker statsTracker = new StatsTracker();
        compressionScheme = randomFrom(Compression.Scheme.DEFLATE, Compression.Scheme.LZ4);
        handler = new OutboundHandler("node", Version.CURRENT, statsTracker, threadPool, recycler, new HandlingTimeTracker(), false);

        final LongSupplier millisSupplier = () -> TimeValue.nsecToMSec(System.nanoTime());
        final InboundDecoder decoder = new InboundDecoder(Version.CURRENT, this.recycler);
        final Supplier<CircuitBreaker> breaker = () -> new NoopCircuitBreaker("test");
        final InboundAggregator aggregator = new InboundAggregator(breaker, (Predicate<String>) action -> true);
        pipeline = new InboundPipeline(statsTracker, millisSupplier, decoder, aggregator, (c, m) -> {
            try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
                Streams.copy(m.openOrGetStreamInput(), streamOutput);
                message.set(new Tuple<>(m.getHeader(), streamOutput.bytes()));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
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
        ActionListener<Void> sendListener = channel.getListenerCaptor().get();
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
        boolean compressUnsupportedDueToVersion = compressionScheme == Compression.Scheme.LZ4
            && version.before(Compression.Scheme.LZ4_VERSION);
        String value = "message";
        threadContext.putHeader("header", "header_value");
        TestRequest request = new TestRequest(value);

        AtomicReference<DiscoveryNode> nodeRef = new AtomicReference<>();
        AtomicLong requestIdRef = new AtomicLong();
        AtomicReference<String> actionRef = new AtomicReference<>();
        AtomicReference<TransportRequest> requestRef = new AtomicReference<>();
        handler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onRequestSent(
                DiscoveryNode node,
                long requestId,
                String action,
                TransportRequest request,
                TransportRequestOptions options
            ) {
                nodeRef.set(node);
                requestIdRef.set(requestId);
                actionRef.set(action);
                requestRef.set(request);
            }
        });
        if (compress) {
            handler.sendRequest(node, channel, requestId, action, request, options, version, compressionScheme, isHandshake);
        } else {
            handler.sendRequest(node, channel, requestId, action, request, options, version, null, isHandshake);
        }

        BytesReference reference = channel.getMessageCaptor().get();
        ActionListener<Void> sendListener = channel.getListenerCaptor().get();
        if (randomBoolean()) {
            sendListener.onResponse(null);
        } else {
            sendListener.onFailure(new IOException("failed"));
        }
        assertEquals(node, nodeRef.get());
        assertEquals(requestId, requestIdRef.get());
        assertEquals(action, actionRef.get());
        assertEquals(request, requestRef.get());

        pipeline.handleBytes(channel, new ReleasableBytesReference(reference, () -> {}));
        final Tuple<Header, BytesReference> tuple = message.get();
        final Header header = tuple.v1();
        final TestRequest message = new TestRequest(tuple.v2().streamInput());
        assertEquals(version, header.getVersion());
        assertEquals(requestId, header.getRequestId());
        assertTrue(header.isRequest());
        assertFalse(header.isResponse());
        if (isHandshake) {
            assertTrue(header.isHandshake());
        } else {
            assertFalse(header.isHandshake());
        }
        if (compress && compressUnsupportedDueToVersion == false) {
            assertTrue(header.isCompressed());
        } else {
            assertFalse(header.isCompressed());
        }

        assertEquals(value, message.value);
        assertEquals("header_value", header.getHeaders().v1().get("header"));
    }

    public void testSendResponse() throws IOException {
        ThreadContext threadContext = threadPool.getThreadContext();
        Version version = randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion());
        String action = "handshake";
        long requestId = randomLongBetween(0, 300);
        boolean isHandshake = randomBoolean();
        boolean compress = randomBoolean();
        boolean compressUnsupportedDueToVersion = compressionScheme == Compression.Scheme.LZ4
            && version.before(Compression.Scheme.LZ4_VERSION);

        String value = "message";
        threadContext.putHeader("header", "header_value");
        TestResponse response = new TestResponse(value);

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
        if (compress) {
            handler.sendResponse(version, channel, requestId, action, response, compressionScheme, isHandshake);
        } else {
            handler.sendResponse(version, channel, requestId, action, response, null, isHandshake);
        }

        BytesReference reference = channel.getMessageCaptor().get();
        ActionListener<Void> sendListener = channel.getListenerCaptor().get();
        if (randomBoolean()) {
            sendListener.onResponse(null);
        } else {
            sendListener.onFailure(new IOException("failed"));
        }
        assertEquals(requestId, requestIdRef.get());
        assertEquals(action, actionRef.get());
        assertEquals(response, responseRef.get());

        pipeline.handleBytes(channel, new ReleasableBytesReference(reference, () -> {}));
        final Tuple<Header, BytesReference> tuple = message.get();
        final Header header = tuple.v1();
        final TestResponse message = new TestResponse(tuple.v2().streamInput());
        assertEquals(version, header.getVersion());
        assertEquals(requestId, header.getRequestId());
        assertFalse(header.isRequest());
        assertTrue(header.isResponse());
        if (isHandshake) {
            assertTrue(header.isHandshake());
        } else {
            assertFalse(header.isHandshake());
        }
        if (compress && compressUnsupportedDueToVersion == false) {
            assertTrue(header.isCompressed());
        } else {
            assertFalse(header.isCompressed());
        }

        assertFalse(header.isError());

        assertEquals(value, message.value);
        assertEquals("header_value", header.getHeaders().v1().get("header"));
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
        ActionListener<Void> sendListener = channel.getListenerCaptor().get();
        if (randomBoolean()) {
            sendListener.onResponse(null);
        } else {
            sendListener.onFailure(new IOException("failed"));
        }
        assertEquals(requestId, requestIdRef.get());
        assertEquals(action, actionRef.get());
        assertEquals(error, responseRef.get());

        pipeline.handleBytes(channel, new ReleasableBytesReference(reference, () -> {}));
        final Tuple<Header, BytesReference> tuple = message.get();
        final Header header = tuple.v1();
        assertEquals(version, header.getVersion());
        assertEquals(requestId, header.getRequestId());
        assertFalse(header.isRequest());
        assertTrue(header.isResponse());
        assertFalse(header.isCompressed());
        assertFalse(header.isHandshake());
        assertTrue(header.isError());

        RemoteTransportException remoteException = tuple.v2().streamInput().readException();
        assertThat(remoteException.getCause(), instanceOf(ElasticsearchException.class));
        assertEquals(remoteException.getCause().getMessage(), "boom");
        assertThat(
            remoteException.getMessage(),
            allOf(containsString('[' + NetworkAddress.format(channel.getLocalAddress()) + ']'), containsString('[' + action + ']'))
        );

        assertEquals("header_value", header.getHeaders().v1().get("header"));
    }

    public void testSlowLogOutboundMessage() throws Exception {
        final MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "expected message",
                OutboundHandler.class.getCanonicalName(),
                Level.WARN,
                "sending transport message "
            )
        );
        final Logger outboundHandlerLogger = LogManager.getLogger(OutboundHandler.class);
        Loggers.addAppender(outboundHandlerLogger, mockAppender);
        handler.setSlowLogThreshold(TimeValue.timeValueMillis(5L));

        try {
            final int length = randomIntBetween(1, 100);
            final PlainActionFuture<Void> f = PlainActionFuture.newFuture();
            handler.sendBytes(new FakeTcpChannel() {
                @Override
                public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
                    try {
                        TimeUnit.SECONDS.sleep(1L);
                        listener.onResponse(null);
                    } catch (InterruptedException e) {
                        listener.onFailure(e);
                    }
                }
            }, new BytesArray(randomByteArrayOfLength(length)), f);
            f.get();
            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(outboundHandlerLogger, mockAppender);
            mockAppender.stop();
        }
    }
}
