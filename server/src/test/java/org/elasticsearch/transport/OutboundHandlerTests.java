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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.HandlingTimeTracker;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Streams;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.TransportVersionUtils;
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

import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

public class OutboundHandlerTests extends ESTestCase {

    private final TestThreadPool threadPool = new TestThreadPool(getClass().getName());
    private final TransportRequestOptions options = TransportRequestOptions.EMPTY;
    private final AtomicReference<Tuple<Header, BytesReference>> message = new AtomicReference<>();
    private final BytesRefRecycler recycler = new BytesRefRecycler(PageCacheRecycler.NON_RECYCLING_INSTANCE);
    private InboundPipeline pipeline;
    private OutboundHandler handler;
    private FakeTcpChannel channel;
    private PlainActionFuture<Void> closeListener;
    private DiscoveryNode node;
    private Compression.Scheme compressionScheme;

    @Before
    public void initHandler() throws Exception {
        channel = new FakeTcpChannel(randomBoolean(), buildNewFakeTransportAddress().address(), buildNewFakeTransportAddress().address());
        closeListener = new PlainActionFuture<>();
        channel.addCloseListener(closeListener);
        TransportAddress transportAddress = buildNewFakeTransportAddress();
        node = DiscoveryNodeUtils.create("", transportAddress);
        StatsTracker statsTracker = new StatsTracker();
        compressionScheme = randomFrom(Compression.Scheme.DEFLATE, Compression.Scheme.LZ4);
        handler = new OutboundHandler(
            "node",
            TransportVersion.current(),
            statsTracker,
            threadPool,
            recycler,
            new HandlingTimeTracker(),
            false
        );

        final LongSupplier millisSupplier = () -> TimeValue.nsecToMSec(System.nanoTime());
        final InboundDecoder decoder = new InboundDecoder(this.recycler);
        final Supplier<CircuitBreaker> breaker = () -> NoopCircuitBreaker.INSTANCE;
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
    public void terminateThreadPool() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
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

        assertThat(reference, equalBytes(bytesArray));
    }

    public void testSendRequest() throws IOException {
        ThreadContext threadContext = threadPool.getThreadContext();
        String action = "handshake";
        long requestId = randomLongBetween(0, 300);
        boolean isHandshake = randomBoolean();
        TransportVersion version = isHandshake
            ? randomFrom(TransportHandshaker.ALLOWED_HANDSHAKE_VERSIONS)
            : TransportVersionUtils.randomCompatibleVersion();
        boolean compress = randomBoolean();
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
                TransportRequestOptions options,
                int networkMessageSize
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
        if (compress) {
            assertTrue(header.isCompressed());
        } else {
            assertFalse(header.isCompressed());
        }

        assertEquals(value, message.value);
        assertEquals("header_value", header.getHeaders().v1().get("header"));
    }

    public void testSendResponse() throws IOException {
        ThreadContext threadContext = threadPool.getThreadContext();
        String action = "handshake";
        long requestId = randomLongBetween(0, 300);
        boolean isHandshake = randomBoolean();
        TransportVersion version = isHandshake
            ? randomFrom(TransportHandshaker.ALLOWED_HANDSHAKE_VERSIONS)
            : TransportVersionUtils.randomCompatibleVersion();
        boolean compress = randomBoolean();

        String value = "message";
        threadContext.putHeader("header", "header_value");
        TestResponse response = new TestResponse(value);

        AtomicLong requestIdRef = new AtomicLong();
        AtomicReference<String> actionRef = new AtomicReference<>();
        handler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onResponseSent(long requestId, String action, int networkMessageSize) {
                requestIdRef.set(requestId);
                actionRef.set(action);
            }
        });
        if (compress) {
            handler.sendResponse(
                version,
                channel,
                requestId,
                action,
                response,
                compressionScheme,
                isHandshake,
                ResponseStatsConsumer.NONE,
                null
            );
        } else {
            handler.sendResponse(version, channel, requestId, action, response, null, isHandshake, ResponseStatsConsumer.NONE, null);
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
        if (compress) {
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
        TransportVersion version = TransportVersionUtils.randomCompatibleVersion();
        String action = "not-a-handshake";
        long requestId = randomLongBetween(0, 300);
        threadContext.putHeader("header", "header_value");
        ElasticsearchException error = new ElasticsearchException("boom");

        AtomicLong requestIdRef = new AtomicLong();
        AtomicReference<String> actionRef = new AtomicReference<>();
        AtomicReference<Exception> responseRef = new AtomicReference<>();
        handler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onResponseSent(long requestId, String action, Exception error, int networkMessageSize) {
                requestIdRef.set(requestId);
                actionRef.set(action);
                responseRef.set(error);
            }
        });
        handler.sendErrorResponse(version, channel, requestId, action, ResponseStatsConsumer.NONE, error);

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

    public void testSendErrorAfterFailToSendResponse() throws Exception {
        TransportVersion version = TransportVersionUtils.randomCompatibleVersion();
        String action = randomAlphaOfLength(10);
        long requestId = randomLongBetween(0, 300);
        var response = new ReleasbleTestResponse(randomAlphaOfLength(10)) {
            @Override
            public void writeTo(StreamOutput out) {
                throw new CircuitBreakingException("simulated cbe", CircuitBreaker.Durability.TRANSIENT);
            }
        };

        AtomicLong requestIdRef = new AtomicLong();
        AtomicReference<String> actionRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        handler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onResponseSent(long requestId, String action, int networkMessageSize) {
                assertNull(channel.getMessageCaptor().get());
                assertThat(requestIdRef.get(), equalTo(0L));
                requestIdRef.set(requestId);
                assertNull(actionRef.get());
                actionRef.set(action);
            }

            @Override
            public void onResponseSent(long requestId, String action, Exception error, int networkMessageSize) {
                assertNotNull(channel.getMessageCaptor().get());
                assertThat(requestIdRef.get(), equalTo(requestId));
                assertThat(actionRef.get(), equalTo(action));
                exceptionRef.set(error);
            }
        });
        Compression.Scheme compress = randomFrom(compressionScheme, null);
        try {
            handler.sendResponse(version, channel, requestId, action, response, compress, false, ResponseStatsConsumer.NONE, null);
        } finally {
            response.decRef();
        }
        ActionListener<Void> sendListener = channel.getListenerCaptor().get();
        if (randomBoolean()) {
            sendListener.onResponse(null);
        } else {
            sendListener.onFailure(new IOException("failed"));
        }
        assertEquals(requestId, requestIdRef.get());
        assertEquals(action, actionRef.get());
        assertThat(exceptionRef.get().getMessage(), equalTo("simulated cbe"));
        assertTrue(response.released.get());
        BytesReference reference = channel.getMessageCaptor().get();
        pipeline.handleBytes(channel, new ReleasableBytesReference(reference, () -> {}));
        try (StreamInput input = message.get().v2().streamInput()) {
            RemoteTransportException rme = input.readException();
            assertThat(rme.getCause(), instanceOf(CircuitBreakingException.class));
            assertThat(rme.getCause().getMessage(), equalTo("simulated cbe"));
        }
        assertTrue(channel.isOpen());
        assertFalse(closeListener.isDone());
    }

    public void testFailToSendResponseThenFailToSendError() {
        channel.close();
        channel = new FakeTcpChannel(randomBoolean(), buildNewFakeTransportAddress().address(), buildNewFakeTransportAddress().address()) {
            @Override
            public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
                throw new IllegalStateException("pipe broken");
            }
        };
        closeListener = new PlainActionFuture<>();
        channel.addCloseListener(closeListener);
        TransportVersion version = TransportVersionUtils.randomVersion();
        String action = randomAlphaOfLength(10);
        long requestId = randomLongBetween(0, 300);
        AtomicLong requestIdRef = new AtomicLong();
        AtomicReference<String> actionRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        handler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onResponseSent(long requestId, String action, int networkMessageSize) {
                assertNull(channel.getMessageCaptor().get());
                assertThat(requestIdRef.get(), equalTo(0L));
                requestIdRef.set(requestId);
                assertNull(actionRef.get());
                actionRef.set(action);
            }

            @Override
            public void onResponseSent(long requestId, String action, Exception error, int networkMessageSize) {
                assertNull(channel.getMessageCaptor().get());
                assertThat(requestIdRef.get(), equalTo(requestId));
                assertThat(actionRef.get(), equalTo(action));
                exceptionRef.set(error);
            }
        });
        Compression.Scheme compress = randomFrom(compressionScheme, null);
        var response = new ReleasbleTestResponse(randomAlphaOfLength(10)) {
            @Override
            public void writeTo(StreamOutput out) {
                throw new CircuitBreakingException("simulated cbe", CircuitBreaker.Durability.TRANSIENT);
            }
        };
        try {
            handler.sendResponse(version, channel, requestId, action, response, compress, false, ResponseStatsConsumer.NONE, null);
        } finally {
            response.decRef();
        }
        assertEquals(requestId, requestIdRef.get());
        assertEquals(action, actionRef.get());
        assertThat(exceptionRef.get().getMessage(), equalTo("simulated cbe"));
        assertTrue(response.released.get());
        assertNull(channel.getMessageCaptor().get());
        assertNull(channel.getListenerCaptor().get());
        assertFalse(channel.isOpen());
        assertTrue(closeListener.isDone());
        expectThrows(Exception.class, () -> closeListener.get());
    }

    public void testFailToSendHandshakeResponse() {
        TransportVersion version = TransportVersionUtils.randomVersion();
        String action = randomAlphaOfLength(10);
        long requestId = randomLongBetween(0, 300);
        var response = new ReleasbleTestResponse(randomAlphaOfLength(10)) {
            @Override
            public void writeTo(StreamOutput out) {
                throw new CircuitBreakingException("simulated", CircuitBreaker.Durability.TRANSIENT);
            }
        };

        AtomicLong requestIdRef = new AtomicLong();
        AtomicReference<String> actionRef = new AtomicReference<>();
        handler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onResponseSent(long requestId, String action, int networkMessageSize) {
                assertNull(channel.getMessageCaptor().get());
                assertThat(requestIdRef.get(), equalTo(0L));
                requestIdRef.set(requestId);
                assertNull(actionRef.get());
                actionRef.set(action);
            }

            @Override
            public void onResponseSent(long requestId, String action, Exception error, int networkMessageSize) {
                throw new AssertionError("failing to send a handshake response should not send failure");
            }
        });
        Compression.Scheme compress = randomFrom(compressionScheme, null);
        try {
            handler.sendResponse(version, channel, requestId, action, response, compress, true, ResponseStatsConsumer.NONE, null);
        } finally {
            response.decRef();
        }
        assertNull(channel.getMessageCaptor().get());
        assertNull(channel.getListenerCaptor().get());
        assertEquals(requestId, requestIdRef.get());
        assertEquals(action, actionRef.get());
        assertTrue(response.released.get());
        assertFalse(channel.isOpen());
        assertTrue(closeListener.isDone());
        expectThrows(Exception.class, () -> closeListener.get());
    }

    public void testFailToSendErrorResponse() {
        channel.close();
        channel = new FakeTcpChannel(randomBoolean(), buildNewFakeTransportAddress().address(), buildNewFakeTransportAddress().address()) {
            @Override
            public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
                throw new IllegalStateException("pipe broken");
            }
        };
        closeListener = new PlainActionFuture<>();
        channel.addCloseListener(closeListener);
        TransportVersion version = TransportVersionUtils.randomVersion();
        String action = randomAlphaOfLength(10);
        long requestId = randomLongBetween(0, 300);

        AtomicLong requestIdRef = new AtomicLong();
        AtomicReference<String> actionRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        handler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onResponseSent(long requestId, String action, int networkMessageSize) {
                throw new AssertionError("must not be called");
            }

            @Override
            public void onResponseSent(long requestId, String action, Exception error, int networkMessageSize) {
                assertNull(channel.getMessageCaptor().get());
                assertThat(requestIdRef.get(), equalTo(0L));
                requestIdRef.set(requestId);
                assertNull(actionRef.get());
                actionRef.set(action);
                assertNull(exceptionRef.get());
                exceptionRef.set(error);
            }
        });
        IOException exception = new IOException("file doesn't exist");
        handler.sendErrorResponse(version, channel, requestId, action, ResponseStatsConsumer.NONE, exception);
        assertEquals(requestId, requestIdRef.get());
        assertEquals(action, actionRef.get());
        assertEquals(exception, exceptionRef.get());
        assertFalse(channel.isOpen());
        assertNull(channel.getMessageCaptor().get());
        assertNull(channel.getListenerCaptor().get());
        assertTrue(closeListener.isDone());
        expectThrows(Exception.class, () -> closeListener.get());
    }

    /**
     * This logger is mentioned in the docs by name, so we cannot rename it without adjusting the docs. Thus we fix the expected logger
     * name in this string constant rather than using {@code OutboundHandler.class.getCanonicalName()}.
     */
    private static final String EXPECTED_LOGGER_NAME = "org.elasticsearch.transport.OutboundHandler";

    public void testSlowLogOutboundMessage() throws Exception {
        handler.setSlowLogThreshold(TimeValue.timeValueMillis(5L));

        try (var mockLog = MockLog.capture(OutboundHandler.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation("expected message", EXPECTED_LOGGER_NAME, Level.WARN, "sending transport message ")
            );

            final int length = randomIntBetween(1, 100);
            final PlainActionFuture<Void> f = new PlainActionFuture<>();
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
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testSendRequestReportsNetworkMessageSize() throws IOException {
        AtomicReference<Integer> capturedNetworkMessageSize = new AtomicReference<>();
        handler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onRequestSent(
                DiscoveryNode node,
                long requestId,
                String action,
                TransportRequest request,
                TransportRequestOptions options,
                int networkMessageSize
            ) {
                assertTrue("request reported more than once", capturedNetworkMessageSize.compareAndSet(null, networkMessageSize));
            }
        });
        Compression.Scheme compress = randomFrom(compressionScheme, null);
        handler.sendRequest(
            node,
            channel,
            randomLongBetween(0, 300),
            randomAlphaOfLength(10),
            new TestRequest(randomAlphaOfLength(randomIntBetween(1, 1000))),
            options,
            TransportVersionUtils.randomCompatibleVersion(),
            compress,
            false
        );
        BytesReference reference = channel.getMessageCaptor().get();
        ActionListener<Void> sendListener = channel.getListenerCaptor().get();
        if (randomBoolean()) {
            sendListener.onResponse(null);
        } else {
            sendListener.onFailure(new IOException("failed"));
        }

        assertEquals(reference.length(), capturedNetworkMessageSize.get().intValue());
    }

    public void testSendResponseReportsNetworkMessageSize() {
        AtomicReference<Integer> capturedNetworkMessageSize = new AtomicReference<>();
        handler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onResponseSent(long requestId, String action, int networkMessageSize) {
                assertTrue("response reported more than once", capturedNetworkMessageSize.compareAndSet(null, networkMessageSize));
            }

            @Override
            public void onResponseSent(long requestId, String action, Exception error, int networkMessageSize) {
                throw new AssertionError("must not be called");
            }
        });
        Compression.Scheme compress = randomFrom(compressionScheme, null);
        handler.sendResponse(
            TransportVersionUtils.randomCompatibleVersion(),
            channel,
            randomLongBetween(0, 300),
            randomAlphaOfLength(10),
            new TestResponse(randomAlphaOfLength(randomIntBetween(1, 1000))),
            compress,
            false,
            ResponseStatsConsumer.NONE,
            null
        );
        BytesReference reference = channel.getMessageCaptor().get();
        ActionListener<Void> sendListener = channel.getListenerCaptor().get();
        if (randomBoolean()) {
            sendListener.onResponse(null);
        } else {
            sendListener.onFailure(new IOException("failed"));
        }

        assertEquals(reference.length(), capturedNetworkMessageSize.get().intValue());
    }

    public void testSendErrorResponseReportsNetworkMessageSize() {
        AtomicReference<Integer> capturedNetworkMessageSize = new AtomicReference<>();
        handler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onResponseSent(long requestId, String action, int networkMessageSize) {
                throw new AssertionError("must not be called");
            }

            @Override
            public void onResponseSent(long requestId, String action, Exception error, int networkMessageSize) {
                assertTrue("response reported more than once", capturedNetworkMessageSize.compareAndSet(null, networkMessageSize));
            }
        });
        handler.sendErrorResponse(
            TransportVersionUtils.randomCompatibleVersion(),
            channel,
            randomLongBetween(0, 300),
            randomAlphaOfLength(10),
            ResponseStatsConsumer.NONE,
            new ElasticsearchException(randomAlphaOfLength(randomIntBetween(1, 1000)))
        );
        BytesReference reference = channel.getMessageCaptor().get();
        ActionListener<Void> sendListener = channel.getListenerCaptor().get();
        if (randomBoolean()) {
            sendListener.onResponse(null);
        } else {
            sendListener.onFailure(new IOException("failed"));
        }

        assertEquals(reference.length(), capturedNetworkMessageSize.get().intValue());
    }

    public void testSendResponseChargesBreakerUntilWritten() throws IOException {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(10));
        String value = randomAlphaOfLength(randomIntBetween(1, 100_000));
        long requestId = randomLongBetween(0, 300);
        Compression.Scheme compress = randomFrom(compressionScheme, null);
        sendResponseWithBreaker(requestId, new TestResponse(value), compress, breaker);

        BytesReference reference = channel.getMessageCaptor().get();
        // The pages stay charged while the message waits to be written, each one exactly once.
        final long pages = (reference.length() + recycler.pageSize() - 1) / recycler.pageSize();
        assertThat(breaker.getUsed(), equalTo(pages * recycler.pageSize()));
        ActionListener<Void> sendListener = channel.getListenerCaptor().get();
        if (randomBoolean()) {
            sendListener.onResponse(null);
        } else {
            sendListener.onFailure(new IOException("failed"));
        }
        assertThat(breaker.getUsed(), equalTo(0L));

        pipeline.handleBytes(channel, new ReleasableBytesReference(reference, () -> {}));
        Header header = message.get().v1();
        assertEquals(requestId, header.getRequestId());
        assertFalse(header.isError());
        assertEquals(compress != null, header.isCompressed());
        assertEquals(value, new TestResponse(message.get().v2().streamInput()).value);
    }

    public void testSendResponseSendsErrorOnFirstPageTrip() throws IOException {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ZERO);
        assertTripSentAsError(breaker, randomFrom(compressionScheme, null));
        assertThat(breaker.getTrippedCount(), equalTo(1L));
    }

    public void testSendResponseSendsErrorOnTripDuringSerialization() throws IOException {
        // The first page fits, the rest of the response does not, so the trip happens inside the compressing stream.
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(recycler.pageSize()));
        assertTripSentAsError(breaker, compressionScheme);
        // Closing a compressed stream after the trip writes its trailer and can trip again.
        assertThat(breaker.getTrippedCount(), greaterThanOrEqualTo(1L));

        // The trip released the thread-local compression resources, so the next compressed response on this thread succeeds.
        String value = randomAlphaOfLength(randomIntBetween(1, 100_000));
        long requestId = randomLongBetween(0, 300);
        sendResponseWithBreaker(requestId, new TestResponse(value), compressionScheme, null);
        BytesReference reference = channel.getMessageCaptor().get();
        channel.getListenerCaptor().get().onResponse(null);
        pipeline.handleBytes(channel, new ReleasableBytesReference(reference, () -> {}));
        Header header = message.get().v1();
        assertEquals(requestId, header.getRequestId());
        assertFalse(header.isError());
        assertEquals(value, new TestResponse(message.get().v2().streamInput()).value);
    }

    public void testSendResponseReleasesLz4BufferOnHeaderTrip() throws Exception {
        // Pages from a MockPageCacheRecycler are checked for leaks on teardown.
        final var trackingRecycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
        final var trackingHandler = new OutboundHandler(
            "node",
            TransportVersion.current(),
            new StatsTracker(),
            threadPool,
            trackingRecycler,
            new HandlingTimeTracker(),
            false
        );
        // Size the thread context so the LZ4 header is the first write that needs a second page.
        final int threadContextSize = trackingRecycler.pageSize() - TcpHeader.HEADER_SIZE - between(1, 3);
        final String headerValue = headerValueForThreadContextSize(threadContextSize);
        final CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(trackingRecycler.pageSize()));
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("header", headerValue);
            assertEquals(threadContextSize, serializedSize(threadContext));
            trackingHandler.sendResponse(
                TransportVersion.current(),
                channel,
                randomLongBetween(0, 300),
                randomAlphaOfLength(10),
                new TestResponse(randomAlphaOfLength(10)),
                Compression.Scheme.LZ4,
                false,
                ResponseStatsConsumer.NONE,
                breaker
            );
        }
        assertThat(breaker.getTrippedCount(), equalTo(1L));
        assertThat(breaker.getUsed(), equalTo(0L));
        channel.getListenerCaptor().get().onResponse(null);
        MockPageCacheRecycler.ensureAllPagesAreReleased();
    }

    private static String headerValueForThreadContextSize(int size) throws IOException {
        for (int length = size; length > 0; length--) {
            final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
            final String value = randomAlphaOfLength(length);
            threadContext.putHeader("header", value);
            if (serializedSize(threadContext) == size) {
                return value;
            }
        }
        throw new AssertionError("no header value gives a thread context of [" + size + "] bytes");
    }

    private static int serializedSize(ThreadContext threadContext) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            threadContext.writeTo(out);
            return Math.toIntExact(out.position());
        }
    }

    public void testSerializationFailureLogsWarningUnlessBreakerTrips() {
        final String expectedMessage = "failed to serialize outbound message*";
        MockLog.assertThatLogger(
            () -> sendFailingResponse(new CircuitBreakingException("simulated", CircuitBreaker.Durability.TRANSIENT)),
            OutboundHandler.class,
            new MockLog.UnseenEventExpectation("breaker trip", EXPECTED_LOGGER_NAME, Level.WARN, expectedMessage)
        );
        MockLog.assertThatLogger(
            () -> sendFailingResponse(new IllegalStateException("simulated")),
            OutboundHandler.class,
            new MockLog.SeenEventExpectation("other failure", EXPECTED_LOGGER_NAME, Level.WARN, expectedMessage)
        );
    }

    private void sendFailingResponse(RuntimeException failure) {
        final CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        sendResponseWithBreaker(randomLongBetween(0, 300), new TestResponse(randomAlphaOfLength(10)) {
            @Override
            public void writeTo(StreamOutput out) {
                throw failure;
            }
        }, randomFrom(compressionScheme, null), breaker);
        // The failed response's pages are released before the error response is written.
        assertThat(breaker.getUsed(), equalTo(0L));
        channel.getListenerCaptor().get().onResponse(null);
    }

    public void testSendResponseReleasesBreakerWhenSendThrows() {
        channel.close();
        channel = new FakeTcpChannel(randomBoolean(), buildNewFakeTransportAddress().address(), buildNewFakeTransportAddress().address()) {
            @Override
            public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
                throw new IllegalStateException("pipe broken");
            }
        };
        closeListener = new PlainActionFuture<>();
        channel.addCloseListener(closeListener);
        final CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(10));

        sendResponseWithBreaker(
            randomLongBetween(0, 300),
            new TestResponse(randomAlphaOfLength(randomIntBetween(1, 100_000))),
            randomFrom(compressionScheme, null),
            breaker
        );

        assertThat(breaker.getUsed(), equalTo(0L));
        // The error response fails on the same channel, which is then closed.
        assertFalse(channel.isOpen());
        assertTrue(closeListener.isDone());
    }

    private void sendResponseWithBreaker(long requestId, TestResponse response, Compression.Scheme compress, CircuitBreaker breaker) {
        handler.sendResponse(
            TransportVersionUtils.randomCompatibleVersion(),
            channel,
            requestId,
            randomAlphaOfLength(10),
            response,
            compress,
            false,
            ResponseStatsConsumer.NONE,
            breaker
        );
    }

    private void assertTripSentAsError(CircuitBreaker breaker, Compression.Scheme compress) throws IOException {
        long requestId = randomLongBetween(0, 300);
        AtomicReference<Exception> errorRef = new AtomicReference<>();
        handler.setMessageListener(new TransportMessageListener() {
            @Override
            public void onResponseSent(long requestId, String action, Exception error, int networkMessageSize) {
                errorRef.set(error);
            }
        });
        sendResponseWithBreaker(requestId, new TestResponse(randomAlphaOfLength(100_000)), compress, breaker);

        assertThat(breaker.getUsed(), equalTo(0L));
        BytesReference reference = channel.getMessageCaptor().get();
        channel.getListenerCaptor().get().onResponse(null);
        assertThat(errorRef.get(), instanceOf(CircuitBreakingException.class));
        pipeline.handleBytes(channel, new ReleasableBytesReference(reference, () -> {}));
        Header header = message.get().v1();
        assertEquals(requestId, header.getRequestId());
        assertTrue(header.isError());
        try (StreamInput input = message.get().v2().streamInput()) {
            assertThat(input.readException().getCause(), instanceOf(CircuitBreakingException.class));
        }
        assertTrue(channel.isOpen());
    }

    static class ReleasbleTestResponse extends TestResponse {
        final AtomicBoolean released = new AtomicBoolean();
        final AbstractRefCounted refs = AbstractRefCounted.of(() -> assertTrue(released.compareAndSet(false, true)));

        ReleasbleTestResponse(String value) {
            super(value);
        }

        @Override
        public void incRef() {
            refs.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return refs.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return refs.decRef();
        }

        @Override
        public boolean hasReferences() {
            return refs.hasReferences();
        }
    }
}
