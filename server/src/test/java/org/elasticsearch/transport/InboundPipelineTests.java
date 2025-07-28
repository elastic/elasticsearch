/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.breaker.TestCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.instanceOf;

public class InboundPipelineTests extends ESTestCase {

    private static final int BYTE_THRESHOLD = 128 * 1024;
    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private final BytesRefRecycler recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));

    public void testPipelineHandling() throws IOException {
        final List<Tuple<MessageData, Exception>> expected = new ArrayList<>();
        final List<Tuple<MessageData, Exception>> actual = new ArrayList<>();
        final List<ReleasableBytesReference> toRelease = new ArrayList<>();
        final BiConsumer<TcpChannel, InboundMessage> messageHandler = (c, m) -> {
            try (m) {
                final Header header = m.getHeader();
                final MessageData actualData;
                final TransportVersion version = header.getVersion();
                final boolean isRequest = header.isRequest();
                final long requestId = header.getRequestId();
                final Compression.Scheme compressionScheme = header.getCompressionScheme();
                if (header.isCompressed()) {
                    assertNotNull(compressionScheme);
                } else {
                    assertNull(compressionScheme);
                }
                if (m.isShortCircuit()) {
                    actualData = new MessageData(version, requestId, isRequest, compressionScheme, header.getActionName(), null);
                } else if (isRequest) {
                    final TestRequest request = new TestRequest(m.openOrGetStreamInput());
                    actualData = new MessageData(version, requestId, isRequest, compressionScheme, header.getActionName(), request.value);
                } else {
                    final TestResponse response = new TestResponse(m.openOrGetStreamInput());
                    actualData = new MessageData(version, requestId, isRequest, compressionScheme, null, response.value);
                }
                actual.add(new Tuple<>(actualData, m.getException()));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        };

        final StatsTracker statsTracker = new StatsTracker();
        final LongSupplier millisSupplier = () -> TimeValue.nsecToMSec(System.nanoTime());
        final InboundDecoder decoder = new InboundDecoder(recycler);
        final String breakThisAction = "break_this_action";
        final String actionName = "actionName";
        final Predicate<String> canTripBreaker = breakThisAction::equals;
        final TestCircuitBreaker circuitBreaker = new TestCircuitBreaker();
        circuitBreaker.startBreaking();
        final InboundAggregator aggregator = new InboundAggregator(() -> circuitBreaker, canTripBreaker);
        final InboundPipeline pipeline = new InboundPipeline(statsTracker, millisSupplier, decoder, aggregator, messageHandler);
        final FakeTcpChannel channel = new FakeTcpChannel();

        final int iterations = randomIntBetween(100, 500);
        long totalMessages = 0;
        long bytesReceived = 0;

        for (int i = 0; i < iterations; ++i) {
            actual.clear();
            expected.clear();
            toRelease.clear();
            try (RecyclerBytesStreamOutput streamOutput = new RecyclerBytesStreamOutput(recycler)) {
                while (streamOutput.size() < BYTE_THRESHOLD) {
                    final TransportVersion version = randomFrom(TransportVersion.current(), TransportVersions.MINIMUM_COMPATIBLE);
                    final String value = randomRealisticUnicodeOfCodepointLength(randomIntBetween(200, 400));
                    final boolean isRequest = randomBoolean();
                    Compression.Scheme compressionScheme = getCompressionScheme();
                    final long requestId = totalMessages++;

                    final MessageData messageData;
                    Exception expectedExceptionClass = null;

                    BytesReference message;
                    try (RecyclerBytesStreamOutput temporaryOutput = new RecyclerBytesStreamOutput(recycler)) {
                        if (isRequest) {
                            if (rarely()) {
                                messageData = new MessageData(version, requestId, true, compressionScheme, breakThisAction, null);
                                message = OutboundHandler.serialize(
                                    OutboundHandler.MessageDirection.REQUEST,
                                    breakThisAction,
                                    requestId,
                                    false,
                                    version,
                                    compressionScheme,
                                    new TestRequest(value),
                                    threadContext,
                                    temporaryOutput
                                );
                                expectedExceptionClass = new CircuitBreakingException("", CircuitBreaker.Durability.PERMANENT);
                            } else {
                                messageData = new MessageData(version, requestId, true, compressionScheme, actionName, value);
                                message = OutboundHandler.serialize(
                                    OutboundHandler.MessageDirection.REQUEST,
                                    actionName,
                                    requestId,
                                    false,
                                    version,
                                    compressionScheme,
                                    new TestRequest(value),
                                    threadContext,
                                    temporaryOutput
                                );
                            }
                        } else {
                            messageData = new MessageData(version, requestId, false, compressionScheme, null, value);
                            message = OutboundHandler.serialize(
                                OutboundHandler.MessageDirection.RESPONSE,
                                actionName,
                                requestId,
                                false,
                                version,
                                compressionScheme,
                                new TestResponse(value),
                                threadContext,
                                temporaryOutput
                            );
                        }

                        expected.add(new Tuple<>(messageData, expectedExceptionClass));
                        message.writeTo(streamOutput);
                    }
                }

                final BytesReference networkBytes = streamOutput.bytes();
                int currentOffset = 0;
                while (currentOffset != networkBytes.length()) {
                    final int remainingBytes = networkBytes.length() - currentOffset;
                    final int bytesToRead = Math.min(randomIntBetween(1, 32 * 1024), remainingBytes);
                    final BytesReference slice = networkBytes.slice(currentOffset, bytesToRead);
                    ReleasableBytesReference reference = new ReleasableBytesReference(slice, () -> {});
                    toRelease.add(reference);
                    bytesReceived += reference.length();
                    pipeline.handleBytes(channel, reference);
                    currentOffset += bytesToRead;
                }

                final int messages = expected.size();
                for (int j = 0; j < messages; ++j) {
                    final Tuple<MessageData, Exception> expectedTuple = expected.get(j);
                    final Tuple<MessageData, Exception> actualTuple = actual.get(j);
                    final MessageData expectedMessageData = expectedTuple.v1();
                    final MessageData actualMessageData = actualTuple.v1();
                    assertEquals(expectedMessageData.requestId, actualMessageData.requestId);
                    assertEquals(expectedMessageData.isRequest, actualMessageData.isRequest);
                    assertEquals(expectedMessageData.compressionScheme, actualMessageData.compressionScheme);
                    assertEquals(expectedMessageData.actionName, actualMessageData.actionName);
                    assertEquals(expectedMessageData.value, actualMessageData.value);
                    if (expectedTuple.v2() != null) {
                        assertNotNull(actualTuple.v2());
                        assertThat(actualTuple.v2(), instanceOf(expectedTuple.v2().getClass()));
                    }
                }

                for (ReleasableBytesReference released : toRelease) {
                    assertFalse(released.hasReferences());
                }
            }

            assertEquals(bytesReceived, statsTracker.getBytesRead());
            assertEquals(totalMessages, statsTracker.getMessagesReceived());
        }
    }

    private static Compression.Scheme getCompressionScheme() {
        return randomFrom((Compression.Scheme) null, Compression.Scheme.DEFLATE, Compression.Scheme.LZ4);
    }

    public void testDecodeExceptionIsPropagated() throws IOException {
        BiConsumer<TcpChannel, InboundMessage> messageHandler = (c, m) -> m.close();
        final StatsTracker statsTracker = new StatsTracker();
        final LongSupplier millisSupplier = () -> TimeValue.nsecToMSec(System.nanoTime());
        final InboundDecoder decoder = new InboundDecoder(recycler);
        final Supplier<CircuitBreaker> breaker = () -> new NoopCircuitBreaker("test");
        final InboundAggregator aggregator = new InboundAggregator(breaker, (Predicate<String>) action -> true);
        final InboundPipeline pipeline = new InboundPipeline(statsTracker, millisSupplier, decoder, aggregator, messageHandler);

        try (RecyclerBytesStreamOutput streamOutput = new RecyclerBytesStreamOutput(recycler)) {
            String actionName = "actionName";
            final TransportVersion invalidVersion = TransportVersionUtils.getPreviousVersion(TransportVersions.MINIMUM_COMPATIBLE);
            final String value = randomAlphaOfLength(1000);
            final boolean isRequest = randomBoolean();
            final long requestId = randomNonNegativeLong();

            final BytesReference message = OutboundHandler.serialize(
                isRequest ? OutboundHandler.MessageDirection.REQUEST : OutboundHandler.MessageDirection.RESPONSE,
                actionName,
                requestId,
                false,
                invalidVersion,
                null,
                isRequest ? new TestRequest(value) : new TestResponse(value),
                threadContext,
                streamOutput
            );

            try (ReleasableBytesReference releasable = ReleasableBytesReference.wrap(message)) {
                expectThrows(IllegalStateException.class, () -> pipeline.handleBytes(new FakeTcpChannel(), releasable));
            }

            // Pipeline cannot be reused after uncaught exception
            final IllegalStateException ise = expectThrows(
                IllegalStateException.class,
                () -> pipeline.handleBytes(new FakeTcpChannel(), ReleasableBytesReference.wrap(BytesArray.EMPTY))
            );
            assertEquals("Pipeline state corrupted by uncaught exception", ise.getMessage());
        }
    }

    public void testEnsureBodyIsNotPrematurelyReleased() throws IOException {
        BiConsumer<TcpChannel, InboundMessage> messageHandler = (c, m) -> m.close();
        final StatsTracker statsTracker = new StatsTracker();
        final LongSupplier millisSupplier = () -> TimeValue.nsecToMSec(System.nanoTime());
        final InboundDecoder decoder = new InboundDecoder(recycler);
        final Supplier<CircuitBreaker> breaker = () -> new NoopCircuitBreaker("test");
        final InboundAggregator aggregator = new InboundAggregator(breaker, (Predicate<String>) action -> true);
        final InboundPipeline pipeline = new InboundPipeline(statsTracker, millisSupplier, decoder, aggregator, messageHandler);

        try (RecyclerBytesStreamOutput streamOutput = new RecyclerBytesStreamOutput(recycler)) {
            String actionName = "actionName";
            final TransportVersion version = TransportVersion.current();
            final String value = randomAlphaOfLength(1000);
            final boolean isRequest = randomBoolean();
            final long requestId = randomNonNegativeLong();

            final BytesReference reference = OutboundHandler.serialize(
                isRequest ? OutboundHandler.MessageDirection.REQUEST : OutboundHandler.MessageDirection.RESPONSE,
                actionName,
                requestId,
                false,
                version,
                null,
                isRequest ? new TestRequest(value) : new TestResponse(value),
                threadContext,
                streamOutput
            );

            final int variableHeaderSize = reference.getInt(TcpHeader.HEADER_SIZE - 4);
            final int totalHeaderSize = TcpHeader.HEADER_SIZE + variableHeaderSize;
            final AtomicBoolean bodyReleased = new AtomicBoolean(false);
            for (int i = 0; i < totalHeaderSize - 1; ++i) {
                try (ReleasableBytesReference slice = ReleasableBytesReference.wrap(reference.slice(i, 1))) {
                    pipeline.handleBytes(new FakeTcpChannel(), slice);
                }
            }

            final Releasable releasable = () -> bodyReleased.set(true);
            final int from = totalHeaderSize - 1;
            final BytesReference partHeaderPartBody = reference.slice(from, reference.length() - from - 1);
            pipeline.handleBytes(new FakeTcpChannel(), new ReleasableBytesReference(partHeaderPartBody, releasable));
            assertFalse(bodyReleased.get());
            pipeline.handleBytes(
                new FakeTcpChannel(),
                new ReleasableBytesReference(reference.slice(reference.length() - 1, 1), releasable)
            );
            assertTrue(bodyReleased.get());
        }
    }

    private record MessageData(
        TransportVersion version,
        long requestId,
        boolean isRequest,
        Compression.Scheme compressionScheme,
        String actionName,
        String value
    ) {}
}
