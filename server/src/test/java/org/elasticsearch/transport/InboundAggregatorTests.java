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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.breaker.TestCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import static org.elasticsearch.common.bytes.ReleasableBytesReferenceStreamInputTests.wrapAsReleasable;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class InboundAggregatorTests extends ESTestCase {

    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private final String unBreakableAction = "non_breakable_action";
    private final String unknownAction = "unknown_action";
    private InboundAggregator aggregator;
    private TestCircuitBreaker circuitBreaker;

    @Before
    public void initAggregator() throws Exception {
        Predicate<String> requestCanTripBreaker = action -> {
            if (unknownAction.equals(action)) {
                throw new ActionNotFoundTransportException(action);
            } else {
                return unBreakableAction.equals(action) == false;
            }
        };
        circuitBreaker = new TestCircuitBreaker();
        aggregator = new InboundAggregator(() -> circuitBreaker, requestCanTripBreaker);
    }

    public void testInboundAggregation() throws IOException {
        long requestId = randomNonNegativeLong();
        Header header = new Header(randomInt(), requestId, TransportStatus.setRequest((byte) 0), TransportVersion.current());
        header.headers = new Tuple<>(Collections.emptyMap(), Collections.emptyMap());
        header.actionName = "action_name";
        // Initiate Message
        aggregator.headerReceived(header);

        BytesArray bytes = new BytesArray(randomByteArrayOfLength(10));
        ArrayList<ReleasableBytesReference> references = new ArrayList<>();
        if (randomBoolean()) {
            final ReleasableBytesReference content = wrapAsReleasable(bytes);
            references.add(content);
            aggregator.aggregate(content);
            content.close();
        } else {
            final ReleasableBytesReference content1 = wrapAsReleasable(bytes.slice(0, 3));
            references.add(content1);
            aggregator.aggregate(content1);
            content1.close();
            final ReleasableBytesReference content2 = wrapAsReleasable(bytes.slice(3, 3));
            references.add(content2);
            aggregator.aggregate(content2);
            content2.close();
            final ReleasableBytesReference content3 = wrapAsReleasable(bytes.slice(6, 4));
            references.add(content3);
            aggregator.aggregate(content3);
            content3.close();
        }

        // Signal EOS
        InboundMessage aggregated = aggregator.finishAggregation();

        assertThat(aggregated, notNullValue());
        assertFalse(aggregated.isPing());
        assertTrue(aggregated.getHeader().isRequest());
        assertThat(aggregated.getHeader().getRequestId(), equalTo(requestId));
        assertThat(aggregated.getHeader().getVersion(), equalTo(TransportVersion.current()));
        for (ReleasableBytesReference reference : references) {
            assertTrue(reference.hasReferences());
        }
        aggregated.close();
        for (ReleasableBytesReference reference : references) {
            assertFalse(reference.hasReferences());
        }
    }

    public void testInboundUnknownAction() throws IOException {
        long requestId = randomNonNegativeLong();
        Header header = new Header(randomInt(), requestId, TransportStatus.setRequest((byte) 0), TransportVersion.current());
        header.headers = new Tuple<>(Collections.emptyMap(), Collections.emptyMap());
        header.actionName = unknownAction;
        // Initiate Message
        aggregator.headerReceived(header);

        BytesArray bytes = new BytesArray(randomByteArrayOfLength(10));
        final ReleasableBytesReference content = wrapAsReleasable(bytes);
        aggregator.aggregate(content);
        content.close();
        assertFalse(content.hasReferences());

        // Signal EOS
        InboundMessage aggregated = aggregator.finishAggregation();

        assertThat(aggregated, notNullValue());
        assertTrue(aggregated.isShortCircuit());
        assertThat(aggregated.getException(), instanceOf(ActionNotFoundTransportException.class));
        assertNotNull(aggregated.takeBreakerReleaseControl());
    }

    public void testCircuitBreak() throws IOException {
        circuitBreaker.startBreaking();
        // Actions are breakable
        Header breakableHeader = new Header(
            randomInt(),
            randomNonNegativeLong(),
            TransportStatus.setRequest((byte) 0),
            TransportVersion.current()
        );
        breakableHeader.headers = new Tuple<>(Collections.emptyMap(), Collections.emptyMap());
        breakableHeader.actionName = "action_name";
        // Initiate Message
        aggregator.headerReceived(breakableHeader);

        BytesArray bytes = new BytesArray(randomByteArrayOfLength(10));
        final ReleasableBytesReference content1 = wrapAsReleasable(bytes);
        aggregator.aggregate(content1);
        content1.close();

        // Signal EOS
        InboundMessage aggregated1 = aggregator.finishAggregation();

        assertFalse(content1.hasReferences());
        assertThat(aggregated1, notNullValue());
        assertTrue(aggregated1.isShortCircuit());
        assertThat(aggregated1.getException(), instanceOf(CircuitBreakingException.class));

        // Actions marked as unbreakable are not broken
        Header unbreakableHeader = new Header(
            randomInt(),
            randomNonNegativeLong(),
            TransportStatus.setRequest((byte) 0),
            TransportVersion.current()
        );
        unbreakableHeader.headers = new Tuple<>(Collections.emptyMap(), Collections.emptyMap());
        unbreakableHeader.actionName = unBreakableAction;
        // Initiate Message
        aggregator.headerReceived(unbreakableHeader);

        final ReleasableBytesReference content2 = wrapAsReleasable(bytes);
        aggregator.aggregate(content2);
        content2.close();

        // Signal EOS
        InboundMessage aggregated2 = aggregator.finishAggregation();

        assertTrue(content2.hasReferences());
        assertThat(aggregated2, notNullValue());
        assertFalse(aggregated2.isShortCircuit());

        // Handshakes are not broken
        final byte handshakeStatus = TransportStatus.setHandshake(TransportStatus.setRequest((byte) 0));
        Header handshakeHeader = new Header(randomInt(), randomNonNegativeLong(), handshakeStatus, TransportVersion.current());
        handshakeHeader.headers = new Tuple<>(Collections.emptyMap(), Collections.emptyMap());
        handshakeHeader.actionName = "handshake";
        // Initiate Message
        aggregator.headerReceived(handshakeHeader);

        final ReleasableBytesReference content3 = wrapAsReleasable(bytes);
        aggregator.aggregate(content3);
        content3.close();

        // Signal EOS
        InboundMessage aggregated3 = aggregator.finishAggregation();

        assertTrue(content3.hasReferences());
        assertThat(aggregated3, notNullValue());
        assertFalse(aggregated3.isShortCircuit());
    }

    public void testCloseWillCloseContent() {
        long requestId = randomNonNegativeLong();
        Header header = new Header(randomInt(), requestId, TransportStatus.setRequest((byte) 0), TransportVersion.current());
        header.headers = new Tuple<>(Collections.emptyMap(), Collections.emptyMap());
        header.actionName = "action_name";
        // Initiate Message
        aggregator.headerReceived(header);

        BytesArray bytes = new BytesArray(randomByteArrayOfLength(10));
        ArrayList<ReleasableBytesReference> references = new ArrayList<>();
        if (randomBoolean()) {
            final ReleasableBytesReference content = wrapAsReleasable(bytes);
            references.add(content);
            aggregator.aggregate(content);
            content.close();
        } else {
            final ReleasableBytesReference content1 = wrapAsReleasable(bytes.slice(0, 5));
            references.add(content1);
            aggregator.aggregate(content1);
            content1.close();
            final ReleasableBytesReference content2 = wrapAsReleasable(bytes.slice(5, 5));
            references.add(content2);
            aggregator.aggregate(content2);
            content2.close();
        }

        aggregator.close();

        for (ReleasableBytesReference reference : references) {
            assertFalse(reference.hasReferences());
        }
    }

    public void testFinishAggregationWillFinishHeader() throws IOException {
        long requestId = randomNonNegativeLong();
        final String actionName;
        final boolean unknownAction = randomBoolean();
        if (unknownAction) {
            actionName = this.unknownAction;
        } else {
            actionName = "action_name";
        }
        Header header = new Header(randomInt(), requestId, TransportStatus.setRequest((byte) 0), TransportVersion.current());
        // Initiate Message
        aggregator.headerReceived(header);

        try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
            threadContext.writeTo(streamOutput);
            streamOutput.writeString(actionName);
            streamOutput.write(randomByteArrayOfLength(10));

            final ReleasableBytesReference content = wrapAsReleasable(streamOutput.bytes());
            aggregator.aggregate(content);
            content.close();

            // Signal EOS
            InboundMessage aggregated = aggregator.finishAggregation();

            assertThat(aggregated, notNullValue());
            assertFalse(header.needsToReadVariableHeader());
            assertEquals(actionName, header.getActionName());
            if (unknownAction) {
                assertFalse(content.hasReferences());
                assertTrue(aggregated.isShortCircuit());
            } else {
                assertTrue(content.hasReferences());
                assertFalse(aggregated.isShortCircuit());
            }
        }
    }

    private static Header requestHeader(int networkMessageSize, boolean compressed) {
        byte status = TransportStatus.setRequest((byte) 0);
        if (compressed) {
            status = TransportStatus.setCompress(status);
        }
        Header header = new Header(networkMessageSize, randomNonNegativeLong(), status, TransportVersion.current());
        header.headers = new Tuple<>(Collections.emptyMap(), Collections.emptyMap());
        header.actionName = "action_name";
        return header;
    }

    private void startAggregating(boolean compressed, int declaredSize) {
        aggregator.headerReceived(requestHeader(declaredSize, compressed));
        if (compressed) {
            aggregator.updateCompressionScheme(randomFrom(Compression.Scheme.values()));
        }
    }

    private static ReleasableBytesReference fragment(int length) {
        return wrapAsReleasable(new BytesArray(randomByteArrayOfLength(length)));
    }

    public void testMessageIsChargedFragmentByFragment() throws IOException {
        final CircuitBreaker limitedBreaker = newLimitedBreaker(ByteSizeValue.ofBytes(100));
        aggregator = new InboundAggregator(() -> limitedBreaker, action -> true);

        // Whether or not the message is compressed, nothing is charged for the declared size, only for what has actually arrived
        startAggregating(randomBoolean(), between(1, 1000));
        assertThat(limitedBreaker.getUsed(), equalTo(0L));

        final ReleasableBytesReference fragment1 = fragment(40);
        aggregator.aggregate(fragment1);
        fragment1.close();
        assertThat(limitedBreaker.getUsed(), equalTo(40L));

        final ReleasableBytesReference fragment2 = fragment(30);
        aggregator.aggregate(fragment2);
        fragment2.close();
        assertThat(limitedBreaker.getUsed(), equalTo(70L));

        final InboundMessage aggregated = aggregator.finishAggregation();
        assertFalse(aggregated.isShortCircuit());
        assertThat(limitedBreaker.getUsed(), equalTo(70L));

        aggregated.close();
        assertThat(limitedBreaker.getUsed(), equalTo(0L));
    }

    public void testBreakerTripsPartWayThroughReadingAndReleasesWhatWasBuffered() throws IOException {
        final CircuitBreaker limitedBreaker = newLimitedBreaker(ByteSizeValue.ofBytes(100));
        aggregator = new InboundAggregator(() -> limitedBreaker, action -> true);

        startAggregating(randomBoolean(), between(1, 1000));

        final ReleasableBytesReference fragment1 = fragment(60);
        aggregator.aggregate(fragment1);
        fragment1.close();
        assertTrue(fragment1.hasReferences());
        assertThat(limitedBreaker.getUsed(), equalTo(60L));

        // The second fragment would exceed the limit: what was buffered is released straight away rather than when the message ends
        final ReleasableBytesReference fragment2 = fragment(60);
        aggregator.aggregate(fragment2);
        fragment2.close();
        assertFalse(fragment1.hasReferences());
        assertFalse(fragment2.hasReferences());
        assertThat(limitedBreaker.getUsed(), equalTo(0L));

        // The rest of the message is discarded
        final ReleasableBytesReference fragment3 = fragment(10);
        aggregator.aggregate(fragment3);
        fragment3.close();
        assertFalse(fragment3.hasReferences());
        assertThat(limitedBreaker.getUsed(), equalTo(0L));

        final InboundMessage aggregated = aggregator.finishAggregation();
        assertTrue(aggregated.isShortCircuit());
        assertThat(aggregated.getException(), instanceOf(CircuitBreakingException.class));
        assertThat(limitedBreaker.getUsed(), equalTo(0L));
    }

    public void testCloseReleasesChargedBytes() {
        final CircuitBreaker limitedBreaker = newLimitedBreaker(ByteSizeValue.ofKb(1));
        aggregator = new InboundAggregator(() -> limitedBreaker, action -> true);

        startAggregating(randomBoolean(), between(1, 1000));
        final ReleasableBytesReference fragment = fragment(between(1, 100));
        aggregator.aggregate(fragment);
        fragment.close();
        assertThat(limitedBreaker.getUsed(), greaterThan(0L));

        aggregator.close();
        assertThat(limitedBreaker.getUsed(), equalTo(0L));
    }

    /**
     * Models several large messages (e.g. fetch chunks from different shards) that each fit in the heap on their own but not together,
     * and whose headers all arrive before any of them has filled its buffers. The breaker stands in for the parent breaker with real
     * memory accounting: it compares the bytes actually held at the time of the call, plus the bytes being reserved, with the limit,
     * and does not account for anything reserved earlier that has not been buffered yet.
     */
    public void testConcurrentLargeMessagesTripAsTheHeapFills() throws IOException {
        final int messageCount = 6;
        final int fragmentsPerMessage = 10;
        final int fragmentLength = 100;
        final int declaredSize = fragmentsPerMessage * fragmentLength;
        final long heapLimit = 4000;
        assertThat((long) messageCount * declaredSize, greaterThan(heapLimit));

        final List<ReleasableBytesReference> fragments = new ArrayList<>();
        final LongSupplier heapUsed = () -> fragments.stream()
            .filter(ReleasableBytesReference::hasReferences)
            .mapToLong(ReleasableBytesReference::length)
            .sum();
        final CircuitBreaker realMemoryBreaker = new NoopCircuitBreaker("test") {
            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
                if (heapUsed.getAsLong() + bytes > heapLimit) {
                    throw new CircuitBreakingException("heap would exceed the limit", getDurability());
                }
            }
        };

        // All the headers arrive up front, as they do when several shards respond at the same moment. Each message is declared as
        // smaller than the limit, so none of them could be rejected on the strength of its header alone.
        final boolean compressed = randomBoolean();
        final List<InboundAggregator> aggregators = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            final InboundAggregator messageAggregator = new InboundAggregator(() -> realMemoryBreaker, action -> true);
            messageAggregator.headerReceived(requestHeader(declaredSize, compressed));
            if (compressed) {
                messageAggregator.updateCompressionScheme(randomFrom(Compression.Scheme.values()));
            }
            aggregators.add(messageAggregator);
        }

        // The messages then fill up in step with each other
        long peakHeapUsed = 0;
        for (int fragmentIndex = 0; fragmentIndex < fragmentsPerMessage; fragmentIndex++) {
            for (InboundAggregator messageAggregator : aggregators) {
                final ReleasableBytesReference fragment = fragment(fragmentLength);
                fragments.add(fragment);
                messageAggregator.aggregate(fragment);
                fragment.close();
                peakHeapUsed = Math.max(peakHeapUsed, heapUsed.getAsLong());
            }
        }

        assertThat("buffered more than the limit allows", peakHeapUsed, lessThanOrEqualTo(heapLimit));

        int tripped = 0;
        for (InboundAggregator messageAggregator : aggregators) {
            try (InboundMessage aggregated = messageAggregator.finishAggregation()) {
                if (aggregated.isShortCircuit()) {
                    assertThat(aggregated.getException(), instanceOf(CircuitBreakingException.class));
                    tripped++;
                }
            }
        }
        assertThat("some messages must have been rejected", tripped, greaterThan(0));
        assertThat("everything buffered must have been released", heapUsed.getAsLong(), equalTo(0L));
    }

}
