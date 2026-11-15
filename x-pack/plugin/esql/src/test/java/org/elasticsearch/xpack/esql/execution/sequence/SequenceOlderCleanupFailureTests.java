/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.AbstractList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType.NONE;
import static org.hamcrest.Matchers.containsString;

/** Faults only breaker admission/refunds; actual values, blocks, readers and matchers retain their normal ownership. */
public class SequenceOlderCleanupFailureTests extends ESTestCase {
    public void testCandidateCopyKeepsLayoutFailureWhenContainerRefundFails() {
        AtomicLong used = new AtomicLong();
        var cleanup = new IllegalStateException("candidate container refund failed");
        var breaker = new NoopCircuitBreaker("candidate-refund") {
            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
                used.addAndGet(bytes);
            }

            @Override
            public void addWithoutBreaking(long bytes, String label) {
                used.addAndGet(bytes);
                throw cleanup;
            }
        };
        var blocks = blocks(new NoopCircuitBreaker("candidate-blocks"));
        try (Page page = new Page(blocks.newConstantIntBlockWith(7, 1))) {
            var demands = List.of(demand("first"), demand("missing"));
            var failure = expectThrows(
                IllegalArgumentException.class,
                () -> SequenceCandidateValues.copyRow(page, 0, demands, new int[] { 0, 1 }, blocks, breaker)
            );
            assertThat(failure.getMessage(), containsString("missing mandatory sequence column channel"));
            assertEquals(1, failure.getSuppressed().length);
            assertSame(cleanup, failure.getSuppressed()[0]);
            assertEquals(0, used.get());
        }
        blocks.ensureAllBlocksAreReleased();
    }

    public void testHitReaderKeepsConversionFailureWhenBuiltColumnRefundFails() {
        AtomicBoolean armed = new AtomicBoolean();
        AtomicInteger refunds = new AtomicInteger();
        var breaker = new NoopCircuitBreaker("reader-refunds") {
            @Override
            public void addWithoutBreaking(long bytes) {
                if (armed.get() && bytes < 0) {
                    throw new IllegalStateException("reader refund failure " + refunds.incrementAndGet());
                }
            }
        };
        var blocks = blocks(breaker);
        SearchHit hit = SearchHit.unpooled(0, "document");
        hit.setDocumentField(new DocumentField("keyword", List.of(new BytesRef("valid"))));
        // Arm only after the first real column has been built, just before the invalid long is converted.
        hit.setDocumentField(new DocumentField("long", new AbstractList<>() {
            @Override
            public Object get(int index) {
                armed.set(true);
                return "invalid";
            }

            @Override
            public int size() {
                return 1;
            }
        }));
        var reader = new SequenceHitReader(
            List.of(field("keyword", DataType.KEYWORD), field("long", DataType.LONG)),
            EsqlTestUtils.TEST_CFG
        );
        var failure = expectThrows(IllegalArgumentException.class, () -> reader.read(new SearchHit[] { hit }, blocks));
        assertThat(failure.getMessage(), containsString("cannot be represented as [long]"));
        assertTrue("builder and already-built column cleanup both remain attached", failure.getSuppressed().length >= 2);
        assertTrue(refunds.get() >= 2);
        armed.set(false);
        blocks.ensureAllBlocksAreReleased();
    }

    public void testMatcherCommitKeepsPrimaryWhenUnreturnedOutputRefundFails() {
        AtomicBoolean armed = new AtomicBoolean();
        AtomicLong used = new AtomicLong();
        AtomicInteger matcherAdmissions = new AtomicInteger();
        AtomicInteger refunds = new AtomicInteger();
        var primary = new IllegalArgumentException("primary committed-marker refund failure");
        var cleanup = new IllegalStateException("secondary unreturned-output refund failure");
        var breaker = new NoopCircuitBreaker("matcher-commit") {
            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
                used.addAndGet(bytes);
                if (armed.get() && label.equals("esql_sequence_matcher")) {
                    matcherAdmissions.incrementAndGet();
                }
            }

            @Override
            public void addWithoutBreaking(long bytes, String label) {
                used.addAndGet(bytes);
                if (armed.get() && bytes < 0 && label.equals("esql_sequence_matcher") && matcherAdmissions.get() >= 2) {
                    int refund = refunds.getAndIncrement();
                    if (refund == 0) {
                        throw primary;
                    }
                    if (refund == 1) {
                        throw cleanup;
                    }
                }
            }
        };
        try (var matcher = matcher(breaker)) {
            assertNull(matcher.match(0, new BytesRef("k"), new SequenceOrdinal(0, 0, 0), new BytesRef("start"), null));
            armed.set(true);
            var failure = expectThrows(
                IllegalArgumentException.class,
                () -> matcher.match(1, new BytesRef("k"), new SequenceOrdinal(1, 0, 1), new BytesRef("final"), null)
            );
            assertSame(primary, failure);
            assertEquals(1, failure.getSuppressed().length);
            assertSame(cleanup, failure.getSuppressed()[0]);
            assertEquals(0, used.get());
        }
    }

    public void testMatcherCompletionKeepsHandleAdmissionFailureWhenOutputRefundFails() {
        AtomicBoolean armed = new AtomicBoolean();
        AtomicBoolean rejected = new AtomicBoolean();
        AtomicBoolean refunded = new AtomicBoolean();
        AtomicLong used = new AtomicLong();
        var primary = new IllegalArgumentException("primary final-handle admission failure");
        var cleanup = new IllegalStateException("secondary incomplete-output refund failure");
        var breaker = new NoopCircuitBreaker("matcher-completion") {
            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
                if (armed.get() && label.equals("esql_sequence_payload") && rejected.compareAndSet(false, true)) {
                    throw primary;
                }
                used.addAndGet(bytes);
            }

            @Override
            public void addWithoutBreaking(long bytes, String label) {
                used.addAndGet(bytes);
                if (rejected.get() && bytes < 0 && label.equals("esql_sequence_matcher") && refunded.compareAndSet(false, true)) {
                    throw cleanup;
                }
            }
        };
        try (var matcher = matcher(breaker)) {
            assertNull(matcher.match(0, new BytesRef("k"), new SequenceOrdinal(0, 0, 0), new BytesRef("start"), null));
            armed.set(true);
            var failure = expectThrows(
                IllegalArgumentException.class,
                () -> matcher.match(1, new BytesRef("k"), new SequenceOrdinal(1, 0, 1), new BytesRef("final"), null)
            );
            assertSame(primary, failure);
            assertEquals(1, failure.getSuppressed().length);
            assertSame(cleanup, failure.getSuppressed()[0]);
            assertEquals(0, used.get());
        }
    }

    private static SequenceLatestValidMatcher matcher(CircuitBreaker breaker) {
        return new SequenceLatestValidMatcher(
            2,
            TimeValue.timeValueSeconds(10),
            breaker,
            () -> false,
            BytesRef::compareTo,
            null,
            SequenceLatestValidConfig.defaults(List.of(List.of(), List.of()), List.of())
        );
    }

    private static SequenceLatestValidConfig.ValueDemand demand(String name) {
        return new SequenceLatestValidConfig.ValueDemand(new ReferenceAttribute(Source.EMPTY, null, name, DataType.INTEGER));
    }

    private static Attribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, null, null, name, new EsField(name, type, Map.of(), true, NONE), false);
    }

    private static MockBlockFactory blocks(CircuitBreaker breaker) {
        return new MockBlockFactory(BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker));
    }
}
