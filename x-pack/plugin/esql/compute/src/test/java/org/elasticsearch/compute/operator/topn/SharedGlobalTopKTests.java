/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link SharedGlobalTopK}: global heap merge, publish timing, and correctness
 * under multiple merge rounds.
 */
public class SharedGlobalTopKTests extends ESTestCase {
    private final CircuitBreaker breaker = new NoopCircuitBreaker(CircuitBreaker.REQUEST);
    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    public void testPartialGlobalHeapDoesNotPublish() {
        SharedMinCompetitive.Supplier minCompetitiveSupplier = minCompetitiveSupplier(false, false);
        try (
            SharedMinCompetitive minCompetitive = minCompetitiveSupplier.get();
            SharedGlobalTopK globalTopK = globalTopKSupplier(3, minCompetitiveSupplier).get()
        ) {
            assertFalse(globalTopK.mergeKeys(encodedKeys(false, false, 100L, 200L)));
            assertThat(minCompetitive.rawBound(), nullValue());
            assertThat(globalTopK.publishCount(), equalTo(0));
        }
    }

    public void testMergePublishesWhenGlobalHeapFull() {
        SharedMinCompetitive.Supplier minCompetitiveSupplier = minCompetitiveSupplier(false, false);
        try (SharedGlobalTopK globalTopK = globalTopKSupplier(2, minCompetitiveSupplier).get()) {
            assertTrue(globalTopK.mergeKeys(encodedKeys(false, false, 1_000L, 2_000L)));
            assertThat(globalTopK.publishCount(), equalTo(1));
        }
    }

    public void testEmptyKeyListIsNoOp() {
        SharedMinCompetitive.Supplier minCompetitiveSupplier = minCompetitiveSupplier(false, false);
        try (SharedGlobalTopK globalTopK = globalTopKSupplier(2, minCompetitiveSupplier).get()) {
            // First fill the global heap so it would publish if any key were contributed.
            assertTrue(globalTopK.mergeKeys(encodedKeys(false, false, 1_000L, 2_000L)));
            int publishesBefore = globalTopK.publishCount();

            // Passing an empty list must be a no-op: no publish, no exception.
            assertFalse(globalTopK.mergeKeys(List.of()));
            assertThat(globalTopK.publishCount(), equalTo(publishesBefore));
        }
    }

    /**
     * Two merge rounds for the same driver: the second round passes only the new key that entered
     * the local queue (evicting the previous worst). The bound must reflect the N-th best row
     * across both rounds, not be skewed by re-adding already-contributed rows.
     *
     * <p>Scenario (N=3, DESC sort — keeping largest timestamps):
     * <ol>
     *   <li>Round 1 delta = {ts=1, ts=2, ts=3}. Global fills: bound = ts=1 (worst of {1,2,3}).</li>
     *   <li>ts=4 arrives; local evicts ts=1. Round 2 delta = {ts=4}.
     *       Correct global after merge: {ts=2, ts=3, ts=4}; bound = ts=2.</li>
     * </ol>
     * If the full local queue {ts=2, ts=3, ts=4} were re-passed in round 2, the global heap would
     * contain duplicates and publish a bound of ts=3 instead of the correct ts=2, which would cause
     * documents with timestamps between 2 and 3 to be incorrectly skipped.
     */
    public void testSecondMergeRoundDoesNotBiasGlobalBound() {
        SharedMinCompetitive.Supplier minCompetitiveSupplier = minCompetitiveSupplier(false, false);
        try (
            SharedMinCompetitive minCompetitive = minCompetitiveSupplier.get();
            SharedGlobalTopK globalTopK = globalTopKSupplier(3, minCompetitiveSupplier).get()
        ) {
            // Round 1: contribute {ts=1, ts=2, ts=3}.
            assertTrue(globalTopK.mergeKeys(encodedKeys(false, false, 1L, 2L, 3L)));
            BytesRef boundAfterRound1 = minCompetitive.rawBound();
            assertThat(boundAfterRound1, equalTo(encodedKey(false, false, 1L)));

            // Round 2: only the new row (ts=4) entered the local queue; old rows must NOT be re-passed.
            globalTopK.mergeKeys(encodedKeys(false, false, 4L));

            // The bound should now reflect {ts=2, ts=3, ts=4}: worst = ts=2.
            // If the full queue {ts=2, ts=3, ts=4} were re-added, duplicates would shift the bound to ts=3.
            BytesRef boundAfterRound2 = minCompetitive.rawBound();
            assertThat(boundAfterRound2, equalTo(encodedKey(false, false, 2L)));
        }
    }

    /**
     * Regression test for a refcount leak: {@link SharedGlobalTopK#closeSideChannel()} must also
     * close the {@link SharedMinCompetitive} reference it acquired in {@code build()}, otherwise the
     * {@code BreakingBytesRefBuilder} inside {@link SharedMinCompetitive} is never released.
     */
    public void testNoCircuitBreakerLeakAfterClose() {
        CircuitBreaker tracked = newLimitedBreaker(ByteSizeValue.ofGb(1));
        SharedMinCompetitive.Supplier minCompetitiveSupplier = new SharedMinCompetitive.Supplier(
            tracked,
            List.of(new SharedMinCompetitive.KeyConfig(ElementType.LONG, TopNEncoder.DEFAULT_SORTABLE.toSortable(false), false, false))
        );
        SharedGlobalTopK.Supplier globalTopKSupplier = new SharedGlobalTopK.Supplier(tracked, 2, minCompetitiveSupplier);
        try (SharedMinCompetitive minCompetitive = minCompetitiveSupplier.get(); SharedGlobalTopK globalTopK = globalTopKSupplier.get()) {
            // Trigger a publish so SharedMinCompetitive.value holds a live allocation.
            assertTrue(globalTopK.mergeKeys(encodedKeys(false, false, 1_000L, 2_000L)));
        }
        // ESTestCase @After asserts tracked.getUsed() == 0, catching any breaker leak.
    }

    public void testAllNullGlobalTopKMarksNoFurtherCandidates() {
        SharedMinCompetitive.Supplier minCompetitiveSupplier = minCompetitiveSupplier(false, true);
        try (
            SharedMinCompetitive minCompetitive = minCompetitiveSupplier.get();
            SharedGlobalTopK globalTopK = globalTopKSupplier(2, minCompetitiveSupplier).get()
        ) {
            assertTrue(globalTopK.mergeKeys(encodedKeys(false, true, null, null)));
            assertTrue(minCompetitive.noFurtherCandidates());
            assertThat(globalTopK.publishCount(), equalTo(1));
        }
    }

    private SharedMinCompetitive.Supplier minCompetitiveSupplier(boolean asc, boolean nullsFirst) {
        return new SharedMinCompetitive.Supplier(
            breaker,
            List.of(new SharedMinCompetitive.KeyConfig(ElementType.LONG, TopNEncoder.DEFAULT_SORTABLE.toSortable(asc), asc, nullsFirst))
        );
    }

    private SharedGlobalTopK.Supplier globalTopKSupplier(int topCount, SharedMinCompetitive.Supplier minCompetitive) {
        return new SharedGlobalTopK.Supplier(breaker, topCount, minCompetitive);
    }

    /** Returns encoded sort keys for the given values as a list ready to pass to {@link SharedGlobalTopK#mergeKeys}. */
    private List<BytesRef> encodedKeys(boolean asc, boolean nullsFirst, Long... values) {
        List<BytesRef> keys = new java.util.ArrayList<>(values.length);
        for (Long value : values) {
            keys.add(encodedKey(asc, nullsFirst, value));
        }
        return keys;
    }

    private BytesRef encodedKey(boolean asc, boolean nullsFirst, Long value) {
        LongBlock block = value == null
            ? blockFactory.newLongBlockBuilder(1).appendNull().build()
            : blockFactory.newLongBlockBuilder(1).appendLong(value).build();
        TopNRow row = new TopNRow(breaker, 32, 0);
        try {
            TopNEncoder encoder = TopNEncoder.DEFAULT_SORTABLE.toSortable(asc);
            byte nul = nullsFirst ? TopNOperator.SMALL_NULL : TopNOperator.BIG_NULL;
            byte nonNul = nullsFirst ? TopNOperator.BIG_NULL : TopNOperator.SMALL_NULL;
            KeyExtractorForLong extractor = KeyExtractorForLong.extractorFor(encoder, asc, nul, nonNul, block);
            extractor.writeKey(row.keys, 0);
            return BytesRef.deepCopyOf(row.keys.bytesRefView());
        } finally {
            Releasables.close(block, row);
        }
    }
}
