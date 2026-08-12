/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
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
 * Unit tests for {@link SharedGlobalTopK}: global heap merge, publish timing, and dirty-skip.
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
            try (TopNQueue local = localQueue(3, false, false, 100L, 200L)) {
                assertFalse(globalTopK.mergeLocalHeap(local, null));
            }
            assertThat(minCompetitive.minCompetitiveValue(), nullValue());
            assertThat(globalTopK.publishCount(), equalTo(0));
        }
    }

    public void testMergePublishesWhenGlobalHeapFull() {
        SharedMinCompetitive.Supplier minCompetitiveSupplier = minCompetitiveSupplier(false, false);
        try (SharedGlobalTopK globalTopK = globalTopKSupplier(2, minCompetitiveSupplier).get()) {
            try (TopNQueue local = localQueue(2, false, false, 1_000L, 2_000L)) {
                assertTrue(globalTopK.mergeLocalHeap(local, null));
            }
            assertThat(globalTopK.publishCount(), equalTo(1));
        }
    }

    public void testSkipsMergeWhenLocalWorstKeptUnchanged() {
        SharedMinCompetitive.Supplier minCompetitiveSupplier = minCompetitiveSupplier(false, false);
        try (SharedGlobalTopK globalTopK = globalTopKSupplier(2, minCompetitiveSupplier).get()) {
            try (TopNQueue local = localQueue(2, false, false, 1_000L, 2_000L)) {
                assertTrue(globalTopK.mergeLocalHeap(local, null));
                var worstKept = local.top().keys.bytesRefView();
                assertFalse(globalTopK.mergeLocalHeap(local, worstKept));
            }
            assertThat(globalTopK.mergesSkippedUnchanged(), equalTo(1));
            assertThat(globalTopK.publishCount(), equalTo(1));
        }
    }

    public void testAllNullGlobalTopKMarksNoFurtherCandidates() {
        SharedMinCompetitive.Supplier minCompetitiveSupplier = minCompetitiveSupplier(false, true);
        try (
            SharedMinCompetitive minCompetitive = minCompetitiveSupplier.get();
            SharedGlobalTopK globalTopK = globalTopKSupplier(2, minCompetitiveSupplier).get()
        ) {
            try (TopNQueue local = localQueue(2, false, true, null, null)) {
                assertTrue(globalTopK.mergeLocalHeap(local, null));
            }
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

    private TopNQueue localQueue(int topCount, boolean asc, boolean nullsFirst, Long... values) {
        TopNQueue queue = TopNQueue.build(breaker, topCount);
        for (Long value : values) {
            TopNRow row = longRow(value, asc, nullsFirst);
            TopNRow leftover = queue.addRow(row);
            if (leftover != null) {
                leftover.close();
            }
        }
        return queue;
    }

    private TopNRow longRow(Long value, boolean asc, boolean nullsFirst) {
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
        } finally {
            Releasables.close(block);
        }
        return row;
    }
}
