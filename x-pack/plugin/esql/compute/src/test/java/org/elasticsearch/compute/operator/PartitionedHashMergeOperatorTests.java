/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.HashImplFactory;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end tests for {@link PartitionedHashMergeOperator}: the coordinator-side counterpart to
 * {@link PartitionedHashAggregationOperator}. Each test drives the full data-node → coordinator
 * pipeline:
 * <ol>
 *   <li>Raw (key, value) pages → {@link PartitionedHashAggregationOperator} → intermediate pages
 *       (some tagged with a partition id, some untagged depending on whether conversion happened).
 *   <li>Intermediate pages → {@link PartitionedHashMergeOperator} → final (key, sum) pages.
 *   <li>Final pages are validated against the expected per-key sums.
 * </ol>
 */
public class PartitionedHashMergeOperatorTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = new BlockFactory(new NoopCircuitBreaker("test-noop"), BigArrays.NON_RECYCLING_INSTANCE);
        assumeTrue("SwissHash not available on this JVM", HashImplFactory.SWISS_HASH_AVAILABLE);
    }

    /**
     * Data node never converts (high threshold) → all intermediate pages are untagged.
     * Merge operator stays in the non-promoted single-table path.
     */
    public void testNonPromotedPath() {
        Map<Long, Long> oracle = new HashMap<>();
        List<Page> raw = rawInput(500, 20, oracle);

        List<Page> intermediate = runDataNodeOp(raw, 8, 10_000 /* never crossed */);
        assertTrue(
            "expected only untagged pages when conversion threshold is never crossed",
            intermediate.stream().allMatch(p -> p.partitionId() == null)
        );

        Map<Long, Long> actual = runMergeOp(intermediate, 8);
        assertThat(actual, equalTo(oracle));
    }

    /**
     * Data node converts to partitioned mode → intermediate pages are tagged.
     * Merge operator promotes on the first tagged page and routes subsequent ones to the correct worker.
     */
    public void testPromotedPath() {
        Map<Long, Long> oracle = new HashMap<>();
        List<Page> raw = rawInput(4_000, 200, oracle);

        List<Page> intermediate = runDataNodeOp(raw, 8, 50 /* crossed quickly */);
        assertTrue("expected at least some tagged pages after conversion", intermediate.stream().anyMatch(p -> p.partitionId() != null));

        Map<Long, Long> actual = runMergeOp(intermediate, 8);
        assertThat(actual, equalTo(oracle));
    }

    /**
     * Two simulated data nodes: one converts (produces tagged pages), one doesn't (produces untagged
     * pages). The merge operator must correctly handle the mix — promoting on the first tagged page
     * and routing untagged pages to the NONE table for reconciliation at finish().
     */
    public void testMixedTaggedAndUntagged() {
        Map<Long, Long> oracle = new HashMap<>();

        // Node A: converts
        List<Page> rawA = rawInput(3_000, 150, oracle);
        List<Page> intermediateA = runDataNodeOp(rawA, 8, 30);

        // Node B: never converts
        List<Page> rawB = rawInput(500, 20, oracle);
        List<Page> intermediateB = runDataNodeOp(rawB, 8, 10_000);

        // Interleave pages from both nodes as the coordinator would see them.
        List<Page> allIntermediate = new ArrayList<>();
        allIntermediate.addAll(intermediateA);
        allIntermediate.addAll(intermediateB);

        Map<Long, Long> actual = runMergeOp(allIntermediate, 8);
        assertThat(actual, equalTo(oracle));
    }

    /**
     * Tagged pages arrive BEFORE untagged ones, then more tagged pages follow. Verifies that
     * once promoted the NONE table continues accumulating untagged arrivals, and those are
     * correctly folded in at reconciliation.
     */
    public void testUntaggedArrivingAfterPromotion() {
        Map<Long, Long> oracle = new HashMap<>();

        List<Page> rawConverting = rawInput(4_000, 200, oracle);
        List<Page> tagged = runDataNodeOp(rawConverting, 4, 30);

        List<Page> rawUntagged = rawInput(800, 50, oracle);
        List<Page> untagged = runDataNodeOp(rawUntagged, 4, 10_000);

        // Feed tagged first so the operator promotes, then feed untagged.
        List<Page> ordered = new ArrayList<>(tagged);
        ordered.addAll(untagged);

        Map<Long, Long> actual = runMergeOp(ordered, 4);
        assertThat(actual, equalTo(oracle));
    }

    /**
     * Exercises the null-key path: null grouping keys should be routed to partition 0 (NULL_PARTITION)
     * on the data node and again to partition 0 during NONE-table reconciliation on the coordinator.
     */
    public void testNullKeyHandling() {
        Map<Long, Long> oracle = new HashMap<>();
        List<Page> raw = rawInputWithNulls(3_000, 100, oracle);

        List<Page> intermediate = runDataNodeOp(raw, 8, 30);
        Map<Long, Long> actual = runMergeOp(intermediate, 8);
        assertThat(actual, equalTo(oracle));
    }

    // ---- helpers ----

    /**
     * Builds {@code rows} random (LONG key, LONG value) pairs over {@code cardinality} distinct
     * keys, split across small pages, updating {@code oracle} (key → expected sum) in place.
     */
    private List<Page> rawInput(int rows, int cardinality, Map<Long, Long> oracle) {
        List<Page> pages = new ArrayList<>();
        int remaining = rows;
        while (remaining > 0) {
            int pageSize = Math.min(remaining, between(50, 300));
            remaining -= pageSize;
            try (
                LongBlock.Builder keyBuilder = blockFactory.newLongBlockBuilder(pageSize);
                LongBlock.Builder valueBuilder = blockFactory.newLongBlockBuilder(pageSize)
            ) {
                for (int i = 0; i < pageSize; i++) {
                    long key = randomLongBetween(0, cardinality - 1);
                    long value = randomLongBetween(-1_000, 1_000);
                    keyBuilder.appendLong(key);
                    valueBuilder.appendLong(value);
                    oracle.merge(key, value, Long::sum);
                }
                pages.add(new Page(keyBuilder.build(), valueBuilder.build()));
            }
        }
        return pages;
    }

    /** Like {@link #rawInput} but occasionally emits null keys (folded into oracle key 0L). */
    private List<Page> rawInputWithNulls(int rows, int cardinality, Map<Long, Long> oracle) {
        List<Page> pages = new ArrayList<>();
        int remaining = rows;
        while (remaining > 0) {
            int pageSize = Math.min(remaining, between(50, 300));
            remaining -= pageSize;
            try (
                LongBlock.Builder keyBuilder = blockFactory.newLongBlockBuilder(pageSize);
                LongBlock.Builder valueBuilder = blockFactory.newLongBlockBuilder(pageSize)
            ) {
                for (int i = 0; i < pageSize; i++) {
                    long value = randomLongBetween(-1_000, 1_000);
                    if (rarely()) {
                        keyBuilder.appendNull();
                        oracle.merge(0L, value, Long::sum);
                    } else {
                        long key = randomLongBetween(0, cardinality - 1);
                        keyBuilder.appendLong(key);
                        oracle.merge(key, value, Long::sum);
                    }
                    valueBuilder.appendLong(value);
                }
                pages.add(new Page(keyBuilder.build(), valueBuilder.build()));
            }
        }
        return pages;
    }

    /**
     * Runs {@link PartitionedHashAggregationOperator} on {@code raw} pages (grouping key at
     * channel 0, value at channel 1) and returns all emitted intermediate pages. Tagged pages have
     * {@link Page#partitionId()} set; untagged pages have it null.
     * <p>
     *     The returned pages are owned by the caller; they must be passed to
     *     {@link #runMergeOp} (which consumes them) or released explicitly.
     */
    private List<Page> runDataNodeOp(List<Page> raw, int partitionCount, int conversionThreshold) {
        SumLongAggregatorFunctionSupplier sumSupplier = new SumLongAggregatorFunctionSupplier(TestWarningsSource.INSTANCE);
        PartitionedHashAggregationOperator op = new PartitionedHashAggregationOperator.Builder().groupSpecs(
            List.of(new BlockHash.GroupSpec(0, ElementType.LONG))
        )
            .aggregators(List.of(new PartitionedHashAggregationOperator.AggregatorSpec(sumSupplier, List.of(1))))
            .partitionCount(partitionCount)
            .partitionConversionThreshold(conversionThreshold)
            .perPartitionEmit(Integer.MAX_VALUE, 1.0) // disable periodic early emit; only finish() emits
            .maxPageSize(Integer.MAX_VALUE)
            .aggregationBatchSize(Integer.MAX_VALUE)
            .build()
            .get(driverContext());
        List<Page> output = new ArrayList<>();
        try {
            for (Page page : raw) {
                op.addInput(copyPage(page)); // data-node op's addInput releases the copy's blocks
                Page out;
                while ((out = op.getOutput()) != null) {
                    output.add(out);
                }
            }
            op.finish();
            Page out;
            while ((out = op.getOutput()) != null) {
                output.add(out);
            }
        } finally {
            op.close();
        }
        return output;
    }

    /**
     * Feeds {@code intermediatePages} (from {@link #runDataNodeOp}) through a
     * {@link PartitionedHashMergeOperator} and returns the per-key final sums.
     * <p>
     *     Consumes (releases) all pages in {@code intermediatePages}.
     */
    private Map<Long, Long> runMergeOp(List<Page> intermediatePages, int partitionCount) {
        SumLongAggregatorFunctionSupplier sumSupplier = new SumLongAggregatorFunctionSupplier(TestWarningsSource.INSTANCE);
        // The data-node op emits intermediate pages with: key at ch 0, sum+seen+failed at ch 1,2,3.
        int intermediateBlockCount = sumSupplier.groupingIntermediateStateDesc().size(); // 3 for SumLong
        List<Integer> intChannels = new ArrayList<>(intermediateBlockCount);
        for (int c = 0; c < intermediateBlockCount; c++) {
            intChannels.add(1 + c); // key is at 0; intermediate state starts at 1
        }

        DriverContext driverContext = driverContext();
        PartitionedHashMergeOperator mergeOp = new PartitionedHashMergeOperator.Builder().groupSpecs(
            List.of(new BlockHash.GroupSpec(0, ElementType.LONG))
        )
            .aggregators(List.of(new PartitionedHashMergeOperator.AggregatorSpec(sumSupplier, intChannels)))
            .partitionCount(partitionCount)
            .maxPageSize(Integer.MAX_VALUE)
            .aggregationBatchSize(Integer.MAX_VALUE)
            .build()
            .get(driverContext);
        try {
            for (Page page : intermediatePages) {
                assertTrue(mergeOp.needsInput());
                mergeOp.addInput(page); // consumes (releases) the page
                mergeOp.tryPromote(driverContext); // simulate Driver promotion check
            }
            mergeOp.finish();

            Map<Long, Long> actual = new HashMap<>();
            Page out;
            while ((out = mergeOp.getOutput()) != null) {
                // Final output: key at ch 0, sum at ch 1 (FINAL mode → one block per aggregator).
                LongBlock keys = out.getBlock(0);
                LongBlock sums = out.getBlock(1);
                for (int i = 0; i < out.getPositionCount(); i++) {
                    long key = keys.isNull(i) ? 0L : keys.getLong(i);
                    actual.merge(key, sums.getLong(i), Long::sum);
                }
                out.releaseBlocks();
            }
            return actual;
        } finally {
            mergeOp.close();
        }
    }

    private DriverContext driverContext() {
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
    }

    private Page copyPage(Page page) {
        Block[] blocks = new Block[page.getBlockCount()];
        boolean success = false;
        try {
            for (int i = 0; i < blocks.length; i++) {
                Block b = page.getBlock(i);
                blocks[i] = b.elementType()
                    .newBlockBuilder(b.getPositionCount(), blockFactory)
                    .copyFrom(b, 0, b.getPositionCount())
                    .build();
            }
            Page copy = new Page(blocks);
            success = true;
            return copy;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }
}
