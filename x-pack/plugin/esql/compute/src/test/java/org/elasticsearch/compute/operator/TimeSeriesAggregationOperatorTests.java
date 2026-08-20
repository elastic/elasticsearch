/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.DimensionValuesByteRefGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SumIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesBooleanAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.WindowAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.compute.test.operator.blocksource.ListRowsBlockSourceOperator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class TimeSeriesAggregationOperatorTests extends ComputeTestCase {

    private static final int HASH_CHANNEL_COUNT = 2;

    public void testValuesAggregator() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null, "test");
        List<BiFunction<List<Integer>, DriverContext, GroupingAggregatorFunction>> functions = List.of(
            (channels, ctx) -> new ValuesBooleanAggregatorFunctionSupplier().groupingAggregator(ctx, channels),
            (channels, ctx) -> new ValuesIntAggregatorFunctionSupplier().groupingAggregator(ctx, channels),
            (channels, ctx) -> new ValuesLongAggregatorFunctionSupplier().groupingAggregator(ctx, channels),
            (channels, ctx) -> new ValuesBytesRefAggregatorFunctionSupplier().groupingAggregator(ctx, channels),
            DimensionValuesByteRefGroupingAggregatorFunction::new
        );
        for (var fn : functions) {
            try (GroupingAggregatorFunction aggregator = fn.apply(List.of(randomNonNegativeInt()), driverContext)) {
                assertTrue(TimeSeriesAggregationOperator.isValuesAggregator(aggregator));
            }
        }
    }

    /**
     * A non-multiple window (7m over 5m buckets) merges the boundary bucket's partial channel into the buckets fully
     * covered by the window. The partial channel is filtered to the trailing 2m of each bucket, so the value of the
     * bucket labeled {@code L} covers {@code [L - 2m, L + 5m)}. Verified end to end through a SINGLE-mode pipeline
     * with two TSIDs.
     */
    public void testNonMultipleWindowMergesPartialChannel() {
        Rounding.Prepared fiveMinBucket = Rounding.builder(TimeValue.timeValueMinutes(5)).build().prepareForUnknown();

        final long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13");
        long baseTime = fiveMinBucket.round(startTime);

        List<List<Object>> rows = new ArrayList<>();
        for (String tsid : List.of("a", "b")) {
            int value = tsid.equals("a") ? 1 : 10;
            for (int i = 0; i < 15; i++) {
                long ts = baseTime + TimeValue.timeValueMinutes(i).millis();
                rows.add(List.of(tsid, fiveMinBucket.round(ts), value, ts));
            }
        }

        List<Page> results = runPartialWindowPipeline(fiveMinBucket, Duration.ofMinutes(7), Duration.ofMinutes(2), rows);
        List<OutputRow> outputRows = extractRows(results);

        // 2 TSIDs × 3 buckets (0, 5m, 10m)
        assertThat(outputRows.size(), equalTo(6));
        outputRows.sort(Comparator.comparing(OutputRow::tsid).thenComparingLong(OutputRow::bucket));
        // TSID "a": each point has value 1.
        assertThat(outputRows.get(0).value(), equalTo(5L));  // [-2m,5m) -> minutes 0..4
        assertThat(outputRows.get(1).value(), equalTo(7L));  // [3m,10m) -> minutes 3..9
        assertThat(outputRows.get(2).value(), equalTo(7L));  // [8m,15m) -> minutes 8..14
        // TSID "b": each point has value 10.
        assertThat(outputRows.get(3).value(), equalTo(50L));
        assertThat(outputRows.get(4).value(), equalTo(70L));
        assertThat(outputRows.get(5).value(), equalTo(70L));
    }

    /**
     * An exact-multiple window (10m over 5m buckets) merges the two covered buckets, no partial channel involved.
     */
    public void testExactMultipleWindow() {
        Rounding.Prepared fiveMinBucket = Rounding.builder(TimeValue.timeValueMinutes(5)).build().prepareForUnknown();
        Duration windowDuration = Duration.ofMinutes(10);

        final long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13");
        long baseTime = fiveMinBucket.round(startTime);

        List<List<Object>> rows = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            long ts = baseTime + TimeValue.timeValueMinutes(i * 5L).millis();
            rows.add(List.of("x", ts, 3));
        }

        List<Page> results = runPipeline(fiveMinBucket, windowDuration, rows);

        List<OutputRow> outputRows = extractRows(results);
        assertThat(outputRows.size(), equalTo(4));
        for (OutputRow row : outputRows) {
            assertThat(fiveMinBucket.round(row.bucket()), equalTo(row.bucket()));
        }
        outputRows.sort(Comparator.comparingLong(OutputRow::bucket));
        assertThat(outputRows.get(0).value(), equalTo(3L));
        assertThat(outputRows.get(1).value(), equalTo(6L));
        assertThat(outputRows.get(2).value(), equalTo(6L));
        assertThat(outputRows.get(3).value(), equalTo(6L));
    }

    /**
     * Verifies that a VALUES-like aggregator combined with a window aggregator produces correct results for groups
     * created by {@code expandWindowBuckets}: the values aggregator must be remapped to the source group that has
     * dimension data, and the windowed sum of a gap bucket comes from the merged neighbors.
     */
    public void testSelectedForValuesAggregatorMapsExpandedGroupsViaOriginalNumGroups() {
        Rounding.Prepared fiveMinBucket = Rounding.builder(TimeValue.timeValueMinutes(5)).build().prepareForUnknown();
        Duration windowDuration = Duration.ofMinutes(10);

        final long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13");
        long baseTime = fiveMinBucket.round(startTime);

        List<List<Object>> rows = new ArrayList<>();
        // Two sparse data points 15 minutes apart: the 5m and 10m buckets exist only through window expansion.
        rows.add(List.of("tsid1", baseTime, 5));
        rows.add(List.of("tsid1", baseTime + TimeValue.timeValueMinutes(15).millis(), 10));

        // Use both a window aggregator (sum) and a values aggregator (values of the tsid column)
        List<GroupingAggregator.Factory> aggregatorFactories = List.of(
            new WindowAggregatorFunctionSupplier(new SumIntAggregatorFunctionSupplier(), windowDuration).groupingAggregatorFactory(
                AggregatorMode.SINGLE,
                List.of(HASH_CHANNEL_COUNT)
            ),
            new org.elasticsearch.compute.aggregation.ValuesBytesRefAggregatorFunctionSupplier().groupingAggregatorFactory(
                AggregatorMode.SINGLE,
                List.of(0)
            )
        );

        var operatorFactory = new TimeSeriesAggregationOperator.Factory(
            fiveMinBucket,
            false,
            List.of(
                new BlockHash.GroupSpec(0, ElementType.BYTES_REF, null, null),
                new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
            ),
            AggregatorMode.SINGLE,
            aggregatorFactories,
            10_000
        );

        BlockFactory blockFactory = blockFactory();
        var driverCtx = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        var source = new ListRowsBlockSourceOperator(
            driverCtx.blockFactory(),
            List.of(ElementType.BYTES_REF, ElementType.LONG, ElementType.INT),
            rows
        );
        List<Page> results = new ArrayList<>();
        try (
            var driver = TestDriverFactory.create(
                driverCtx,
                source,
                List.of(operatorFactory.get(driverCtx)),
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            new TestDriverRunner().run(driver);
        }

        BytesRef expectedTsid = new BytesRef("tsid1");
        record ValuedRow(long bucket, Long sum) {}
        List<ValuedRow> outputRows = new ArrayList<>();
        for (Page page : results) {
            // block 0: tsid key, block 1: timestamp key, block 2: windowed sum, block 3: values(tsid)
            assertThat("expected 4 blocks (2 keys + 2 agg results)", page.getBlockCount(), equalTo(4));
            LongBlock buckets = page.getBlock(1);
            LongBlock sums = page.getBlock(2);
            BytesRefBlock valuesBlock = page.getBlock(3);
            var scratch = new BytesRef();
            for (int p = 0; p < page.getPositionCount(); p++) {
                // The values aggregator must produce the tsid value for every output row,
                // including rows whose group was created by expandWindowBuckets
                assertFalse("values block must not be null at position " + p, valuesBlock.isNull(p));
                BytesRef val = valuesBlock.getBytesRef(valuesBlock.getFirstValueIndex(p), scratch);
                assertThat(val, equalTo(expectedTsid));
                outputRows.add(new ValuedRow(buckets.getLong(p), sums.isNull(p) ? null : sums.getLong(p)));
            }
        }
        outputRows.sort(Comparator.comparingLong(ValuedRow::bucket));
        // Expansion materializes buckets whose window still covers a data bucket, so the 10m bucket
        // (window [5m,15m), empty) is never created.
        assertThat(outputRows.size(), equalTo(3));
        assertThat(outputRows.get(0).sum(), equalTo(5L));   // [-5m,5m) -> the first point
        assertThat(outputRows.get(1).sum(), equalTo(5L));   // [0,10m) -> the first point, expanded group
        assertThat(outputRows.get(2).sum(), equalTo(10L));  // [10m,20m) -> the second point
    }

    /**
     * An expanded gap bucket whose windowed value comes only from the boundary bucket's partial channel: with a 7m
     * window over 5m buckets, the data point in the trailing 2m of the first bucket is the sole contribution to the
     * following (data-free) bucket. The values aggregator must still be remapped to the group with dimension data.
     */
    public void testValuesAggregatorWithSparseDataAndPartialChannelWindow() {
        Rounding.Prepared fiveMinBucket = Rounding.builder(TimeValue.timeValueMinutes(5)).build().prepareForUnknown();

        final long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13");
        long baseTime = fiveMinBucket.round(startTime);

        List<List<Object>> rows = new ArrayList<>();
        // minute 4 sits in the trailing 2m of the first bucket; minute 12 is two buckets later
        rows.add(List.of("s1", baseTime, 5, baseTime + TimeValue.timeValueMinutes(4).millis()));
        rows.add(List.of("s1", baseTime + TimeValue.timeValueMinutes(10).millis(), 10, baseTime + TimeValue.timeValueMinutes(12).millis()));

        List<GroupingAggregator.Factory> aggregatorFactories = List.of(
            partialWindowAggregatorFactory(fiveMinBucket, Duration.ofMinutes(7), Duration.ofMinutes(2)),
            new org.elasticsearch.compute.aggregation.ValuesBytesRefAggregatorFunctionSupplier().groupingAggregatorFactory(
                AggregatorMode.SINGLE,
                List.of(0)
            )
        );

        var operatorFactory = new TimeSeriesAggregationOperator.Factory(
            fiveMinBucket,
            false,
            List.of(
                new BlockHash.GroupSpec(0, ElementType.BYTES_REF, null, null),
                new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
            ),
            AggregatorMode.SINGLE,
            aggregatorFactories,
            10_000
        );

        BlockFactory blockFactory = blockFactory();
        var driverCtx = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        var source = new ListRowsBlockSourceOperator(
            driverCtx.blockFactory(),
            List.of(ElementType.BYTES_REF, ElementType.LONG, ElementType.INT, ElementType.LONG),
            rows
        );
        List<Page> results = new ArrayList<>();
        try (
            var driver = TestDriverFactory.create(
                driverCtx,
                source,
                List.of(operatorFactory.get(driverCtx)),
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            new TestDriverRunner().run(driver);
        }

        BytesRef expectedTsid = new BytesRef("s1");
        record ValuedRow(long bucket, Long sum) {}
        List<ValuedRow> outputRows = new ArrayList<>();
        for (Page page : results) {
            assertThat("expected 4 blocks (2 keys + 2 agg results)", page.getBlockCount(), equalTo(4));
            LongBlock buckets = page.getBlock(1);
            LongBlock sums = page.getBlock(2);
            BytesRefBlock valuesBlock = page.getBlock(3);
            var scratch = new BytesRef();
            for (int p = 0; p < page.getPositionCount(); p++) {
                assertFalse("values block must not be null at position " + p, valuesBlock.isNull(p));
                BytesRef val = valuesBlock.getBytesRef(valuesBlock.getFirstValueIndex(p), scratch);
                assertThat("dimension value must be present for expanded groups", val, equalTo(expectedTsid));
                outputRows.add(new ValuedRow(buckets.getLong(p), sums.isNull(p) ? null : sums.getLong(p)));
            }
        }
        outputRows.sort(Comparator.comparingLong(ValuedRow::bucket));
        assertThat(outputRows.size(), equalTo(3));
        assertThat(outputRows.get(0).sum(), equalTo(5L));   // [-2m,5m) -> minute 4
        assertThat(outputRows.get(1).sum(), equalTo(5L));   // [3m,10m) -> minute 4 via the partial channel only
        assertThat(outputRows.get(2).sum(), equalTo(10L));  // [8m,15m) -> minute 12
    }

    /**
     * Time-series aggregations never emit partial results periodically (rate and {@code *_over_time} require every
     * sample of a {@code _tsid} to be aggregated by a single driver in one pass): nothing is emitted until finish(),
     * no matter how many high-cardinality batches are fed.
     */
    public void testDoesNotEmitPartialResultsPeriodically() {
        Rounding.Prepared oneMinBucket = Rounding.builder(TimeValue.timeValueMinutes(1)).build().prepareForUnknown();
        var operatorFactory = new TimeSeriesAggregationOperator.Factory(
            oneMinBucket,
            false,
            List.of(
                new BlockHash.GroupSpec(0, ElementType.BYTES_REF, null, null),
                new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
            ),
            AggregatorMode.INITIAL,
            List.of(new SumIntAggregatorFunctionSupplier().groupingAggregatorFactory(AggregatorMode.INITIAL, List.of(2))),
            1024,
            5
        );
        BlockFactory bf = blockFactory();
        var driverCtx = new DriverContext(bf.bigArrays(), bf, null);
        List<Page> collected = new ArrayList<>();
        try (Operator op = operatorFactory.get(driverCtx)) {
            for (String tsid : List.of("a", "b", "c", "d", "e", "f", "g", "h")) {
                List<List<Object>> batch = new ArrayList<>();
                addTsidRows(batch, tsid, 4);
                op.addInput(buildPage(bf, batch));
                assertNull("periodic partial emission must be disabled: no output before finish()", op.getOutput());
            }
            op.finish();
            drainInto(op, collected);
            assertTrue(op.isFinished());
            int emitted = summarize(collected).stream().mapToInt(PageSummary::positionCount).sum();
            assertThat("all groups are emitted once finished", emitted, equalTo(32));
        } finally {
            for (Page page : collected) {
                page.releaseBlocks();
            }
        }
    }

    /**
     * Low-cardinality partial-mode output that fits within {@code targetChunkRows} is emitted as a single page (no
     * chunking, preserving today's behaviour when the data fits).
     */
    public void testEmitsSinglePageInPartialModeWhenBelowChunkSize() {
        List<List<Object>> batch = new ArrayList<>();
        for (String tsid : List.of("a", "b", "c")) {
            addTsidRows(batch, tsid, 1);
        }
        List<PageSummary> pages = runPartialMode(100_000, List.of(batch));
        assertThat(pages.size(), equalTo(1));
        assertThat(pages.get(0).positionCount(), equalTo(3));
    }

    /**
     * When the partial-mode output exceeds {@code targetChunkRows} it is split into several pages, each no larger than
     * {@code targetChunkRows}, and the chunks together cover every group exactly once.
     */
    public void testChunksPartialOutputAboveChunkSize() {
        int targetChunkRows = 5;
        int groupsPerTsid = 4;
        List<String> tsids = List.of("a", "b", "c", "d");
        List<List<Object>> rows = new ArrayList<>();
        for (String tsid : tsids) {
            addTsidRows(rows, tsid, groupsPerTsid);
        }
        List<PageSummary> pages = runPartialMode(targetChunkRows, List.of(rows));
        assertThat("output larger than the chunk size must be chunked", pages.size(), greaterThan(1));
        int emitted = 0;
        for (PageSummary page : pages) {
            assertThat("no chunk exceeds targetChunkRows", page.positionCount(), lessThanOrEqualTo(targetChunkRows));
            emitted += page.positionCount();
        }
        assertThat("every group is emitted exactly once across chunks", emitted, equalTo(tsids.size() * groupsPerTsid));
    }

    /**
     * DimensionValues must survive output chunking: its single value builder is materialized once and the requested
     * subset is copied per page, so chunked partial output yields the correct value for every group without
     * re-building (and closing) the shared builder.
     */
    public void testDimensionValuesSupportsChunkedPartialOutput() {
        int targetChunkRows = 3;
        List<String> tsids = List.of("a", "b", "c", "d", "e", "f", "g");
        List<List<Object>> batch = new ArrayList<>();
        for (String tsid : tsids) {
            batch.add(List.of(tsid, 0L, 1)); // one group per tsid; more than targetChunkRows so the output is chunked
        }
        Rounding.Prepared oneMinBucket = Rounding.builder(TimeValue.timeValueMinutes(1)).build().prepareForUnknown();
        var operatorFactory = new TimeSeriesAggregationOperator.Factory(
            oneMinBucket,
            false,
            List.of(
                new BlockHash.GroupSpec(0, ElementType.BYTES_REF, null, null),
                new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
            ),
            AggregatorMode.INITIAL,
            List.of(
                new DimensionValuesByteRefGroupingAggregatorFunction.FunctionSupplier().groupingAggregatorFactory(
                    AggregatorMode.INITIAL,
                    List.of(0)
                )
            ),
            1024,
            targetChunkRows
        );
        BlockFactory bf = blockFactory();
        var driverCtx = new DriverContext(bf.bigArrays(), bf, null);
        List<Page> collected = new ArrayList<>();
        Map<String, String> dimensionByTsid = new HashMap<>();
        try (Operator op = operatorFactory.get(driverCtx)) {
            op.addInput(buildPage(bf, batch));
            op.finish();
            drainInto(op, collected);
            assertThat("output must be chunked into multiple pages", collected.size(), greaterThan(1));
            var scratch = new BytesRef();
            for (Page page : collected) {
                assertThat("no chunk exceeds targetChunkRows", page.getPositionCount(), lessThanOrEqualTo(targetChunkRows));
                BytesRefBlock tsidBlock = page.getBlock(0);
                BytesRefBlock dimensionBlock = page.getBlock(2); // 2 key blocks + the DimensionValues intermediate state
                for (int p = 0; p < page.getPositionCount(); p++) {
                    String tsid = tsidBlock.getBytesRef(tsidBlock.getFirstValueIndex(p), scratch).utf8ToString();
                    assertFalse("dimension value must be present at position " + p, dimensionBlock.isNull(p));
                    String dim = dimensionBlock.getBytesRef(dimensionBlock.getFirstValueIndex(p), scratch).utf8ToString();
                    dimensionByTsid.put(tsid, dim);
                }
            }
        } finally {
            for (Page page : collected) {
                page.releaseBlocks();
            }
        }
        Map<String, String> expected = new HashMap<>();
        for (String tsid : tsids) {
            expected.put(tsid, tsid);
        }
        assertThat("every tsid's dimension value is its own tsid, across all chunks", dimensionByTsid, equalTo(expected));
    }

    /**
     * Chunked partial pages are what reduction stages receive in distributed execution. Feed two independently chunked
     * initial outputs through intermediate and final aggregations, verifying both stages re-group keys across chunk
     * boundaries.
     */
    public void testReductionsConsumeChunkedPartialOutput() {
        int targetChunkRows = 5;
        int groupsPerTsid = 4;
        List<String> tsids = List.of("a", "b", "c", "d");
        List<List<Object>> firstShardRows = new ArrayList<>();
        List<List<Object>> secondShardRows = new ArrayList<>();
        Map<String, Long> expected = new HashMap<>();
        for (String tsid : tsids) {
            for (int t = 0; t < groupsPerTsid; t++) {
                firstShardRows.add(List.of(tsid, (long) t, 1));
                secondShardRows.add(List.of(tsid, (long) t, 10));
                expected.put(tsid + "/" + t, 11L);
            }
        }

        List<Page> partialPages = new ArrayList<>();
        List<Page> intermediatePages = new ArrayList<>();
        List<Page> finalPages = new ArrayList<>();
        try {
            partialPages.addAll(runInitialPages(targetChunkRows, firstShardRows));
            partialPages.addAll(runInitialPages(targetChunkRows, secondShardRows));
            assertThat("initial output must be chunked before final reduction", partialPages.size(), greaterThan(2));
            for (Page page : partialPages) {
                assertThat("partial chunk exceeds targetChunkRows", page.getPositionCount(), lessThanOrEqualTo(targetChunkRows));
            }

            Rounding.Prepared oneMinBucket = Rounding.builder(TimeValue.timeValueMinutes(1)).build().prepareForUnknown();
            var intermediateFactory = new TimeSeriesAggregationOperator.Factory(
                oneMinBucket,
                false,
                List.of(
                    new BlockHash.GroupSpec(0, ElementType.BYTES_REF, null, null),
                    new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
                ),
                AggregatorMode.INTERMEDIATE,
                List.of(new SumIntAggregatorFunctionSupplier().groupingAggregatorFactory(AggregatorMode.INTERMEDIATE, List.of(2, 3))),
                1024,
                targetChunkRows
            );
            BlockFactory bf = blockFactory();
            var driverCtx = new DriverContext(bf.bigArrays(), bf, null);
            try (Operator op = intermediateFactory.get(driverCtx)) {
                while (partialPages.isEmpty() == false) {
                    op.addInput(partialPages.remove(0));
                }
                op.finish();
                drainInto(op, intermediatePages);
                assertTrue("intermediate operator should be finished once drained", op.isFinished());
            }
            assertThat("intermediate output must also be chunked", intermediatePages.size(), greaterThan(1));
            for (Page page : intermediatePages) {
                assertThat("intermediate chunk exceeds targetChunkRows", page.getPositionCount(), lessThanOrEqualTo(targetChunkRows));
            }

            var finalFactory = new TimeSeriesAggregationOperator.Factory(
                oneMinBucket,
                false,
                List.of(
                    new BlockHash.GroupSpec(0, ElementType.BYTES_REF, null, null),
                    new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
                ),
                AggregatorMode.FINAL,
                List.of(new SumIntAggregatorFunctionSupplier().groupingAggregatorFactory(AggregatorMode.FINAL, List.of(2, 3))),
                1024,
                targetChunkRows
            );
            try (Operator op = finalFactory.get(driverCtx)) {
                while (intermediatePages.isEmpty() == false) {
                    op.addInput(intermediatePages.remove(0));
                }
                op.finish();
                drainInto(op, finalPages);
                assertTrue("final operator should be finished once drained", op.isFinished());
            }

            Map<String, Long> actual = new HashMap<>();
            for (OutputRow row : extractRows(finalPages)) {
                actual.put(row.tsid() + "/" + row.bucket(), row.value());
            }
            assertThat(actual, equalTo(expected));
        } finally {
            for (Page page : partialPages) {
                page.releaseBlocks();
            }
            for (Page page : intermediatePages) {
                page.releaseBlocks();
            }
            for (Page page : finalPages) {
                page.releaseBlocks();
            }
        }
    }

    /**
     * With the single {@code targetChunkRows} knob applied to final mode too, a FINAL aggregation whose selected group
     * count exceeds the target must slice its output into multiple pages, each no larger than the target, and the
     * concatenation of those pages must equal the result produced when the output is emitted as a single page. This
     * bounds the coordinator's peak memory during final evaluation.
     */
    public void testFinalOutputIsChunkedIntoMultiplePages() {
        int targetChunkRows = 5;
        int groupsPerTsid = 4;
        List<String> tsids = List.of("a", "b", "c", "d", "e");
        List<List<Object>> rows = new ArrayList<>();
        Map<String, Long> expected = new HashMap<>();
        for (String tsid : tsids) {
            for (int t = 0; t < groupsPerTsid; t++) {
                rows.add(List.of(tsid, (long) t, 3)); // distinct timestamps => distinct groups; tsids * groupsPerTsid > targetChunkRows
                expected.put(tsid + "/" + t, 3L);
            }
        }

        List<Page> chunked = new ArrayList<>();
        List<Page> single = new ArrayList<>();
        try {
            chunked.addAll(runInitialThenFinal(targetChunkRows, rows));
            assertThat("final output larger than the chunk size must be chunked", chunked.size(), greaterThan(1));
            for (Page page : chunked) {
                assertThat("no final chunk exceeds targetChunkRows", page.getPositionCount(), lessThanOrEqualTo(targetChunkRows));
            }

            single.addAll(runInitialThenFinal(Integer.MAX_VALUE, rows));
            assertThat("unchunked final output is a single page", single.size(), equalTo(1));

            assertThat("chunked final output has the expected values", toValueMap(chunked), equalTo(expected));
            assertThat("chunking does not change the final result", toValueMap(chunked), equalTo(toValueMap(single)));
        } finally {
            releasePages(chunked);
            releasePages(single);
        }
    }

    /**
     * Final chunking must compose with window-bucket expansion. {@code expandWindowBuckets} materializes extra groups
     * and {@code customizeSelected} remaps the VALUES-like aggregator's selection back to the source groups; chunking
     * then slices that remapped selection. The chunked output must equal the single-page output, and every (expanded)
     * group must keep its own tsid dimension.
     */
    public void testFinalChunkingWithWindowExpansion() {
        Rounding.Prepared oneMinBucket = Rounding.builder(TimeValue.timeValueMinutes(1)).build().prepareForUnknown();
        Duration windowDuration = Duration.ofMinutes(7);
        List<List<Object>> rows = sparseWindowRows();

        List<Page> chunked = new ArrayList<>();
        List<Page> single = new ArrayList<>();
        try {
            chunked.addAll(runWindowedSingle(7, oneMinBucket, windowSumAndValues(windowDuration), rows));
            assertThat("window-expanded output larger than the chunk size must be chunked", chunked.size(), greaterThan(1));
            for (Page page : chunked) {
                assertThat("no chunk exceeds targetChunkRows", page.getPositionCount(), lessThanOrEqualTo(7));
            }

            single.addAll(runWindowedSingle(Integer.MAX_VALUE, oneMinBucket, windowSumAndValues(windowDuration), rows));
            assertThat("unchunked output is a single page", single.size(), equalTo(1));

            List<WindowedRow> chunkedRows = extractWindowedRows(chunked);
            assertThat("chunking preserves the window-expanded result", asMap(chunkedRows), equalTo(asMap(extractWindowedRows(single))));
            for (WindowedRow row : chunkedRows) {
                assertThat("each expanded group keeps its own tsid dimension across chunks", row.dimension(), equalTo(row.tsid()));
            }
        } finally {
            releasePages(chunked);
            releasePages(single);
        }
    }

    /**
     * When window expansion is active the VALUES-like aggregator receives a selection that repeats/reorders group ids
     * (expanded groups remapped to their source group). Combined with chunking,
     * {@link DimensionValuesByteRefGroupingAggregatorFunction}'s {@code incRef} identity fast path must not fire for a
     * partial slice, so each chunk copies the correct dimension for every position.
     */
    public void testFinalChunkingWithReorderedSelection() {
        Rounding.Prepared oneMinBucket = Rounding.builder(TimeValue.timeValueMinutes(1)).build().prepareForUnknown();
        Duration windowDuration = Duration.ofMinutes(7);
        List<List<Object>> rows = sparseWindowRows();

        List<Page> chunked = new ArrayList<>();
        List<Page> single = new ArrayList<>();
        try {
            chunked.addAll(runWindowedSingle(7, oneMinBucket, windowSumAndDimensionValues(windowDuration), rows));
            assertThat("window-expanded output larger than the chunk size must be chunked", chunked.size(), greaterThan(1));
            for (Page page : chunked) {
                assertThat("no chunk exceeds targetChunkRows", page.getPositionCount(), lessThanOrEqualTo(7));
            }

            single.addAll(runWindowedSingle(Integer.MAX_VALUE, oneMinBucket, windowSumAndDimensionValues(windowDuration), rows));
            assertThat("unchunked output is a single page", single.size(), equalTo(1));

            List<WindowedRow> chunkedRows = extractWindowedRows(chunked);
            assertThat(
                "chunking preserves DimensionValues over the remapped selection",
                asMap(chunkedRows),
                equalTo(asMap(extractWindowedRows(single)))
            );
            for (WindowedRow row : chunkedRows) {
                assertThat(
                    "expanded/remapped group keeps its own tsid, so the incRef fast path must not leak across chunks",
                    row.dimension(),
                    equalTo(row.tsid())
                );
            }
        } finally {
            releasePages(chunked);
            releasePages(single);
        }
    }

    /**
     * Final chunking must also compose with the partial-channel merge of non-multiple windows: the merged windowed
     * values are sliced into pages and the chunked output must equal the single-page output.
     */
    public void testFinalChunkingWithPartialChannelWindow() {
        Rounding.Prepared fiveMinBucket = Rounding.builder(TimeValue.timeValueMinutes(5)).build().prepareForUnknown();
        long baseTime = fiveMinBucket.round(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13"));
        List<List<Object>> rows = new ArrayList<>();
        Map<String, Long> expected = new HashMap<>();
        for (String tsid : List.of("a", "b", "c", "d", "e", "f", "g", "h")) {
            // minute 4 sits in the trailing 2m of the first bucket, so it also feeds the next bucket's window
            rows.add(List.of(tsid, baseTime, 1, baseTime + TimeValue.timeValueMinutes(4).millis()));
            rows.add(
                List.of(tsid, baseTime + TimeValue.timeValueMinutes(5).millis(), 1, baseTime + TimeValue.timeValueMinutes(8).millis())
            );
            expected.put(tsid + "/" + baseTime, 1L);
            expected.put(tsid + "/" + (baseTime + TimeValue.timeValueMinutes(5).millis()), 2L);
        }

        List<Page> chunked = new ArrayList<>();
        List<Page> single = new ArrayList<>();
        try {
            chunked.addAll(runPartialWindowPipeline(fiveMinBucket, Duration.ofMinutes(7), Duration.ofMinutes(2), rows, 3));
            assertThat("partial-channel result larger than the chunk size must be chunked", chunked.size(), greaterThan(1));
            for (Page page : chunked) {
                assertThat("no chunk exceeds targetChunkRows", page.getPositionCount(), lessThanOrEqualTo(3));
            }

            single.addAll(runPartialWindowPipeline(fiveMinBucket, Duration.ofMinutes(7), Duration.ofMinutes(2), rows, Integer.MAX_VALUE));
            assertThat("unchunked result is a single page", single.size(), equalTo(1));

            assertThat("chunked output has the expected merged values", toValueMap(chunked), equalTo(expected));
            assertThat("chunking preserves the partial-channel result", toValueMap(chunked), equalTo(toValueMap(single)));
        } finally {
            releasePages(chunked);
            releasePages(single);
        }
    }

    // --- helpers ---

    private List<Page> runPipeline(Rounding.Prepared timeBucket, Duration windowDuration, List<List<Object>> rows) {
        var aggregatorFactory = new WindowAggregatorFunctionSupplier(new SumIntAggregatorFunctionSupplier(), windowDuration)
            .groupingAggregatorFactory(AggregatorMode.SINGLE, List.of(HASH_CHANNEL_COUNT));
        return runPipeline(timeBucket, aggregatorFactory, List.of(ElementType.BYTES_REF, ElementType.LONG, ElementType.INT), rows);
    }

    /**
     * Produces unchunked partial (INITIAL) output and re-aggregates it in FINAL mode with the given
     * {@code finalTargetChunkRows}. This is the canonical two-stage path (data node -> coordinator), so it exercises
     * final-output chunking end to end.
     */
    private List<Page> runInitialThenFinal(int finalTargetChunkRows, List<List<Object>> rows) {
        List<Page> partial = runInitialPages(1_000_000, rows);
        Rounding.Prepared oneMinBucket = Rounding.builder(TimeValue.timeValueMinutes(1)).build().prepareForUnknown();
        var finalFactory = new TimeSeriesAggregationOperator.Factory(
            oneMinBucket,
            false,
            List.of(
                new BlockHash.GroupSpec(0, ElementType.BYTES_REF, null, null),
                new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
            ),
            AggregatorMode.FINAL,
            List.of(new SumIntAggregatorFunctionSupplier().groupingAggregatorFactory(AggregatorMode.FINAL, List.of(2, 3))),
            1024,
            finalTargetChunkRows
        );
        BlockFactory bf = blockFactory();
        var driverCtx = new DriverContext(bf.bigArrays(), bf, null);
        List<Page> collected = new ArrayList<>();
        boolean success = false;
        try (Operator op = finalFactory.get(driverCtx)) {
            while (partial.isEmpty() == false) {
                op.addInput(partial.remove(0));
            }
            op.finish();
            drainInto(op, collected);
            assertTrue("final operator should be finished once drained", op.isFinished());
            success = true;
            return collected;
        } finally {
            if (success == false) {
                releasePages(collected);
                releasePages(partial);
            }
        }
    }

    /**
     * Runs a windowed {@link AggregatorMode#SINGLE} time-series aggregation. Window-bucket expansion runs, letting
     * tests exercise expansion + {@code customizeSelected} remapping under output chunking.
     */
    private List<Page> runWindowedSingle(
        int targetChunkRows,
        Rounding.Prepared timeBucket,
        List<GroupingAggregator.Factory> aggregators,
        List<List<Object>> rows
    ) {
        var operatorFactory = new TimeSeriesAggregationOperator.Factory(
            timeBucket,
            false,
            List.of(
                new BlockHash.GroupSpec(0, ElementType.BYTES_REF, null, null),
                new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
            ),
            AggregatorMode.SINGLE,
            aggregators,
            10_000,
            targetChunkRows
        );
        BlockFactory bf = blockFactory();
        var driverCtx = new DriverContext(bf.bigArrays(), bf, null);
        var source = new ListRowsBlockSourceOperator(
            driverCtx.blockFactory(),
            List.of(ElementType.BYTES_REF, ElementType.LONG, ElementType.INT),
            rows
        );
        List<Page> results = new ArrayList<>();
        try (
            var driver = TestDriverFactory.create(
                driverCtx,
                source,
                List.of(operatorFactory.get(driverCtx)),
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            new TestDriverRunner().run(driver);
        }
        return results;
    }

    private List<GroupingAggregator.Factory> windowSumAndValues(Duration window) {
        return List.of(
            new WindowAggregatorFunctionSupplier(new SumIntAggregatorFunctionSupplier(), window).groupingAggregatorFactory(
                AggregatorMode.SINGLE,
                List.of(HASH_CHANNEL_COUNT)
            ),
            new ValuesBytesRefAggregatorFunctionSupplier().groupingAggregatorFactory(AggregatorMode.SINGLE, List.of(0))
        );
    }

    private List<GroupingAggregator.Factory> windowSumAndDimensionValues(Duration window) {
        return List.of(
            new WindowAggregatorFunctionSupplier(new SumIntAggregatorFunctionSupplier(), window).groupingAggregatorFactory(
                AggregatorMode.SINGLE,
                List.of(HASH_CHANNEL_COUNT)
            ),
            new DimensionValuesByteRefGroupingAggregatorFunction.FunctionSupplier().groupingAggregatorFactory(
                AggregatorMode.SINGLE,
                List.of(0)
            )
        );
    }

    /**
     * Sparse data (points at minute 2 and 8 for each of several tsids) with a 7m window so window expansion creates new
     * groups that must be remapped. The total (original + expanded) group count comfortably exceeds the chunk size used
     * by the window-expansion tests, forcing multi-page output.
     */
    private static List<List<Object>> sparseWindowRows() {
        Rounding.Prepared oneMinBucket = Rounding.builder(TimeValue.timeValueMinutes(1)).build().prepareForUnknown();
        long baseTime = oneMinBucket.round(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13"));
        List<List<Object>> rows = new ArrayList<>();
        for (String tsid : List.of("a", "b", "c", "d", "e", "f", "g", "h")) {
            rows.add(List.of(tsid, baseTime + TimeValue.timeValueMinutes(2).millis(), 1));
            rows.add(List.of(tsid, baseTime + TimeValue.timeValueMinutes(8).millis(), 1));
        }
        return rows;
    }

    private static Map<String, Long> toValueMap(List<Page> pages) {
        Map<String, Long> map = new HashMap<>();
        for (OutputRow row : extractRows(pages)) {
            map.put(row.tsid() + "/" + row.bucket(), row.value());
        }
        return map;
    }

    private static void releasePages(List<Page> pages) {
        for (Page page : pages) {
            page.releaseBlocks();
        }
    }

    private record WindowedRow(String tsid, long bucket, long sum, String dimension) {}

    private static List<WindowedRow> extractWindowedRows(List<Page> pages) {
        List<WindowedRow> out = new ArrayList<>();
        var scratch = new BytesRef();
        for (Page page : pages) {
            BytesRefBlock tsids = page.getBlock(0);
            LongBlock buckets = page.getBlock(1);
            LongBlock sums = page.getBlock(2);
            BytesRefBlock dims = page.getBlock(3);
            for (int p = 0; p < page.getPositionCount(); p++) {
                String tsid = tsids.getBytesRef(tsids.getFirstValueIndex(p), scratch).utf8ToString();
                String dim = dims.isNull(p) ? null : dims.getBytesRef(dims.getFirstValueIndex(p), scratch).utf8ToString();
                out.add(new WindowedRow(tsid, buckets.getLong(p), sums.getLong(p), dim));
            }
        }
        return out;
    }

    private static Map<String, WindowedRow> asMap(List<WindowedRow> rows) {
        Map<String, WindowedRow> map = new HashMap<>();
        for (WindowedRow row : rows) {
            map.put(row.tsid() + "/" + row.bucket(), row);
        }
        return map;
    }

    /**
     * Runs a SINGLE-mode pipeline for a non-multiple window: rows are {@code (tsid, bucket, value, timestamp)} and
     * the partial channel is filtered to the trailing {@code remainder} of each bucket via the raw timestamp column.
     */
    private List<Page> runPartialWindowPipeline(
        Rounding.Prepared timeBucket,
        Duration windowDuration,
        Duration remainder,
        List<List<Object>> rows
    ) {
        return runPartialWindowPipeline(timeBucket, windowDuration, remainder, rows, Integer.MAX_VALUE);
    }

    private List<Page> runPartialWindowPipeline(
        Rounding.Prepared timeBucket,
        Duration windowDuration,
        Duration remainder,
        List<List<Object>> rows,
        int targetChunkRows
    ) {
        return runPipeline(
            timeBucket,
            partialWindowAggregatorFactory(timeBucket, windowDuration, remainder),
            List.of(ElementType.BYTES_REF, ElementType.LONG, ElementType.INT, ElementType.LONG),
            rows,
            targetChunkRows
        );
    }

    private List<Page> runPipeline(
        Rounding.Prepared timeBucket,
        GroupingAggregator.Factory aggregatorFactory,
        List<ElementType> elementTypes,
        List<List<Object>> rows
    ) {
        return runPipeline(timeBucket, aggregatorFactory, elementTypes, rows, Integer.MAX_VALUE);
    }

    private List<Page> runPipeline(
        Rounding.Prepared timeBucket,
        GroupingAggregator.Factory aggregatorFactory,
        List<ElementType> elementTypes,
        List<List<Object>> rows,
        int targetChunkRows
    ) {
        var operatorFactory = new TimeSeriesAggregationOperator.Factory(
            timeBucket,
            false,
            List.of(
                new BlockHash.GroupSpec(0, ElementType.BYTES_REF, null, null),
                new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
            ),
            AggregatorMode.SINGLE,
            List.of(aggregatorFactory),
            10_000,
            targetChunkRows
        );

        BlockFactory bf = blockFactory();
        var driverCtx = new DriverContext(bf.bigArrays(), bf, null);
        var source = new ListRowsBlockSourceOperator(driverCtx.blockFactory(), elementTypes, rows);
        List<Page> results = new ArrayList<>();
        try (
            var driver = TestDriverFactory.create(
                driverCtx,
                source,
                List.of(operatorFactory.get(driverCtx)),
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            new TestDriverRunner().run(driver);
        }
        return results;
    }

    /**
     * Builds a SINGLE-mode aggregator factory for a non-multiple window {@code W = k * B + remainder}: a sum over
     * the value column (channel 2), with the partial channel filtered by {@link #trailingWindowFilter} over the raw
     * timestamp column (channel 3).
     */
    private static GroupingAggregator.Factory partialWindowAggregatorFactory(
        Rounding.Prepared timeBucket,
        Duration windowDuration,
        Duration remainder
    ) {
        var sum = new SumIntAggregatorFunctionSupplier();
        var partial = new org.elasticsearch.compute.aggregation.FilteredAggregatorFunctionSupplier(
            sum,
            trailingWindowFilter(timeBucket, remainder.toMillis(), 1, 3)
        );
        return new WindowAggregatorFunctionSupplier(sum, partial, windowDuration, remainder).groupingAggregatorFactory(
            AggregatorMode.SINGLE,
            List.of(HASH_CHANNEL_COUNT)
        );
    }

    /**
     * A stand-in for the {@code WindowFilter} row predicate, which lives in the esql module and is not available
     * here: keeps rows whose raw timestamp falls in the trailing {@code remainderMillis} of their bucket.
     */
    private static ExpressionEvaluator.Factory trailingWindowFilter(
        Rounding.Prepared timeBucket,
        long remainderMillis,
        int bucketChannel,
        int timestampChannel
    ) {
        return context -> new ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                LongBlock bucketBlock = page.getBlock(bucketChannel);
                LongBlock timestampBlock = page.getBlock(timestampChannel);
                try (var builder = context.blockFactory().newBooleanVectorFixedBuilder(page.getPositionCount())) {
                    for (int p = 0; p < page.getPositionCount(); p++) {
                        long bucketEnd = timeBucket.nextRoundingValue(bucketBlock.getLong(p));
                        builder.appendBoolean(p, timestampBlock.getLong(p) >= bucketEnd - remainderMillis);
                    }
                    return builder.build().asBlock();
                }
            }

            @Override
            public long baseRamBytesUsed() {
                return 0;
            }

            @Override
            public void close() {}
        };
    }

    private record OutputRow(String tsid, long bucket, long value) {}

    private static List<OutputRow> extractRows(List<Page> results) {
        List<OutputRow> outputRows = new ArrayList<>();
        for (Page page : results) {
            BytesRefBlock tsids = page.getBlock(0);
            LongBlock buckets = page.getBlock(1);
            LongBlock values = page.getBlock(2);
            var scratch = new BytesRef();
            for (int p = 0; p < page.getPositionCount(); p++) {
                outputRows.add(new OutputRow(tsids.getBytesRef(p, scratch).utf8ToString(), buckets.getLong(p), values.getLong(p)));
            }
        }
        return outputRows;
    }

    private List<Page> runInitialPages(int targetChunkRows, List<List<Object>> rows) {
        Rounding.Prepared oneMinBucket = Rounding.builder(TimeValue.timeValueMinutes(1)).build().prepareForUnknown();
        var operatorFactory = new TimeSeriesAggregationOperator.Factory(
            oneMinBucket,
            false,
            List.of(
                new BlockHash.GroupSpec(0, ElementType.BYTES_REF, null, null),
                new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
            ),
            AggregatorMode.INITIAL,
            List.of(new SumIntAggregatorFunctionSupplier().groupingAggregatorFactory(AggregatorMode.INITIAL, List.of(2))),
            1024,
            targetChunkRows
        );

        BlockFactory bf = blockFactory();
        var driverCtx = new DriverContext(bf.bigArrays(), bf, null);
        List<Page> collected = new ArrayList<>();
        boolean success = false;
        try (Operator op = operatorFactory.get(driverCtx)) {
            op.addInput(buildPage(bf, rows));
            op.finish();
            drainInto(op, collected);
            assertTrue("initial operator should be finished once drained", op.isFinished());
            success = true;
            return collected;
        } finally {
            if (success == false) {
                for (Page page : collected) {
                    page.releaseBlocks();
                }
            }
        }
    }

    /**
     * Drives a {@link TimeSeriesAggregationOperator} in {@link AggregatorMode#INITIAL} (partial output) so that output
     * chunking is active: the single emitted result is sliced into pages of about {@code targetChunkRows}. Each element
     * of {@code inputBatches} becomes one input page (one {@code addInput} call), letting tests control batching, and
     * the operator's output is summarized into {@link PageSummary} so the (tracked) output pages can be released before
     * assertions run.
     */
    private List<PageSummary> runPartialMode(int targetChunkRows, List<List<List<Object>>> inputBatches) {
        Rounding.Prepared oneMinBucket = Rounding.builder(TimeValue.timeValueMinutes(1)).build().prepareForUnknown();
        var operatorFactory = new TimeSeriesAggregationOperator.Factory(
            oneMinBucket,
            false,
            List.of(
                new BlockHash.GroupSpec(0, ElementType.BYTES_REF, null, null),
                new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
            ),
            AggregatorMode.INITIAL,
            List.of(new SumIntAggregatorFunctionSupplier().groupingAggregatorFactory(AggregatorMode.INITIAL, List.of(2))),
            1024,
            targetChunkRows
        );

        BlockFactory bf = blockFactory();
        var driverCtx = new DriverContext(bf.bigArrays(), bf, null);
        List<Page> collected = new ArrayList<>();
        try (Operator op = operatorFactory.get(driverCtx)) {
            for (List<List<Object>> batch : inputBatches) {
                assertTrue("operator should accept input before each batch", op.needsInput());
                op.addInput(buildPage(bf, batch));
                drainInto(op, collected);
            }
            op.finish();
            drainInto(op, collected);
            assertTrue("operator should be finished once drained", op.isFinished());
            return summarize(collected);
        } finally {
            for (Page page : collected) {
                page.releaseBlocks();
            }
        }
    }

    private static void drainInto(Operator op, List<Page> collected) {
        Page output;
        while ((output = op.getOutput()) != null) {
            collected.add(output);
        }
    }

    /**
     * Builds one input page of {@code (tsid, timestamp, value)} rows. Callers must feed rows already sorted by
     * {@code (tsid, timestamp)} because {@link TimeSeriesAggregationOperator} assumes time-series-sorted input.
     */
    private static Page buildPage(BlockFactory bf, List<List<Object>> rows) {
        int positions = rows.size();
        try (
            var tsids = bf.newBytesRefVectorBuilder(positions);
            var timestamps = bf.newLongVectorBuilder(positions);
            var values = bf.newIntVectorBuilder(positions)
        ) {
            for (List<Object> row : rows) {
                tsids.appendBytesRef(new BytesRef((String) row.get(0)));
                timestamps.appendLong(((Number) row.get(1)).longValue());
                values.appendInt(((Number) row.get(2)).intValue());
            }
            return new Page(tsids.build().asBlock(), timestamps.build().asBlock(), values.build().asBlock());
        }
    }

    /**
     * Appends {@code groupCount} rows for {@code tsid}, each with a distinct timestamp, so the tsid contributes
     * exactly {@code groupCount} groups (output rows) to the partial aggregation.
     */
    private static void addTsidRows(List<List<Object>> rows, String tsid, int groupCount) {
        for (int i = 0; i < groupCount; i++) {
            rows.add(List.of(tsid, (long) i, 1));
        }
    }

    /**
     * A view over one partial-output page: its row count and the per-row {@code _tsid} (key block 0), captured so
     * assertions can run after the underlying page has been released.
     */
    private record PageSummary(int positionCount, List<String> tsids) {}

    private static List<PageSummary> summarize(List<Page> pages) {
        List<PageSummary> summaries = new ArrayList<>(pages.size());
        BytesRef scratch = new BytesRef();
        for (Page page : pages) {
            BytesRefBlock tsidBlock = page.getBlock(0);
            List<String> tsids = new ArrayList<>(page.getPositionCount());
            for (int p = 0; p < page.getPositionCount(); p++) {
                tsids.add(tsidBlock.getBytesRef(tsidBlock.getFirstValueIndex(p), scratch).utf8ToString());
            }
            summaries.add(new PageSummary(page.getPositionCount(), tsids));
        }
        return summaries;
    }

}
