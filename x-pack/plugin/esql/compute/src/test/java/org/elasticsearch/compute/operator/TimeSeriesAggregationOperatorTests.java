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
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
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
     * Multiple TSIDs with non-multiple window/bucket (7m window, 5m output bucket, 1m internal bucket).
     * Verifies the optimized emit path produces only output-aligned rows with correct windowed sums.
     */
    public void testEmitFiltersToOutputAlignedGroupsBeforeEvaluation() {
        Rounding.Prepared oneMinBucket = Rounding.builder(TimeValue.timeValueMinutes(1)).build().prepareForUnknown();
        Rounding.Prepared fiveMinBucket = Rounding.builder(TimeValue.timeValueMinutes(5)).build().prepareForUnknown();
        Duration windowDuration = Duration.ofMinutes(7);

        final long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13");
        long baseTime = oneMinBucket.round(startTime);

        List<List<Object>> rows = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            long ts = baseTime + TimeValue.timeValueMinutes(i).millis();
            rows.add(List.of("a", ts, 1));
            rows.add(List.of("b", ts, 10));
        }

        List<Page> results = runPipeline(oneMinBucket, fiveMinBucket, windowDuration, rows);

        List<OutputRow> outputRows = extractRows(results);

        for (OutputRow row : outputRows) {
            assertThat(
                "every output timestamp must be aligned to the 5-minute boundary",
                fiveMinBucket.round(row.bucket()),
                equalTo(row.bucket())
            );
        }
        // 2 TSIDs × 3 output buckets (0, 5m, 10m)
        assertThat(outputRows.size(), equalTo(6));

        outputRows.sort(Comparator.comparing(OutputRow::tsid).thenComparingLong(OutputRow::bucket));
        // TSID "a": each point has value 1.
        // With no leading trim, early buckets contain partial windows.
        assertThat(outputRows.get(0).value(), equalTo(1L));  // [0,1m) -> 1 point
        assertThat(outputRows.get(1).value(), equalTo(6L));  // [0,6m) -> 6 points
        assertThat(outputRows.get(2).value(), equalTo(7L));  // [4m,11m) -> 7 points
        // TSID "b": each point has value 10.
        assertThat(outputRows.get(3).value(), equalTo(10L));
        assertThat(outputRows.get(4).value(), equalTo(60L));
        assertThat(outputRows.get(5).value(), equalTo(70L));
    }

    /**
     * When internal bucket == output bucket, all groups are naturally aligned.
     * {@code computeOutputAlignedPositions} returns null, and the standard super.emit() path is used.
     */
    public void testEmitFallsBackToSuperWhenAllGroupsAligned() {
        Rounding.Prepared fiveMinBucket = Rounding.builder(TimeValue.timeValueMinutes(5)).build().prepareForUnknown();
        Duration windowDuration = Duration.ofMinutes(10);

        final long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13");
        long baseTime = fiveMinBucket.round(startTime);

        List<List<Object>> rows = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            long ts = baseTime + TimeValue.timeValueMinutes(i * 5L).millis();
            rows.add(List.of("x", ts, 3));
        }

        // internal bucket == output bucket → no sub-bucketing, all groups aligned
        List<Page> results = runPipeline(fiveMinBucket, fiveMinBucket, windowDuration, rows);

        List<OutputRow> outputRows = extractRows(results);
        assertThat(outputRows.size(), greaterThan(0));
        for (OutputRow row : outputRows) {
            assertThat(fiveMinBucket.round(row.bucket()), equalTo(row.bucket()));
        }
    }

    /**
     * Verifies that a VALUES-like aggregator combined with a window aggregator produces correct
     * results through the optimized emit path where expanded groups are mapped via originalNumGroups.
     * The DimensionValuesByteRefGroupingAggregatorFunction reads dimension values for each group;
     * expanded (window-filled) groups must be mapped to their source group's values.
     */
    public void testSelectedForValuesAggregatorMapsExpandedGroupsViaOriginalNumGroups() {
        Rounding.Prepared oneMinBucket = Rounding.builder(TimeValue.timeValueMinutes(1)).build().prepareForUnknown();
        Rounding.Prepared fiveMinBucket = Rounding.builder(TimeValue.timeValueMinutes(5)).build().prepareForUnknown();
        Duration windowDuration = Duration.ofMinutes(7);

        final long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13");
        long baseTime = oneMinBucket.round(startTime);

        List<List<Object>> rows = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            long ts = baseTime + TimeValue.timeValueMinutes(i).millis();
            rows.add(List.of("tsid1", ts, 5));
        }

        // Use both a window aggregator (sum) and a values aggregator (dimension values on tsid column)
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
            oneMinBucket,
            false,
            List.of(
                new BlockHash.GroupSpec(0, ElementType.BYTES_REF, null, null),
                new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
            ),
            AggregatorMode.SINGLE,
            aggregatorFactories,
            10_000,
            fiveMinBucket
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
        assertThat("should produce output rows", results.isEmpty(), equalTo(false));
        for (Page page : results) {
            // block 0: tsid key, block 1: timestamp key, block 2: windowed sum, block 3: values(tsid)
            assertThat("expected 4 blocks (2 keys + 2 agg results)", page.getBlockCount(), equalTo(4));
            LongBlock buckets = page.getBlock(1);
            BytesRefBlock valuesBlock = page.getBlock(3);
            var scratch = new BytesRef();
            for (int p = 0; p < page.getPositionCount(); p++) {
                assertThat(fiveMinBucket.round(buckets.getLong(p)), equalTo(buckets.getLong(p)));
                // The values aggregator must produce the tsid value for every output row,
                // including rows whose group was created by expandWindowBuckets
                assertFalse("values block must not be null at position " + p, valuesBlock.isNull(p));
                BytesRef val = valuesBlock.getBytesRef(valuesBlock.getFirstValueIndex(p), scratch);
                assertThat(val, equalTo(expectedTsid));
            }
        }
    }

    /**
     * Sparse data where output-aligned sub-buckets have no direct data points.
     * With a backward 7m window, 5m output bucket, 1m internal bucket, and sparse data:
     * - Output group at 10m is an expanded group (no direct data) and must keep VALUES output
     * This exercises the path where expandWindowBuckets creates groups that become output-aligned,
     * and selectedForValuesAggregator must remap them to the original group that has dimension data.
     */
    public void testValuesAggregatorWithSparseDataAndNonMultipleWindow() {
        Rounding.Prepared oneMinBucket = Rounding.builder(TimeValue.timeValueMinutes(1)).build().prepareForUnknown();
        Rounding.Prepared fiveMinBucket = Rounding.builder(TimeValue.timeValueMinutes(5)).build().prepareForUnknown();
        Duration windowDuration = Duration.ofMinutes(7);

        final long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13");
        long baseTime = oneMinBucket.round(startTime);

        List<List<Object>> rows = new ArrayList<>();
        // Only two data points: minute 2 and minute 8
        rows.add(List.of("s1", baseTime + TimeValue.timeValueMinutes(2).millis(), 5));
        rows.add(List.of("s1", baseTime + TimeValue.timeValueMinutes(8).millis(), 10));

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
            oneMinBucket,
            false,
            List.of(
                new BlockHash.GroupSpec(0, ElementType.BYTES_REF, null, null),
                new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
            ),
            AggregatorMode.SINGLE,
            aggregatorFactories,
            10_000,
            fiveMinBucket
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

        BytesRef expectedTsid = new BytesRef("s1");
        assertThat("should produce output rows", results.isEmpty(), equalTo(false));
        List<OutputRow> outputRows = new ArrayList<>();
        for (Page page : results) {
            // block 0: tsid key, block 1: timestamp key, block 2: windowed sum, block 3: values(tsid)
            assertThat("expected 4 blocks (2 keys + 2 agg results)", page.getBlockCount(), equalTo(4));
            BytesRefBlock tsids = page.getBlock(0);
            LongBlock buckets = page.getBlock(1);
            LongBlock sums = page.getBlock(2);
            BytesRefBlock valuesBlock = page.getBlock(3);
            var scratch = new BytesRef();
            for (int p = 0; p < page.getPositionCount(); p++) {
                long bucket = buckets.getLong(p);
                assertThat("output must be aligned to 5m", fiveMinBucket.round(bucket), equalTo(bucket));
                assertFalse("values block must not be null at position " + p, valuesBlock.isNull(p));
                BytesRef val = valuesBlock.getBytesRef(valuesBlock.getFirstValueIndex(p), scratch);
                assertThat("dimension value must be present for expanded output-aligned group", val, equalTo(expectedTsid));
                outputRows.add(new OutputRow(tsids.getBytesRef(p, scratch).utf8ToString(), bucket, sums.getLong(p)));
            }
        }
        // With no leading trim and range-guarding to [2m,8m], only 05:00 remains output-aligned.
        assertThat(outputRows.size(), equalTo(1));
        outputRows.sort(Comparator.comparingLong(OutputRow::bucket));
        // Window [−1m,6m) intersects available data at minute 2 only.
        assertThat(outputRows.get(0).value(), equalTo(5L));
    }

    /**
     * Verifies that the evaluation context resolves timestamps correctly when the keys blocks are
     * filtered (positions no longer match group IDs). This exercises the tsBlockHash-based lookup
     * introduced by the optimization, through a full pipeline with sub-bucketing.
     */
    public void testEvaluationContextUsesBlockHashDirectlyForTimestampLookups() {
        Rounding.Prepared oneMinBucket = Rounding.builder(TimeValue.timeValueMinutes(1)).build().prepareForUnknown();
        Rounding.Prepared threeMinBucket = Rounding.builder(TimeValue.timeValueMinutes(3)).build().prepareForUnknown();
        Duration windowDuration = Duration.ofMinutes(4);

        final long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13");
        long baseTime = oneMinBucket.round(startTime);

        List<List<Object>> rows = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            long ts = baseTime + TimeValue.timeValueMinutes(i).millis();
            rows.add(List.of("z", ts, 2));
        }

        // 4m backward window, 3m output bucket, 1m internal bucket.
        List<Page> results = runPipeline(oneMinBucket, threeMinBucket, windowDuration, rows);

        List<OutputRow> outputRows = extractRows(results);
        assertThat(outputRows.size(), equalTo(3)); // 0, 3m, 6m
        for (OutputRow row : outputRows) {
            assertThat(threeMinBucket.round(row.bucket()), equalTo(row.bucket()));
        }
        outputRows.sort(Comparator.comparingLong(OutputRow::bucket));
        // [0,1m) -> minute 0 -> 1 point × 2 = 2
        assertThat(outputRows.get(0).value(), equalTo(2L));
        // [0,4m) -> minutes 0..3 -> 4 points × 2 = 8
        assertThat(outputRows.get(1).value(), equalTo(8L));
        // [3m,7m) -> minutes 3..6 -> 4 points × 2 = 8
        assertThat(outputRows.get(2).value(), equalTo(8L));
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
            null,
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
            null,
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
                null,
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
                null,
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

    // --- helpers ---

    private List<Page> runPipeline(
        Rounding.Prepared internalBucket,
        Rounding.Prepared outputBucket,
        Duration windowDuration,
        List<List<Object>> rows
    ) {
        var operatorFactory = new TimeSeriesAggregationOperator.Factory(
            internalBucket,
            false,
            List.of(
                new BlockHash.GroupSpec(0, ElementType.BYTES_REF, null, null),
                new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
            ),
            AggregatorMode.SINGLE,
            List.of(
                new WindowAggregatorFunctionSupplier(new SumIntAggregatorFunctionSupplier(), windowDuration).groupingAggregatorFactory(
                    AggregatorMode.SINGLE,
                    List.of(HASH_CHANNEL_COUNT)
                )
            ),
            10_000,
            outputBucket
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
            null,
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
            null,
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
