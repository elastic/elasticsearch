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
import java.util.List;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

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
        // TSID "a": each point has value 1
        assertThat(outputRows.get(0).value(), equalTo(7L));  // [0,7m) → 7 points
        assertThat(outputRows.get(1).value(), equalTo(7L));  // [5m,12m) → 7 points
        assertThat(outputRows.get(2).value(), equalTo(5L));  // [10m,17m) → 5 points
        // TSID "b": each point has value 10
        assertThat(outputRows.get(3).value(), equalTo(70L));
        assertThat(outputRows.get(4).value(), equalTo(70L));
        assertThat(outputRows.get(5).value(), equalTo(50L));
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
        for (int i = 0; i < 10; i++) {
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
            fiveMinBucket,
            false
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
     * With 7m window, 5m output bucket, 1m internal bucket, and data only at minutes 2 and 8:
     * - Output group at 00:00 is an expanded group (no direct data) — VALUES must still resolve
     * - Output group at 05:00 is an expanded group (no direct data) — VALUES must still resolve
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
            fiveMinBucket,
            false
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
        // Output-aligned groups: 00:00 and 05:00
        assertThat(outputRows.size(), equalTo(2));
        outputRows.sort(Comparator.comparingLong(OutputRow::bucket));
        // Window [00:00, 07:00) contains minute 2 (val=5)
        assertThat(outputRows.get(0).value(), equalTo(5L));
        // Window [05:00, 12:00) contains minute 8 (val=10)
        assertThat(outputRows.get(1).value(), equalTo(10L));
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

        // 4m window, 3m output bucket, 1m internal bucket → GCD = 1m sub-buckets, output every 3m
        List<Page> results = runPipeline(oneMinBucket, threeMinBucket, windowDuration, rows);

        List<OutputRow> outputRows = extractRows(results);
        assertThat(outputRows.size(), equalTo(3)); // 0, 3m, 6m
        for (OutputRow row : outputRows) {
            assertThat(threeMinBucket.round(row.bucket()), equalTo(row.bucket()));
        }
        outputRows.sort(Comparator.comparingLong(OutputRow::bucket));
        // [0,4m) → minutes 0..3 → 4 points × 2 = 8
        assertThat(outputRows.get(0).value(), equalTo(8L));
        // [3m,7m) → minutes 3..6 → 4 points × 2 = 8
        assertThat(outputRows.get(1).value(), equalTo(8L));
        // [6m,10m) → minutes 6..8 → 3 points × 2 = 6
        assertThat(outputRows.get(2).value(), equalTo(6L));
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
            outputBucket,
            false
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
}
