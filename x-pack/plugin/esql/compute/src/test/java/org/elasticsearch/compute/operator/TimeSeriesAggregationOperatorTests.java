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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    // All of these run the operator in a partial output mode (AggregatorMode.INITIAL) where periodic partial
    // emission and tsid-aligned output chunking are enabled. See runPartialMode() for the harness.

    /**
     * Partial-mode driver with a small {@code partialEmitKeysThreshold} and a uniqueness ratio above the gate
     * should emit more than one output page (periodic partial emission fires).
     */
    public void testEmitsMultiplePagesInPartialModeWhenKeysExceedThreshold() {
        List<List<Object>> firstBatch = new ArrayList<>();
        for (String tsid : List.of("a", "b", "c", "d", "e")) {
            addTsidRows(firstBatch, tsid, 1);
        }
        List<List<Object>> secondBatch = new ArrayList<>();
        for (String tsid : List.of("f", "g", "h", "i", "j")) {
            addTsidRows(secondBatch, tsid, 1);
        }
        // Each 5-key batch clears the key-count threshold and the uniqueness gate, so periodic partial emission
        // fires once per batch. targetChunkRows is effectively disabled, so each emit cycle is a single page.
        List<PageSummary> pages = runPartialMode(4, 0.1, Integer.MAX_VALUE, Integer.MAX_VALUE, List.of(firstBatch, secondBatch));
        assertThat(pages.size(), equalTo(2));
    }

    /**
     * Low-cardinality partial-mode driver under default thresholds should emit exactly one page, taking today's
     * unchunked code path (Goal 2 — no perf regression when data fits).
     */
    public void testEmitsSinglePageInPartialModeWhenKeysBelowThreshold() {
        List<List<Object>> batch = new ArrayList<>();
        for (String tsid : List.of("a", "b", "c")) {
            addTsidRows(batch, tsid, 1);
        }
        // Three low-cardinality batches with overlapping keys: numKeys (3) never reaches the high threshold, so
        // periodic emission never fires and the operator takes the single-page path at finish().
        List<PageSummary> pages = runPartialMode(100_000, 0.1, Integer.MAX_VALUE, Integer.MAX_VALUE, List.of(batch, batch, batch));
        assertThat(pages.size(), equalTo(1));
        assertThat(pages.get(0).positionCount(), equalTo(3));
    }

    /**
     * Within a single emit cycle, no {@code _tsid}'s groups may appear in more than one chunk: a {@code _tsid}
     * that started inside a chunk is emitted whole, even when that overshoots {@code targetChunkRows}.
     */
    public void testSliceDoesNotSplitTsidWithinEmitCycle() {
        List<List<Object>> rows = new ArrayList<>();
        addTsidRows(rows, "a", 3);
        addTsidRows(rows, "b", 4);
        addTsidRows(rows, "c", 7);
        addTsidRows(rows, "d", 2);
        addTsidRows(rows, "e", 6);
        // High threshold => no periodic emit => a single emit cycle at finish(); targetChunkRows 5 forces several chunks.
        List<PageSummary> pages = runPartialMode(Integer.MAX_VALUE, 1.0, 5, 10_000, List.of(rows));
        assertThat("targetChunkRows should split the cycle into several chunks", pages.size(), greaterThan(1));
        Map<String, Integer> chunksPerTsid = new HashMap<>();
        for (PageSummary page : pages) {
            for (String tsid : page.distinctTsids()) {
                chunksPerTsid.merge(tsid, 1, Integer::sum);
            }
        }
        for (var entry : chunksPerTsid.entrySet()) {
            assertThat("tsid [" + entry.getKey() + "] must not be split across chunks", entry.getValue(), equalTo(1));
        }
    }

    /**
     * When the running row count crosses {@code targetChunkRows} mid-{@code _tsid}, the chunk extends to the next
     * {@code _tsid} boundary (the {@code 10000 -> 10100} example).
     */
    public void testSliceExceedsTargetChunkRowsToCompleteTsid() {
        int targetChunkRows = 5;
        List<List<Object>> rows = new ArrayList<>();
        addTsidRows(rows, "a", 3);
        addTsidRows(rows, "b", 4); // the running count crosses targetChunkRows part-way through "b"
        addTsidRows(rows, "c", 5);
        List<PageSummary> pages = runPartialMode(Integer.MAX_VALUE, 1.0, targetChunkRows, 10_000, List.of(rows));
        PageSummary first = pages.get(0);
        assertThat(
            "first chunk overshoots targetChunkRows to finish the straddling tsid",
            first.positionCount(),
            greaterThan(targetChunkRows)
        );
        long bInFirstChunk = first.tsids().stream().filter("b"::equals).count();
        assertThat("tsid b is emitted whole inside the overshooting chunk", bInFirstChunk, equalTo(4L));
        for (int i = 1; i < pages.size(); i++) {
            assertThat("tsid b must not leak into a later chunk", pages.get(i).tsids().contains("b"), equalTo(false));
        }
    }

    /**
     * Many small {@code _tsid}s whose combined size is below {@code targetChunkRows} should share a single chunk
     * (multi-tsid pages are allowed and expected).
     */
    public void testSlicePacksSmallTsidsTogether() {
        List<List<Object>> rows = new ArrayList<>();
        for (String tsid : List.of("a", "b", "c", "d", "e", "f")) {
            addTsidRows(rows, tsid, 2);
        }
        // targetChunkRows 10 leaves room for several 2-group tsids in one chunk before cutting at a boundary.
        List<PageSummary> pages = runPartialMode(Integer.MAX_VALUE, 1.0, 10, 10_000, List.of(rows));
        boolean someChunkPacksMultipleTsids = pages.stream().anyMatch(page -> page.distinctTsids().size() > 1);
        assertThat("small tsids should share a chunk", someChunkPacksMultipleTsids, equalTo(true));
    }

    /**
     * A single pathological {@code _tsid} with more groups than {@code maxChunkRows} is split across pages, all
     * carrying that same leading {@code _tsid}; this is the only case that splits a {@code _tsid}.
     */
    public void testMaxChunkRowsSplitsPathologicalSingleTsid() {
        int maxChunkRows = 10;
        List<List<Object>> rows = new ArrayList<>();
        addTsidRows(rows, "x", 25); // one tsid with far more groups than maxChunkRows
        List<PageSummary> pages = runPartialMode(Integer.MAX_VALUE, 1.0, 5, maxChunkRows, List.of(rows));
        assertThat("maxChunkRows must split the oversized tsid", pages.size(), greaterThan(1));
        for (PageSummary page : pages) {
            assertThat("each chunk carries only the single tsid", page.distinctTsids(), equalTo(Set.of("x")));
            assertThat("no chunk exceeds maxChunkRows", page.positionCount(), lessThanOrEqualTo(maxChunkRows));
        }
        assertThat("non-final chunks are filled up to maxChunkRows", pages.get(0).positionCount(), equalTo(maxChunkRows));
    }

    /**
     * The target chunk size is now a direct setting (no longer derived), while {@code maxChunkRows} is a fixed
     * multiple of that target, giving a hard per-page ceiling well above it. The default target reproduces the
     * historical 200,000-row ceiling.
     */
    public void testFactoryDerivesMaxChunkRowsFromTargetChunkSize() {
        assertThat(TimeSeriesAggregationOperator.DEFAULT_TARGET_CHUNK_SIZE, equalTo(100_000));
        assertThat(
            TimeSeriesAggregationOperator.Factory.maxChunkRowsFor(TimeSeriesAggregationOperator.DEFAULT_TARGET_CHUNK_SIZE),
            equalTo(200_000)
        );
        assertThat(TimeSeriesAggregationOperator.Factory.maxChunkRowsFor(1), equalTo(2));
        // maxChunkRows guards against int overflow at the extreme.
        assertThat(TimeSeriesAggregationOperator.Factory.maxChunkRowsFor(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));
    }

    /**
     * After a periodic partial emit, the next {@code addInput} rebuilds the {@link BlockHash} from the supplier
     * (still a {@code TimeSeriesBlockHash}) so accumulation restarts cleanly.
     */
    public void testBlockHashRebuiltAfterPeriodicEmit() {
        List<List<Object>> firstBatch = new ArrayList<>();
        addTsidRows(firstBatch, "a", 1);
        addTsidRows(firstBatch, "b", 1);
        List<List<Object>> secondBatch = new ArrayList<>();
        addTsidRows(secondBatch, "c", 1);
        addTsidRows(secondBatch, "d", 1);
        // threshold 2 => each batch triggers a periodic emit. If the blockHash were not rebuilt between cycles,
        // the second emit would still carry a and b alongside c and d.
        List<PageSummary> pages = runPartialMode(2, 0.5, Integer.MAX_VALUE, Integer.MAX_VALUE, List.of(firstBatch, secondBatch));
        assertThat(pages.size(), equalTo(2));
        assertThat(pages.get(0).distinctTsids(), equalTo(Set.of("a", "b")));
        assertThat("second cycle only contains freshly accumulated keys", pages.get(1).distinctTsids(), equalTo(Set.of("c", "d")));
    }

    /**
     * Hot-spotted keys (low uniqueness ratio) must not trigger periodic emission even past the key-count
     * threshold, matching the parent's uniqueness gate.
     */
    public void testUniquenessThresholdGatesPeriodicEmit() {
        // 3 distinct keys but many rows per batch: numKeys (3) >= threshold (2), yet rowsAdded * uniqueness >
        // numKeys, so the uniqueness gate blocks periodic emission across every batch and only finish() emits.
        List<List<Object>> hotBatch = new ArrayList<>();
        for (String tsid : List.of("a", "b", "c")) {
            for (int i = 0; i < 30; i++) {
                hotBatch.add(List.of(tsid, 0L, 1));
            }
        }
        List<PageSummary> pages = runPartialMode(2, 0.5, Integer.MAX_VALUE, Integer.MAX_VALUE, List.of(hotBatch, hotBatch, hotBatch));
        assertThat("hot-spotted keys must not trigger periodic emission", pages.size(), equalTo(1));
        assertThat(pages.get(0).positionCount(), equalTo(3));
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

    /**
     * Drives a {@link TimeSeriesAggregationOperator} in {@link AggregatorMode#INITIAL} (partial output) so that
     * periodic partial emission and tsid-aligned output chunking are active. Each element of {@code inputBatches}
     * becomes one input page (one {@code addInput} call), letting tests control batching, and the operator's output
     * is summarized into {@link PageSummary} so the (tracked) output pages can be released before assertions run.
     */
    private List<PageSummary> runPartialMode(
        int partialEmitKeysThreshold,
        double partialEmitUniquenessThreshold,
        int targetChunkRows,
        int maxChunkRows,
        List<List<List<Object>>> inputBatches
    ) {
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
            partialEmitKeysThreshold,
            partialEmitUniquenessThreshold,
            targetChunkRows,
            maxChunkRows
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
    private record PageSummary(int positionCount, List<String> tsids) {
        Set<String> distinctTsids() {
            return new LinkedHashSet<>(tsids);
        }
    }

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
