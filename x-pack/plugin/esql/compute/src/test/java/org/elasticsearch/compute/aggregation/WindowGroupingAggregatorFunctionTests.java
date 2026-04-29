/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.ForkingOperatorTestCase;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.TimeSeriesAggregationOperator;
import org.elasticsearch.compute.test.operator.blocksource.ListRowsBlockSourceOperator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.hamcrest.Matcher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.util.stream.IntStream.range;
import static org.hamcrest.Matchers.equalTo;

// TODO: Make this a base class for time-series aggregation grouping tests
public class WindowGroupingAggregatorFunctionTests extends ForkingOperatorTestCase {
    static final Rounding.Prepared timeBucket = Rounding.builder(TimeValue.timeValueMinutes(1)).build().prepareForUnknown();
    static final int HASH_CHANNEL_COUNT = 2;

    @Override
    protected Operator.OperatorFactory simpleWithMode(SimpleOptions options, AggregatorMode mode) {
        return new TimeSeriesAggregationOperator.Factory(
            timeBucket,
            false,
            List.of(
                new BlockHash.GroupSpec(0, ElementType.BYTES_REF, null, null),
                new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
            ),
            mode,
            List.of(aggregatorFunction().groupingAggregatorFactory(mode, channels(mode))),
            Integer.MAX_VALUE,  // TODO window functions don't support chunking https://github.com/elastic/elasticsearch/issues/138705
            null,
            false
        );
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        final long START_TIME = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13");
        List<BytesRef> groups = List.of(new BytesRef("a"), new BytesRef("b"), new BytesRef("c"), new BytesRef("d"));
        size = 2;
        List<List<Object>> rows = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            long tsOffset = randomLongBetween(0, 20 * 60 * 1000);
            long ts = timeBucket.round(START_TIME + tsOffset);
            int value = randomIntBetween(1, 1000);
            BytesRef tsid = randomFrom(groups);
            rows.add(List.of(tsid, ts, value));
        }
        return new ListRowsBlockSourceOperator(blockFactory, List.of(ElementType.BYTES_REF, ElementType.LONG, ElementType.INT), rows);
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        record Key(String tsid, long bucket) {
            @Override
            public String toString() {
                return tsid + bucket;
            }

        }
        Map<Key, Long> expected = new TreeMap<>(Comparator.comparing(Key::tsid).thenComparingLong(Key::bucket));
        // original groups
        long oneMinute = TimeValue.timeValueMinutes(1).millis();
        long smallestBucket = Long.MAX_VALUE;
        for (Page page : input) {
            LongBlock timestamp = page.getBlock(1);
            for (int p = 0; p < timestamp.getPositionCount(); p++) {
                smallestBucket = Math.min(timestamp.getLong(p), smallestBucket);
            }
        }
        for (Page page : input) {
            BytesRefBlock tsids = page.getBlock(0);
            LongBlock timestamp = page.getBlock(1);
            IntBlock values = page.getBlock(2);
            var scratch = new BytesRef();
            for (int p = 0; p < page.getPositionCount(); p++) {
                long bucket = timestamp.getLong(p);
                var tsid = tsids.getBytesRef(p, scratch).utf8ToString();
                // slide the window over the last 5 minutes
                // bucket = 00:06 -> it should generate buckets at 00:02, 00:03, 00:04, 00:05, 00:06
                for (int i = 0; i < 5; i++) {
                    if (bucket >= smallestBucket) {
                        Key key = new Key(tsid, bucket);
                        long val = values.getInt(p);
                        expected.merge(key, val, Long::sum);
                    }
                    bucket = bucket - oneMinute;
                }
            }
        }
        Map<Key, Long> actual = new TreeMap<>(Comparator.comparing(Key::tsid).thenComparingLong(Key::bucket));
        for (Page page : results) {
            var scratch = new BytesRef();
            BytesRefBlock tsids = page.getBlock(0);
            LongBlock buckets = page.getBlock(1);
            LongBlock values = page.getBlock(2);
            for (int p = 0; p < buckets.getPositionCount(); p++) {
                var tsid = tsids.getBytesRef(p, scratch).utf8ToString();
                Key key = new Key(tsid, buckets.getLong(p));
                long val = values.getLong(p);
                actual.put(key, val);
            }
        }
        assertThat(actual, equalTo(expected));
    }

    @Override
    protected final Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("TimeSeriesAggregationOperator[mode = <not-needed>, aggs = " + expectedDescriptionOfAggregator() + "]");
    }

    @Override
    protected final Matcher<String> expectedToStringOfSimple() {
        String hash = "blockHash=BytesRefLongBlockHash{keys=[tsid[channel=0], timestamp[channel=1]], entries=0, size=%size%}".replace(
            "%size%",
            byteRefBlockHashSize()
        );
        return equalTo(
            "TimeSeriesAggregationOperator["
                + hash
                + ", aggregators=[GroupingAggregator[aggregatorFunction="
                + expectedToStringOfSimpleAggregator()
                + ", mode=SINGLE]]]"
        );
    }

    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new WindowAggregatorFunctionSupplier(new SumIntAggregatorFunctionSupplier(), Duration.ofMinutes(5));
    }

    protected String expectedToStringOfSimpleAggregator() {
        return "Window[agg=SumIntGroupingAggregatorFunction[channels=[2]], window=PT5M]";
    }

    protected String expectedDescriptionOfAggregator() {
        return "Window[agg=sum of ints, window=PT5M]";
    }

    protected int inputCount() {
        return 1;
    }

    protected List<Integer> channels(AggregatorMode mode) {
        return mode.isInputPartial()
            ? range(HASH_CHANNEL_COUNT, HASH_CHANNEL_COUNT + aggregatorIntermediateBlockCount()).boxed().toList()
            : range(HASH_CHANNEL_COUNT, HASH_CHANNEL_COUNT + inputCount()).boxed().toList();
    }

    protected final int aggregatorIntermediateBlockCount() {
        try (var agg = aggregatorFunction().groupingAggregator(driverContext(), List.of())) {
            return agg.intermediateBlockCount();
        }
    }

    public void testMissingGroup() {

    }

    /**
     * Verifies that a 7-minute window over 1-minute sub-buckets (GCD of 7m window and 5m user bucket)
     * produces correct sums, and that output filtering to 5-minute boundaries works.
     * <p>
     * Input: single TSID "x", one data point per minute from t+0 to t+14 (15 points), each with value 1.
     * Internal bucket = 1 minute, window = 7 minutes (forward-looking).
     * Output bucket filter = 5 minutes.
     * <p>
     * After window aggregation, each 1-minute bucket merges up to 7 sub-buckets of data.
     * Only buckets at 5-minute boundaries (t+0, t+5, t+10) appear in output.
     */
    public void testNonMultipleWindowWithSubBucketing() {
        Rounding.Prepared oneMinBucket = Rounding.builder(TimeValue.timeValueMinutes(1)).build().prepareForUnknown();
        Rounding.Prepared fiveMinBucket = Rounding.builder(TimeValue.timeValueMinutes(5)).build().prepareForUnknown();
        Duration windowDuration = Duration.ofMinutes(7);

        final long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13");
        long baseTime = oneMinBucket.round(startTime);
        BytesRef tsid = new BytesRef("x");

        List<List<Object>> rows = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            long ts = baseTime + TimeValue.timeValueMinutes(i).millis();
            rows.add(List.of(tsid, ts, 1));
        }

        var operatorFactory = new TimeSeriesAggregationOperator.Factory(
            oneMinBucket,
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
            fiveMinBucket,
            false
        );

        var driverCtx = driverContext();
        var source = new ListRowsBlockSourceOperator(
            driverCtx.blockFactory(),
            List.of(ElementType.BYTES_REF, ElementType.LONG, ElementType.INT),
            rows
        );
        List<Page> results = new ArrayList<>();
        try (
            var driver = org.elasticsearch.compute.test.TestDriverFactory.create(
                driverCtx,
                source,
                List.of(operatorFactory.get(driverCtx)),
                new org.elasticsearch.compute.test.TestResultPageSinkOperator(results::add)
            )
        ) {
            new org.elasticsearch.compute.test.TestDriverRunner().run(driver);
        }

        record OutputRow(String tsid, long bucket, long value) {}
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
        // Output should only contain rows at 5-minute boundaries
        for (OutputRow row : outputRows) {
            assertThat("output timestamp should be aligned to 5-minute boundary", fiveMinBucket.round(row.bucket()), equalTo(row.bucket()));
        }
        // We expect 3 output rows: at baseTime+0, baseTime+5m, baseTime+10m
        assertThat("expected 3 output rows at 5-minute boundaries", outputRows.size(), equalTo(3));

        // Verify the sums:
        // bucket at baseTime+0: window covers [0,7m) → minutes 0..6 → 7 points → sum=7
        // bucket at baseTime+5m: window covers [5m,12m) → minutes 5..11 → 7 points → sum=7
        // bucket at baseTime+10m: window covers [10m,17m) → minutes 10..14 → 5 points → sum=5
        outputRows.sort(Comparator.comparingLong(OutputRow::bucket));
        assertThat("sum at baseTime+0m", outputRows.get(0).value(), equalTo(7L));
        assertThat("sum at baseTime+5m", outputRows.get(1).value(), equalTo(7L));
        assertThat("sum at baseTime+10m", outputRows.get(2).value(), equalTo(5L));
    }

    /**
     * When the output bucket differs from the internal bucket (GCD sub-bucketing), the optimized
     * emit path sets allGroupIds on the evaluation context so that the window function can build
     * a full intermediate page for neighbor lookups while only merging output-aligned groups.
     * This test uses 3 TSIDs with varying data density to stress the merge.
     */
    public void testWindowWithAllGroupIdsUsesFullIntermediateForMerge() {
        Rounding.Prepared oneMinBucket = Rounding.builder(TimeValue.timeValueMinutes(1)).build().prepareForUnknown();
        Rounding.Prepared fiveMinBucket = Rounding.builder(TimeValue.timeValueMinutes(5)).build().prepareForUnknown();
        Duration windowDuration = Duration.ofMinutes(7);

        final long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13");
        long baseTime = oneMinBucket.round(startTime);

        List<List<Object>> rows = new ArrayList<>();
        // TSID "a": data every minute for 10 minutes, value=1
        for (int i = 0; i < 10; i++) {
            rows.add(List.of("a", baseTime + TimeValue.timeValueMinutes(i).millis(), 1));
        }
        // TSID "b": data every minute for 15 minutes, value=2
        for (int i = 0; i < 15; i++) {
            rows.add(List.of("b", baseTime + TimeValue.timeValueMinutes(i).millis(), 2));
        }
        // TSID "c": sparse data at minutes 0, 5, 10, value=100
        for (int i : new int[] { 0, 5, 10 }) {
            rows.add(List.of("c", baseTime + TimeValue.timeValueMinutes(i).millis(), 100));
        }

        var operatorFactory = new TimeSeriesAggregationOperator.Factory(
            oneMinBucket,
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
            fiveMinBucket,
            false
        );

        var driverCtx = driverContext();
        var source = new ListRowsBlockSourceOperator(
            driverCtx.blockFactory(),
            List.of(ElementType.BYTES_REF, ElementType.LONG, ElementType.INT),
            rows
        );
        List<Page> results = new ArrayList<>();
        try (
            var driver = org.elasticsearch.compute.test.TestDriverFactory.create(
                driverCtx,
                source,
                List.of(operatorFactory.get(driverCtx)),
                new org.elasticsearch.compute.test.TestResultPageSinkOperator(results::add)
            )
        ) {
            new org.elasticsearch.compute.test.TestDriverRunner().run(driver);
        }

        record OutputRow(String tsid, long bucket, long value) {}
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

        for (OutputRow row : outputRows) {
            assertThat(fiveMinBucket.round(row.bucket()), equalTo(row.bucket()));
        }

        outputRows.sort(Comparator.comparing(OutputRow::tsid).thenComparingLong(OutputRow::bucket));

        // TSID "a" (value=1, 10 minutes of data): output at 0, 5m
        // bucket 0: [0,7m) → min(7,10)=7 points → sum=7
        // bucket 5m: [5m,12m) → minutes 5..9 → 5 points → sum=5
        List<OutputRow> aRows = outputRows.stream().filter(r -> r.tsid().equals("a")).toList();
        assertThat(aRows.size(), equalTo(2));
        assertThat(aRows.get(0).value(), equalTo(7L));
        assertThat(aRows.get(1).value(), equalTo(5L));

        // TSID "b" (value=2, 15 minutes of data): output at 0, 5m, 10m
        // bucket 0: [0,7m) → 7 points → sum=14
        // bucket 5m: [5m,12m) → 7 points → sum=14
        // bucket 10m: [10m,17m) → 5 points → sum=10
        List<OutputRow> bRows = outputRows.stream().filter(r -> r.tsid().equals("b")).toList();
        assertThat(bRows.size(), equalTo(3));
        assertThat(bRows.get(0).value(), equalTo(14L));
        assertThat(bRows.get(1).value(), equalTo(14L));
        assertThat(bRows.get(2).value(), equalTo(10L));

        // TSID "c" (value=100, sparse at 0, 5, 10): output at 0, 5m, 10m
        // bucket 0: [0,7m) → points at 0 and 5 → sum=200
        // bucket 5m: [5m,12m) → points at 5 and 10 → sum=200
        // bucket 10m: [10m,17m) → point at 10 → sum=100
        List<OutputRow> cRows = outputRows.stream().filter(r -> r.tsid().equals("c")).toList();
        assertThat(cRows.size(), equalTo(3));
        assertThat(cRows.get(0).value(), equalTo(200L));
        assertThat(cRows.get(1).value(), equalTo(200L));
        assertThat(cRows.get(2).value(), equalTo(100L));
    }

    /**
     * When internal bucket == output bucket (no sub-bucketing), allGroupIds is never set on the
     * context and the window function falls back to using selected for everything. Verifies this
     * path still produces correct windowed results.
     */
    public void testWindowWithoutAllGroupIdsFallsBackToSelected() {
        Rounding.Prepared fiveMinBucket = Rounding.builder(TimeValue.timeValueMinutes(5)).build().prepareForUnknown();
        Duration windowDuration = Duration.ofMinutes(10);

        final long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13");
        long baseTime = fiveMinBucket.round(startTime);

        List<List<Object>> rows = new ArrayList<>();
        // One data point per 5-minute bucket for 20 minutes
        for (int i = 0; i < 4; i++) {
            long ts = baseTime + TimeValue.timeValueMinutes(i * 5L).millis();
            rows.add(List.of("s", ts, 10));
        }

        // No outputTimeBucket → needsOutputFiltering() returns false → super.emit() → allGroupIds stays null
        var operatorFactory = new TimeSeriesAggregationOperator.Factory(
            fiveMinBucket,
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
            10_000
        );

        var driverCtx = driverContext();
        var source = new ListRowsBlockSourceOperator(
            driverCtx.blockFactory(),
            List.of(ElementType.BYTES_REF, ElementType.LONG, ElementType.INT),
            rows
        );
        List<Page> results = new ArrayList<>();
        try (
            var driver = org.elasticsearch.compute.test.TestDriverFactory.create(
                driverCtx,
                source,
                List.of(operatorFactory.get(driverCtx)),
                new org.elasticsearch.compute.test.TestResultPageSinkOperator(results::add)
            )
        ) {
            new org.elasticsearch.compute.test.TestDriverRunner().run(driver);
        }

        record OutputRow(String tsid, long bucket, long value) {}
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

        outputRows.sort(Comparator.comparingLong(OutputRow::bucket));
        // 10m window over 5m buckets: each bucket merges itself + the next bucket
        // bucket 0: [0,10m) → points at 0 and 5m → sum=20
        // bucket 5m: [5m,15m) → points at 5m and 10m → sum=20
        // bucket 10m: [10m,20m) → points at 10m and 15m → sum=20
        // bucket 15m: [15m,25m) → point at 15m → sum=10
        assertThat(outputRows.size(), equalTo(4));
        assertThat(outputRows.get(0).value(), equalTo(20L));
        assertThat(outputRows.get(1).value(), equalTo(20L));
        assertThat(outputRows.get(2).value(), equalTo(20L));
        assertThat(outputRows.get(3).value(), equalTo(10L));
    }
}
