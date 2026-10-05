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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
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
            Integer.MAX_VALUE  // TODO window functions don't support chunking https://github.com/elastic/elasticsearch/issues/138705
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
        long largestBucket = Long.MIN_VALUE;
        for (Page page : input) {
            LongBlock timestamp = page.getBlock(1);
            for (int p = 0; p < timestamp.getPositionCount(); p++) {
                long bucket = timestamp.getLong(p);
                largestBucket = Math.max(bucket, largestBucket);
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
                // A point at bucket T contributes to backward windows anchored at [T, T+4m].
                for (int i = 0; i < 5; i++) {
                    if (bucket <= largestBucket) {
                        Key key = new Key(tsid, bucket);
                        long val = values.getInt(p);
                        expected.merge(key, val, Long::sum);
                    }
                    bucket = bucket + oneMinute;
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
        return aggregatorFunction().groupingIntermediateStateDesc().size();
    }

    public void testMissingGroup() {

    }

    /**
     * A non-multiple window (7m over 5m buckets) merges the boundary bucket's partial channel into the bucket fully
     * covered by the window, so the value of the bucket labeled {@code L} covers {@code [L - 2m, L + 5m)}. Three
     * TSIDs with varying data density stress the merge; TSID "a" ends before the others, so its last bucket exists
     * only through window expansion and is fed by both channels of the previous buckets.
     */
    public void testNonMultipleWindowWithPartialChannel() {
        Rounding.Prepared fiveMinBucket = Rounding.builder(TimeValue.timeValueMinutes(5)).build().prepareForUnknown();

        final long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13");
        long baseTime = fiveMinBucket.round(startTime);

        List<List<Object>> rows = new ArrayList<>();
        // TSID "a": data every minute for 10 minutes, value=1
        for (int i = 0; i < 10; i++) {
            addMinuteRow(rows, "a", baseTime, i, 1, fiveMinBucket);
        }
        // TSID "b": data every minute for 15 minutes, value=2
        for (int i = 0; i < 15; i++) {
            addMinuteRow(rows, "b", baseTime, i, 2, fiveMinBucket);
        }
        // TSID "c": sparse data at minutes 0, 5, 10, value=100; never in the trailing 2m of a bucket
        for (int i : new int[] { 0, 5, 10 }) {
            addMinuteRow(rows, "c", baseTime, i, 100, fiveMinBucket);
        }

        List<OutputRow> outputRows = runPartialWindowPipeline(fiveMinBucket, Duration.ofMinutes(7), Duration.ofMinutes(2), rows);
        outputRows.sort(Comparator.comparing(OutputRow::tsid).thenComparingLong(OutputRow::bucket));

        // TSID "a" (value=1, minutes 0-9): the 10m bucket exists only through expansion and gets
        // its value from the partial channel of the 5m bucket (minutes 8 and 9).
        List<OutputRow> aRows = outputRows.stream().filter(r -> r.tsid().equals("a")).toList();
        assertThat(aRows.size(), equalTo(3));
        assertThat(aRows.get(0).bucket(), equalTo(baseTime));
        assertThat(aRows.get(1).bucket(), equalTo(baseTime + TimeValue.timeValueMinutes(5).millis()));
        assertThat(aRows.get(2).bucket(), equalTo(baseTime + TimeValue.timeValueMinutes(10).millis()));
        assertThat(aRows.get(0).value(), equalTo(5L));  // [-2m,5m) -> minutes 0..4
        assertThat(aRows.get(1).value(), equalTo(7L));  // [3m,10m) -> minutes 3..9
        assertThat(aRows.get(2).value(), equalTo(2L));  // [8m,15m) -> minutes 8..9
        // TSID "b" (value=2, minutes 0-14).
        List<OutputRow> bRows = outputRows.stream().filter(r -> r.tsid().equals("b")).toList();
        assertThat(bRows.size(), equalTo(3));
        assertThat(bRows.get(0).value(), equalTo(10L)); // minutes 0..4
        assertThat(bRows.get(1).value(), equalTo(14L)); // minutes 3..9
        assertThat(bRows.get(2).value(), equalTo(14L)); // minutes 8..14
        // TSID "c" (value=100, sparse at 0, 5, 10): no point falls in a trailing 2m, so no partial contributions.
        List<OutputRow> cRows = outputRows.stream().filter(r -> r.tsid().equals("c")).toList();
        assertThat(cRows.size(), equalTo(3));
        assertThat(cRows.get(0).value(), equalTo(100L));
        assertThat(cRows.get(1).value(), equalTo(100L));
        assertThat(cRows.get(2).value(), equalTo(100L));
    }

    /**
     * An exact-multiple window has no partial channel; the final phase merges the covered neighbor buckets.
     * Backward windows emit partial leading windows.
     */
    public void testExactMultipleWindowMergesNeighborBuckets() {
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

        List<OutputRow> outputRows = extractRows(results);
        outputRows.sort(Comparator.comparingLong(OutputRow::bucket));
        assertThat(outputRows.size(), equalTo(4));
        assertThat(outputRows.get(0).bucket(), equalTo(baseTime));
        assertThat(outputRows.get(1).bucket(), equalTo(baseTime + TimeValue.timeValueMinutes(5).millis()));
        assertThat(outputRows.get(2).bucket(), equalTo(baseTime + TimeValue.timeValueMinutes(10).millis()));
        assertThat(outputRows.get(3).bucket(), equalTo(baseTime + TimeValue.timeValueMinutes(15).millis()));
        assertThat(outputRows.get(0).value(), equalTo(10L));
        assertThat(outputRows.get(1).value(), equalTo(20L));
        assertThat(outputRows.get(2).value(), equalTo(20L));
        assertThat(outputRows.get(3).value(), equalTo(20L));
    }

    /**
     * A window spanning more than one full bucket plus a remainder ({@code W = 2 * B + r}): the merge combines the
     * two fully covered buckets and the partial state of the bucket before them.
     */
    public void testMultiBucketWindowWithPartialChannel() {
        Rounding.Prepared fiveMinBucket = Rounding.builder(TimeValue.timeValueMinutes(5)).build().prepareForUnknown();

        final long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13");
        long baseTime = fiveMinBucket.round(startTime);

        List<List<Object>> rows = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            addMinuteRow(rows, "solo", baseTime, i, 1, fiveMinBucket);
        }

        List<OutputRow> outputRows = runPartialWindowPipeline(fiveMinBucket, Duration.ofMinutes(12), Duration.ofMinutes(2), rows);
        outputRows.sort(Comparator.comparingLong(OutputRow::bucket));
        assertThat(outputRows.size(), equalTo(5));
        for (int i = 0; i < 5; i++) {
            assertThat(outputRows.get(i).bucket(), equalTo(baseTime + TimeValue.timeValueMinutes(i * 5L).millis()));
        }
        assertThat(outputRows.get(0).value(), equalTo(5L));  // [-7m,5m) -> minutes 0..4
        assertThat(outputRows.get(1).value(), equalTo(10L)); // [-2m,10m) -> minutes 0..9
        assertThat(outputRows.get(2).value(), equalTo(12L)); // [3m,15m) -> minutes 3..14
        assertThat(outputRows.get(3).value(), equalTo(12L)); // [8m,20m) -> minutes 8..19
        assertThat(outputRows.get(4).value(), equalTo(12L)); // [13m,25m) -> minutes 13..24
    }

    record OutputRow(String tsid, long bucket, long value) {}

    static List<OutputRow> extractRows(List<Page> results) {
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
     * Adds a row of {@code (tsid, bucket, value, raw timestamp)} for the given minute offset. The raw timestamp
     * column drives the partial-channel filter, the bucket column drives the grouping.
     */
    static void addMinuteRow(List<List<Object>> rows, String tsid, long baseTime, int minute, int value, Rounding.Prepared bucket) {
        long ts = baseTime + TimeValue.timeValueMinutes(minute).millis();
        rows.add(List.of(tsid, bucket.round(ts), value, ts));
    }

    /**
     * Runs a SINGLE-mode pipeline for a non-multiple window: rows are {@code (tsid, bucket, value, timestamp)} and
     * the partial channel is filtered to the trailing {@code remainder} of each bucket via the raw timestamp column.
     */
    private List<OutputRow> runPartialWindowPipeline(
        Rounding.Prepared bucket,
        Duration windowDuration,
        Duration remainder,
        List<List<Object>> rows
    ) {
        var sum = new SumIntAggregatorFunctionSupplier();
        var partial = new FilteredAggregatorFunctionSupplier(sum, trailingWindowFilter(bucket, remainder.toMillis(), 1, 3));
        var operatorFactory = new TimeSeriesAggregationOperator.Factory(
            bucket,
            false,
            List.of(
                new BlockHash.GroupSpec(0, ElementType.BYTES_REF, null, null),
                new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
            ),
            AggregatorMode.SINGLE,
            List.of(
                new WindowAggregatorFunctionSupplier(sum, partial, windowDuration, remainder).groupingAggregatorFactory(
                    AggregatorMode.SINGLE,
                    List.of(HASH_CHANNEL_COUNT)
                )
            ),
            10_000
        );

        var driverCtx = driverContext();
        var source = new ListRowsBlockSourceOperator(
            driverCtx.blockFactory(),
            List.of(ElementType.BYTES_REF, ElementType.LONG, ElementType.INT, ElementType.LONG),
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
        return extractRows(results);
    }

    /**
     * A stand-in for the {@code WindowFilter} row predicate, which lives in the esql module and is not available
     * here: keeps rows whose raw timestamp falls in the trailing {@code remainderMillis} of their bucket.
     */
    static ExpressionEvaluator.Factory trailingWindowFilter(
        Rounding.Prepared bucket,
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
                        long bucketEnd = bucket.nextRoundingValue(bucketBlock.getLong(p));
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

}
