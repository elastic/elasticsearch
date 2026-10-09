/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.TimeSeriesAggregationOperator;
import org.elasticsearch.compute.test.operator.blocksource.ListRowsBlockSourceOperator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import static java.util.stream.IntStream.range;
import static org.hamcrest.Matchers.equalTo;

/**
 * Runs the {@link org.elasticsearch.compute.operator.ForkingOperatorTestCase} matrix over a window with a partial
 * side ({@code W = 90s} over 1-minute buckets, remainder 30s) through INITIAL, INTERMEDIATE, and FINAL phase
 * splits, mirroring the production wiring: the raw-input phase runs the full aggregate and its filtered partial
 * sibling as two ordinary aggregates, the intermediate phase reduces each side separately, and only the final phase
 * pairs them, with the window wrapper reading the full state channels followed by the sibling's. Rows carry a raw
 * timestamp column that drives the partial filter in the raw-input phase.
 */
public class WindowGroupingPartialAggregatorFunctionTests extends WindowGroupingAggregatorFunctionTests {

    private static final Duration WINDOW = Duration.ofSeconds(90);
    private static final Duration REMAINDER = Duration.ofSeconds(30);

    @Override
    protected Operator.OperatorFactory simpleWithMode(SimpleOptions options, AggregatorMode mode) {
        var sum = new SumIntAggregatorFunctionSupplier();
        List<GroupingAggregator.Factory> aggregators = new ArrayList<>(2);
        if (mode.isInputPartial()) {
            int stateSize = sum.groupingIntermediateStateDesc().size();
            List<Integer> fullChannels = range(HASH_CHANNEL_COUNT, HASH_CHANNEL_COUNT + stateSize).boxed().toList();
            List<Integer> partialChannels = range(HASH_CHANNEL_COUNT + stateSize, HASH_CHANNEL_COUNT + 2 * stateSize).boxed().toList();
            if (mode.isOutputPartial()) {
                // INTERMEDIATE: each side reduces its own state as an ordinary aggregate
                aggregators.add(sum.groupingAggregatorFactory(mode, fullChannels));
                aggregators.add(sum.groupingAggregatorFactory(mode, partialChannels));
            } else {
                // FINAL: the window wrapper merges the full state with the sibling's; the sibling also emits its
                // own column, which nothing reads, mirroring the production wiring
                var window = new WindowAggregatorFunctionSupplier(sum, sum, WINDOW, REMAINDER);
                List<Integer> combined = Stream.concat(fullChannels.stream(), partialChannels.stream()).toList();
                aggregators.add(window.groupingAggregatorFactory(mode, combined));
                aggregators.add(sum.groupingAggregatorFactory(mode, partialChannels));
            }
        } else {
            List<Integer> rawChannels = List.of(HASH_CHANNEL_COUNT);
            var filteredPartial = new FilteredAggregatorFunctionSupplier(sum, trailingWindowFilter(timeBucket, REMAINDER.toMillis(), 1, 3));
            if (mode.isOutputPartial()) {
                // INITIAL: the full aggregate and the filtered partial sibling execute as two ordinary aggregates
                aggregators.add(sum.groupingAggregatorFactory(mode, rawChannels));
                aggregators.add(filteredPartial.groupingAggregatorFactory(mode, rawChannels));
            } else {
                // SINGLE: raw input straight to final values; the partial side filters its own rows
                var window = new WindowAggregatorFunctionSupplier(sum, filteredPartial, WINDOW, REMAINDER);
                aggregators.add(window.groupingAggregatorFactory(mode, rawChannels));
            }
        }
        return new TimeSeriesAggregationOperator.Factory(
            timeBucket,
            false,
            List.of(
                new BlockHash.GroupSpec(0, ElementType.BYTES_REF, null, null),
                new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
            ),
            mode,
            aggregators,
            Integer.MAX_VALUE  // TODO window functions don't support chunking https://github.com/elastic/elasticsearch/issues/138705
        );
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        final long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13");
        List<BytesRef> groups = List.of(new BytesRef("a"), new BytesRef("b"), new BytesRef("c"), new BytesRef("d"));
        // enough rows that groups regularly receive several full and partial intermediate states to merge, bounded
        // to keep the forked test matrix fast
        size = Math.min(size, 1_000);
        List<List<Object>> rows = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            long tsOffset = randomLongBetween(0, 20 * 60 * 1000);
            long ts = startTime + tsOffset;
            long bucket = timeBucket.round(ts);
            int value = randomIntBetween(1, 1000);
            BytesRef tsid = randomFrom(groups);
            rows.add(List.of(tsid, bucket, value, ts));
        }
        rows.sort(Comparator.comparing((List<Object> row) -> (BytesRef) row.get(0)).thenComparingLong(row -> (Long) row.get(1)));
        return new ListRowsBlockSourceOperator(
            blockFactory,
            List.of(ElementType.BYTES_REF, ElementType.LONG, ElementType.INT, ElementType.LONG),
            rows
        );
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        record Key(String tsid, long bucket) {}
        long oneMinute = TimeValue.timeValueMinutes(1).millis();
        long largestBucket = Long.MIN_VALUE;
        for (Page page : input) {
            LongBlock bucketBlock = page.getBlock(1);
            for (int p = 0; p < bucketBlock.getPositionCount(); p++) {
                largestBucket = Math.max(bucketBlock.getLong(p), largestBucket);
            }
        }
        Comparator<Key> keyOrder = Comparator.comparing(Key::tsid).thenComparingLong(Key::bucket);
        // The value of the bucket labeled L covers [L + 1m - W, L + 1m) = [L - 30s, L + 1m): its own rows plus the
        // rows in the trailing 30s of the previous bucket. A group with no contributions (created only by window
        // expansion, with nothing in the previous bucket's trailing 30s) emits null.
        Map<Key, Long> fullSums = new TreeMap<>(keyOrder);
        Map<Key, Long> partialSums = new TreeMap<>(keyOrder);
        Map<Key, Long> expected = new TreeMap<>(keyOrder);
        for (Page page : input) {
            BytesRefBlock tsids = page.getBlock(0);
            LongBlock bucketBlock = page.getBlock(1);
            IntBlock values = page.getBlock(2);
            LongBlock timestamps = page.getBlock(3);
            var scratch = new BytesRef();
            for (int p = 0; p < page.getPositionCount(); p++) {
                var tsid = tsids.getBytesRef(p, scratch).utf8ToString();
                long bucket = bucketBlock.getLong(p);
                long value = values.getInt(p);
                fullSums.merge(new Key(tsid, bucket), value, Long::sum);
                if (timestamps.getLong(p) >= bucket + oneMinute - REMAINDER.toMillis()) {
                    partialSums.merge(new Key(tsid, bucket + oneMinute), value, Long::sum);
                }
                expected.putIfAbsent(new Key(tsid, bucket), null);
                if (bucket + oneMinute <= largestBucket) {
                    // window expansion materializes the next bucket, whose window still covers this bucket's tail
                    expected.putIfAbsent(new Key(tsid, bucket + oneMinute), null);
                }
            }
        }
        for (Key key : expected.keySet()) {
            Long full = fullSums.get(key);
            Long partial = partialSums.get(key);
            if (full != null || partial != null) {
                expected.put(key, (full == null ? 0 : full) + (partial == null ? 0 : partial));
            }
        }
        Map<Key, Long> actual = new TreeMap<>(keyOrder);
        for (Page page : results) {
            var scratch = new BytesRef();
            BytesRefBlock tsids = page.getBlock(0);
            LongBlock buckets = page.getBlock(1);
            LongBlock values = page.getBlock(2);
            for (int p = 0; p < buckets.getPositionCount(); p++) {
                var tsid = tsids.getBytesRef(p, scratch).utf8ToString();
                Key key = new Key(tsid, buckets.getLong(p));
                actual.put(key, values.isNull(p) ? null : values.getLong(p));
            }
        }
        assertThat(actual, equalTo(expected));
    }

    @Override
    protected String expectedToStringOfSimpleAggregator() {
        return "Window[agg=SumIntGroupingAggregatorFunction[channels=[2]], window=PT1M30S, partial=PT30S]";
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "Window[agg=sum of ints, window=PT1M30S, partial=PT30S]";
    }
}
