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
            randomPageSize()
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
}
