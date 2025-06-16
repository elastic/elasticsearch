/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.aggregation.RateLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperatorTests;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.junit.After;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.lucene.TimeSeriesSortedSourceOperatorTests.createTimeSeriesSourceOperator;
import static org.elasticsearch.compute.lucene.TimeSeriesSortedSourceOperatorTests.writeTS;
import static org.elasticsearch.compute.operator.TimeSeriesAggregationOperatorFactories.SupplierWithChannels;
import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesAggregationOperatorTests extends ComputeTestCase {

    private IndexReader reader = null;
    private Directory directory = null;

    @After
    public void cleanup() throws IOException {
        IOUtils.close(reader, directory);
    }

    /**
     * A {@link DriverContext} with a nonBreakingBigArrays.
     */
    protected DriverContext driverContext() { // TODO make this final once all operators support memory tracking
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory);
    }

    public void testBasicRate() throws Exception {
        long[] v1 = { 1, 1, 3, 0, 2, 9, 21, 3, 7, 7, 9, 12 };
        long[] t1 = { 1, 5, 11, 20, 21, 59, 88, 91, 92, 97, 99, 112 };

        long[] v2 = { 7, 2, 0, 11, 24, 0, 4, 1, 10, 2 };
        long[] t2 = { 1, 2, 4, 5, 6, 8, 10, 11, 12, 14 };

        long[] v3 = { 0, 1, 0, 1, 1, 4, 2, 2, 2, 2, 3, 5, 5 };
        long[] t3 = { 2, 3, 5, 7, 8, 9, 10, 12, 14, 15, 18, 20, 22 };
        List<Pod> pods = List.of(
            new Pod("p1", "cluster_1", new Interval(2100, t1, v1)),
            new Pod("p2", "cluster_1", new Interval(600, t2, v2)),
            new Pod("p3", "cluster_2", new Interval(1100, t3, v3))
        );
        long unit = between(1, 5);
        {
            List<List<Object>> actual = runRateTest(
                pods,
                List.of("cluster"),
                TimeValue.timeValueMillis(unit),
                TimeValue.timeValueMillis(500)
            );
            List<List<Object>> expected = List.of(
                List.of(new BytesRef("cluster_1"), 35.0 * unit / 111.0 + 42.0 * unit / 13.0),
                List.of(new BytesRef("cluster_2"), 10.0 * unit / 20.0)
            );
            assertThat(actual, equalTo(expected));
        }
        {
            List<List<Object>> actual = runRateTest(pods, List.of("pod"), TimeValue.timeValueMillis(unit), TimeValue.timeValueMillis(500));
            List<List<Object>> expected = List.of(
                List.of(new BytesRef("p1"), 35.0 * unit / 111.0),
                List.of(new BytesRef("p2"), 42.0 * unit / 13.0),
                List.of(new BytesRef("p3"), 10.0 * unit / 20.0)
            );
            assertThat(actual, equalTo(expected));
        }
        {
            List<List<Object>> actual = runRateTest(
                pods,
                List.of("cluster", "bucket"),
                TimeValue.timeValueMillis(unit),
                TimeValue.timeValueMillis(500)
            );
            List<List<Object>> expected = List.of(
                List.of(new BytesRef("cluster_1"), 2000L, 35.0 * unit / 111.0),
                List.of(new BytesRef("cluster_1"), 500L, 42.0 * unit / 13.0),
                List.of(new BytesRef("cluster_2"), 1000L, 10.0 * unit / 20.0)
            );
            assertThat(actual, equalTo(expected));
        }
    }

    public void testRateWithInterval() throws Exception {
        long[] v1 = { 1, 2, 3, 0, 1, 2, 3, 4, 5, 0, 1, 2, 3 };
        long[] t1 = { 0, 10_000, 20_000, 30_000, 40_000, 50_000, 60_000, 70_000, 80_000, 90_000, 100_000, 110_000, 120_000 };

        long[] v2 = { 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2 };
        long[] t2 = { 0, 10_000, 20_000, 30_000, 40_000, 50_000, 60_000, 70_000, 80_000, 90_000, 100_000, 110_000, 120_000 };

        long[] v3 = { 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192 };
        long[] t3 = { 0, 10_000, 20_000, 30_000, 40_000, 50_000, 60_000, 70_000, 80_000, 90_000, 100_000, 110_000, 120_000 };
        List<Pod> pods = List.of(
            new Pod("p1", "cluster_1", new Interval(0, t1, v1)),
            new Pod("p2", "cluster_2", new Interval(0, t2, v2)),
            new Pod("p3", "cluster_2", new Interval(0, t3, v3))
        );
        List<List<Object>> actual = runRateTest(
            pods,
            List.of("pod", "bucket"),
            TimeValue.timeValueMillis(1),
            TimeValue.timeValueMinutes(1)
        );
        List<List<Object>> expected = List.of(
            List.of(new BytesRef("p1]"), 120_000L, 0.0D),
            List.of(new BytesRef("p1"), 60_000L, 8.0E-5D),
            List.of(new BytesRef("p1"), 0, 8.0E-5D),
            List.of(new BytesRef("p2"), 120_000L, 0.0D),
            List.of(new BytesRef("p2"), 60_000L, 0.0D),
            List.of(new BytesRef("p2"), 0L, 0.0D),
            List.of(new BytesRef("p3"), 120_000L, 0.0D),
            List.of(new BytesRef("p3"), 60_000L, 0.07936D),
            List.of(new BytesRef("p3"), 0L, 0.00124D)
        );
    }

    public void testRandomRate() throws Exception {
        int numPods = between(1, 10);
        List<Pod> pods = new ArrayList<>();
        TimeValue unit = TimeValue.timeValueSeconds(1);
        List<List<Object>> expected = new ArrayList<>();
        for (int p = 0; p < numPods; p++) {
            int numIntervals = randomIntBetween(1, 3);
            Interval[] intervals = new Interval[numIntervals];
            long startTimeInHours = between(10, 100);
            String podName = "p" + p;
            for (int interval = 0; interval < numIntervals; interval++) {
                final long startInterval = TimeValue.timeValueHours(--startTimeInHours).millis();
                int numValues = between(2, 100);
                long[] values = new long[numValues];
                long[] times = new long[numValues];
                long delta = 0;
                for (int i = 0; i < numValues; i++) {
                    values[i] = randomIntBetween(0, 100);
                    delta += TimeValue.timeValueSeconds(between(1, 10)).millis();
                    times[i] = delta;
                }
                intervals[interval] = new Interval(startInterval, times, values);
                if (numValues == 1) {
                    expected.add(List.of(new BytesRef(podName), startInterval, null));
                } else {
                    expected.add(List.of(new BytesRef(podName), startInterval, intervals[interval].expectedRate(unit)));
                }
            }
            Pod pod = new Pod(podName, "cluster", intervals);
            pods.add(pod);
        }
        List<List<Object>> actual = runRateTest(pods, List.of("pod", "bucket"), unit, TimeValue.timeValueHours(1));
        assertThat(actual, equalTo(expected));
    }

    record Interval(long offset, long[] times, long[] values) {
        double expectedRate(TimeValue unit) {
            double dv = 0;
            for (int v = 0; v < values.length - 1; v++) {
                if (values[v + 1] < values[v]) {
                    dv += values[v];
                }
            }
            dv += (values[values.length - 1] - values[0]);
            long dt = times[times.length - 1] - times[0];
            return (dv * unit.millis()) / dt;
        }
    }

    record Pod(String name, String cluster, Interval... intervals) {}

    List<List<Object>> runRateTest(List<Pod> pods, List<String> groupings, TimeValue unit, TimeValue bucketInterval) throws IOException {
        cleanup();
        directory = newDirectory();
        long unitInMillis = unit.millis();
        record Doc(String pod, String cluster, long timestamp, long requests) {

        }
        var sourceOperatorFactory = createTimeSeriesSourceOperator(
            directory,
            r -> this.reader = r,
            Integer.MAX_VALUE,
            between(1, 100),
            randomBoolean(),
            writer -> {
                List<Doc> docs = new ArrayList<>();
                for (Pod pod : pods) {
                    for (Interval interval : pod.intervals) {
                        for (int i = 0; i < interval.times.length; i++) {
                            docs.add(new Doc(pod.name, pod.cluster, interval.offset + interval.times[i], interval.values[i]));
                        }
                    }
                }
                Randomness.shuffle(docs);
                for (Doc doc : docs) {
                    writeTS(
                        writer,
                        doc.timestamp,
                        new Object[] { "pod", doc.pod, "cluster", doc.cluster },
                        new Object[] { "requests", doc.requests }
                    );
                }
                return docs.size();
            }
        );
        var ctx = driverContext();

        List<Operator> intermediateOperators = new ArrayList<>();
        final Rounding.Prepared rounding = new Rounding.Builder(bucketInterval).timeZone(ZoneOffset.UTC).build().prepareForUnknown();
        var timeBucket = new EvalOperator(ctx.blockFactory(), new EvalOperator.ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                LongBlock timestampsBlock = page.getBlock(2);
                LongVector timestamps = timestampsBlock.asVector();
                try (var builder = blockFactory().newLongVectorFixedBuilder(timestamps.getPositionCount())) {
                    for (int i = 0; i < timestamps.getPositionCount(); i++) {
                        builder.appendLong(rounding.round(timestampsBlock.getLong(i)));
                    }
                    return builder.build().asBlock();
                }
            }

            @Override
            public void close() {

            }
        });
        intermediateOperators.add(timeBucket);
        var rateField = new NumberFieldMapper.NumberFieldType("requests", NumberFieldMapper.NumberType.LONG);
        Operator extractRate = (ValuesSourceReaderOperatorTests.factory(reader, rateField, ElementType.LONG).get(ctx));
        intermediateOperators.add(extractRate);
        List<String> nonBucketGroupings = new ArrayList<>(groupings);
        nonBucketGroupings.remove("bucket");
        for (String grouping : nonBucketGroupings) {
            var groupingField = new KeywordFieldMapper.KeywordFieldType(grouping);
            intermediateOperators.add(ValuesSourceReaderOperatorTests.factory(reader, groupingField, ElementType.BYTES_REF).get(ctx));
        }
        // _doc, tsid, timestamp, bucket, requests, grouping1, grouping2
        Operator intialAgg = new TimeSeriesAggregationOperatorFactories.Initial(
            1,
            3,
            IntStream.range(0, nonBucketGroupings.size()).mapToObj(n -> new BlockHash.GroupSpec(5 + n, ElementType.BYTES_REF)).toList(),
            List.of(new SupplierWithChannels(new RateLongAggregatorFunctionSupplier(unitInMillis), List.of(4, 2))),
            List.of(),
            between(1, 100)
        ).get(ctx);

        // tsid, bucket, rate[0][0],rate[0][1],rate[0][2], grouping1, grouping2
        Operator intermediateAgg = new TimeSeriesAggregationOperatorFactories.Intermediate(
            0,
            1,
            IntStream.range(0, nonBucketGroupings.size()).mapToObj(n -> new BlockHash.GroupSpec(5 + n, ElementType.BYTES_REF)).toList(),
            List.of(new SupplierWithChannels(new RateLongAggregatorFunctionSupplier(unitInMillis), List.of(2, 3, 4))),
            List.of(),
            between(1, 100)
        ).get(ctx);
        // tsid, bucket, rate, grouping1, grouping2
        List<BlockHash.GroupSpec> finalGroups = new ArrayList<>();
        int groupChannel = 3;
        for (String grouping : groupings) {
            if (grouping.equals("bucket")) {
                finalGroups.add(new BlockHash.GroupSpec(1, ElementType.LONG));
            } else {
                finalGroups.add(new BlockHash.GroupSpec(groupChannel++, ElementType.BYTES_REF));
            }
        }
        Operator finalAgg = new TimeSeriesAggregationOperatorFactories.Final(
            finalGroups,
            List.of(new SupplierWithChannels(new SumDoubleAggregatorFunctionSupplier(), List.of(2))),
            List.of(),
            between(1, 100)
        ).get(ctx);

        List<Page> results = new ArrayList<>();
        OperatorTestCase.runDriver(
            new Driver(
                "test",
                ctx,
                sourceOperatorFactory.get(ctx),
                CollectionUtils.concatLists(intermediateOperators, List.of(intialAgg, intermediateAgg, finalAgg)),
                new TestResultPageSinkOperator(results::add),
                () -> {}
            )
        );
        List<List<Object>> values = new ArrayList<>();
        for (Page result : results) {
            for (int p = 0; p < result.getPositionCount(); p++) {
                int blockCount = result.getBlockCount();
                List<Object> row = new ArrayList<>();
                for (int b = 0; b < blockCount; b++) {
                    row.add(BlockUtils.toJavaObject(result.getBlock(b), p));
                }
                values.add(row);
            }
            result.releaseBlocks();
        }
        values.sort((v1, v2) -> {
            for (int i = 0; i < v1.size(); i++) {
                if (v1.get(i) instanceof BytesRef b1) {
                    int cmp = b1.compareTo((BytesRef) v2.get(i));
                    if (cmp != 0) {
                        return cmp;
                    }
                } else if (v1.get(i) instanceof Long b1) {
                    int cmp = b1.compareTo((Long) v2.get(i));
                    if (cmp != 0) {
                        return -cmp;
                    }
                }
            }
            return 0;
        });
        return values;
    }
}
