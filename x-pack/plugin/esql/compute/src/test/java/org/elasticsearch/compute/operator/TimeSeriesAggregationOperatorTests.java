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
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.RateLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperatorTests;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.compute.lucene.TimeSeriesSortedSourceOperatorTests.createTimeSeriesSourceOperator;
import static org.elasticsearch.compute.lucene.TimeSeriesSortedSourceOperatorTests.writeTS;
import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesAggregationOperatorTests extends AnyOperatorTestCase {

    private IndexReader reader;
    private final Directory directory = newDirectory();

    @After
    public void cleanup() throws IOException {
        IOUtils.close(reader, directory);
    }

    @Override
    protected Operator.OperatorFactory simple() {
        return new TimeSeriesAggregationOperatorFactory(AggregatorMode.FINAL, 0, 1, TimeValue.ZERO, List.of(), 100);
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        return "TimeSeriesAggregationOperator[mode=FINAL, tsHashChannel = 0, timestampIntervalChannel = 1, "
            + "timeSeriesPeriod = 0s, maxPageSize = 100]";
    }

    @Override
    protected String expectedToStringOfSimple() {
        return "HashAggregationOperator[blockHash=TimeSeriesBlockHash{keys=[BytesRefKey[channel=0], "
            + "LongKey[channel=1]], entries=-1b}, aggregators=[]]";
    }

    public void testBasicRate() {
        long[] v1 = { 1, 1, 3, 0, 2, 9, 21, 3, 7, 7, 9, 12 };
        long[] t1 = { 1, 5, 11, 20, 21, 59, 88, 91, 92, 97, 99, 112 };

        long[] v2 = { 7, 2, 0, 11, 24, 0, 4, 1, 10, 2 };
        long[] t2 = { 1, 2, 4, 5, 6, 8, 10, 11, 12, 14 };

        long[] v3 = { 0, 1, 0, 1, 1, 4, 2, 2, 2, 2, 3, 5, 5 };
        long[] t3 = { 2, 3, 5, 7, 8, 9, 10, 12, 14, 15, 18, 20, 22 };
        List<Pod> pods = List.of(new Pod("p1", t1, v1), new Pod("p2", t2, v2), new Pod("p3", t3, v3));
        long unit = between(1, 5);
        Map<Group, Double> actualRates = runRateTest(pods, TimeValue.timeValueMillis(unit), TimeValue.ZERO);
        assertThat(
            actualRates,
            equalTo(
                Map.of(
                    new Group("\u0001\u0003pods\u0002p1", 0),
                    35.0 * unit / 111.0,
                    new Group("\u0001\u0003pods\u0002p2", 0),
                    42.0 * unit / 13.0,
                    new Group("\u0001\u0003pods\u0002p3", 0),
                    10.0 * unit / 20.0
                )
            )
        );
    }

    public void testRateWithInterval() {
        long[] v1 = { 1, 2, 3, 0, 1, 2, 3, 4, 5, 0, 1, 2, 3 };
        long[] t1 = { 0, 10_000, 20_000, 30_000, 40_000, 50_000, 60_000, 70_000, 80_000, 90_000, 100_000, 110_000, 120_000 };

        long[] v2 = { 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2 };
        long[] t2 = { 0, 10_000, 20_000, 30_000, 40_000, 50_000, 60_000, 70_000, 80_000, 90_000, 100_000, 110_000, 120_000 };

        long[] v3 = { 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192 };
        long[] t3 = { 0, 10_000, 20_000, 30_000, 40_000, 50_000, 60_000, 70_000, 80_000, 90_000, 100_000, 110_000, 120_000 };
        List<Pod> pods = List.of(new Pod("p1", t1, v1), new Pod("p2", t2, v2), new Pod("p3", t3, v3));
        Map<Group, Double> actualRates = runRateTest(pods, TimeValue.timeValueMillis(1), TimeValue.timeValueMinutes(1));
        assertMap(
            actualRates,
            matchesMap().entry(new Group("\u0001\u0003pods\u0002p1", 120_000), 0.0D)
                .entry(new Group("\u0001\u0003pods\u0002p1", 60_000), 8.0E-5D)
                .entry(new Group("\u0001\u0003pods\u0002p1", 0), 8.0E-5D)
                .entry(new Group("\u0001\u0003pods\u0002p2", 120_000), 0.0D)
                .entry(new Group("\u0001\u0003pods\u0002p2", 60_000), 0.0D)
                .entry(new Group("\u0001\u0003pods\u0002p2", 0), 0.0D)
                .entry(new Group("\u0001\u0003pods\u0002p3", 120_000), 0.0D)
                .entry(new Group("\u0001\u0003pods\u0002p3", 60_000), 0.07936D)
                .entry(new Group("\u0001\u0003pods\u0002p3", 0), 0.00124D)
        );
    }

    public void testRandomRate() {
        int numPods = between(1, 10);
        List<Pod> pods = new ArrayList<>();
        Map<Group, Double> expectedRates = new HashMap<>();
        TimeValue unit = TimeValue.timeValueSeconds(1);
        for (int p = 0; p < numPods; p++) {
            int numValues = between(2, 100);
            long[] values = new long[numValues];
            long[] times = new long[numValues];
            long t = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-01-01T00:00:00Z");
            for (int i = 0; i < numValues; i++) {
                values[i] = randomIntBetween(0, 100);
                t += TimeValue.timeValueSeconds(between(1, 10)).millis();
                times[i] = t;
            }
            Pod pod = new Pod("p" + p, times, values);
            pods.add(pod);
            if (numValues == 1) {
                expectedRates.put(new Group("\u0001\u0003pods\u0002" + pod.name, 0), null);
            } else {
                expectedRates.put(new Group("\u0001\u0003pods\u0002" + pod.name, 0), pod.expectedRate(unit));
            }
        }
        Map<Group, Double> actualRates = runRateTest(pods, unit, TimeValue.ZERO);
        assertThat(actualRates, equalTo(expectedRates));
    }

    record Pod(String name, long[] times, long[] values) {
        Pod {
            assert times.length == values.length : times.length + "!=" + values.length;
        }

        double expectedRate(TimeValue unit) {
            double dv = 0;
            for (int i = 0; i < values.length - 1; i++) {
                if (values[i + 1] < values[i]) {
                    dv += values[i];
                }
            }
            dv += (values[values.length - 1] - values[0]);
            long dt = times[times.length - 1] - times[0];
            return (dv * unit.millis()) / dt;
        }
    }

    Map<Group, Double> runRateTest(List<Pod> pods, TimeValue unit, TimeValue interval) {
        long unitInMillis = unit.millis();
        record Doc(String pod, long timestamp, long requests) {

        }
        var sourceOperatorFactory = createTimeSeriesSourceOperator(
            directory,
            r -> this.reader = r,
            Integer.MAX_VALUE,
            between(1, 100),
            randomBoolean(),
            interval,
            writer -> {
                List<Doc> docs = new ArrayList<>();
                for (Pod pod : pods) {
                    for (int i = 0; i < pod.times.length; i++) {
                        docs.add(new Doc(pod.name, pod.times[i], pod.values[i]));
                    }
                }
                Randomness.shuffle(docs);
                for (Doc doc : docs) {
                    writeTS(writer, doc.timestamp, new Object[] { "pod", doc.pod }, new Object[] { "requests", doc.requests });
                }
                return docs.size();
            }
        );
        var ctx = driverContext();

        var aggregators = List.of(
            new RateLongAggregatorFunctionSupplier(List.of(4, 2), unitInMillis).groupingAggregatorFactory(AggregatorMode.INITIAL)
        );
        Operator initialHash = new TimeSeriesAggregationOperatorFactory(
            AggregatorMode.INITIAL,
            1,
            3,
            interval,
            aggregators,
            randomIntBetween(1, 1000)
        ).get(ctx);

        aggregators = List.of(
            new RateLongAggregatorFunctionSupplier(List.of(2, 3, 4), unitInMillis).groupingAggregatorFactory(AggregatorMode.FINAL)
        );
        Operator finalHash = new TimeSeriesAggregationOperatorFactory(
            AggregatorMode.FINAL,
            0,
            1,
            interval,
            aggregators,
            randomIntBetween(1, 1000)
        ).get(ctx);
        List<Page> results = new ArrayList<>();
        var requestsField = new NumberFieldMapper.NumberFieldType("requests", NumberFieldMapper.NumberType.LONG);
        OperatorTestCase.runDriver(
            new Driver(
                ctx,
                sourceOperatorFactory.get(ctx),
                List.of(ValuesSourceReaderOperatorTests.factory(reader, requestsField, ElementType.LONG).get(ctx), initialHash, finalHash),
                new TestResultPageSinkOperator(results::add),
                () -> {}
            )
        );
        Map<Group, Double> rates = new HashMap<>();
        for (Page result : results) {
            BytesRefBlock keysBlock = result.getBlock(0);
            LongBlock timestampIntervalsBock = result.getBlock(1);
            DoubleBlock ratesBlock = result.getBlock(2);
            for (int i = 0; i < result.getPositionCount(); i++) {
                var key = new Group(keysBlock.getBytesRef(i, new BytesRef()).utf8ToString(), timestampIntervalsBock.getLong(i));
                rates.put(key, ratesBlock.getDouble(i));
            }
            result.releaseBlocks();
        }
        return rates;
    }

    record Group(String tsidHash, long timestampInterval) {}
}
