/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.ListRowsBlockSourceOperator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class WindowGroupingPredictLinearAggregatorFunctionTests extends WindowGroupingAggregatorFunctionTests {
    private static final long EPOCH = 1_730_000_040_000L;
    private static final double HORIZON = 13;

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        List<List<Object>> rows = new ArrayList<>();
        // Two samples per series and bucket guarantee every window has a defined prediction.
        for (int bucket = 0; bucket < 10; bucket++) {
            for (int series = 0; series < 3; series++) {
                add(rows, series, bucket * 60);
                add(rows, series, bucket * 60 + 59);
            }
        }
        for (int i = 0; i < Math.min(size, 30_000); i++) {
            add(rows, randomIntBetween(0, 2), randomIntBetween(0, 599));
        }
        Collections.shuffle(rows, random());
        return new ListRowsBlockSourceOperator(
            blockFactory,
            List.of(ElementType.BYTES_REF, ElementType.LONG, ElementType.DOUBLE, ElementType.LONG),
            rows
        );
    }

    private static void add(List<List<Object>> rows, int series, long second) {
        long timestamp = EPOCH + second * 1000;
        rows.add(List.of(Integer.toString(series), timeBucket.round(timestamp), value(series, second), timestamp));
    }

    private static double value(int series, double second) {
        return switch (series) {
            case 0 -> Double.MAX_VALUE;
            case 1 -> 10 + second / 4;
            case 2 -> 100 - second / 8;
            default -> throw new AssertionError(series);
        };
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        Set<String> seen = new HashSet<>();
        var scratch = new BytesRef();
        for (Page page : results) {
            BytesRefBlock ids = page.getBlock(0);
            LongBlock buckets = page.getBlock(1);
            DoubleBlock predictions = page.getBlock(2);
            for (int p = 0; p < page.getPositionCount(); p++) {
                int series = Integer.parseInt(ids.getBytesRef(p, scratch).utf8ToString());
                long bucket = buckets.getLong(p);
                assertTrue(seen.add(series + ":" + bucket));
                assertFalse(predictions.isNull(p));
                // This harness uses DOWN rounding: each bucket is evaluated at its exclusive end.
                double expected = value(series, (bucket + 60_000 - EPOCH) / 1000.0 + HORIZON);
                assertEquals(expected, predictions.getDouble(p), Math.abs(expected) * 1e-10);
            }
        }
        assertEquals(30, seen.size());
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new WindowAggregatorFunctionSupplier(new PredictLinearDoubleAggregatorFunctionSupplier(HORIZON), Duration.ofMinutes(5));
    }

    @Override
    protected String expectedToStringOfSimpleAggregator() {
        return "Window[agg=PredictLinearDoubleGroupingAggregatorFunction[channels=[2, 3]], window=PT5M]";
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "Window[agg=predict_linear of doubles, window=PT5M]";
    }

    @Override
    protected int inputCount() {
        return 2;
    }
}
