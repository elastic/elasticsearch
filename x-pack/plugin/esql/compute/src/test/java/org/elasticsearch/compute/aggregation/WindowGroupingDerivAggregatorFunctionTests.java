/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.ListRowsBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.index.mapper.DateFieldMapper;

import java.time.Duration;
import java.util.List;
import java.util.TreeMap;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class WindowGroupingDerivAggregatorFunctionTests extends WindowGroupingAggregatorFunctionTests {

    private static final long BUCKET_0 = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13T00:00:00Z");
    private static final long BUCKET_1 = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13T00:01:00Z");
    private static final long BUCKET_2 = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-11-13T00:02:00Z");

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new ListRowsBlockSourceOperator(
            blockFactory,
            List.of(ElementType.BYTES_REF, ElementType.LONG, ElementType.DOUBLE, ElementType.LONG),
            List.of(List.of(new BytesRef("a"), BUCKET_0, 1.0d, BUCKET_0), List.of(new BytesRef("a"), BUCKET_2, 3.0d, BUCKET_2))
        );
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        TreeMap<Long, Double> actual = new TreeMap<>();
        for (Page page : results) {
            LongBlock buckets = page.getBlock(1);
            DoubleBlock deriv = page.getBlock(2);
            for (int p = 0; p < page.getPositionCount(); p++) {
                actual.put(buckets.getLong(p), deriv.isNull(p) ? null : deriv.getDouble(p));
            }
        }

        assertThat(actual.size(), equalTo(3));
        assertThat(actual.containsKey(BUCKET_0), equalTo(true));
        assertThat(actual.containsKey(BUCKET_1), equalTo(true));
        assertThat(actual.containsKey(BUCKET_2), equalTo(true));
        assertThat(actual.get(BUCKET_0), notNullValue());
        assertThat(actual.get(BUCKET_0), greaterThan(0.0));
        assertThat(actual.get(BUCKET_1), nullValue());
        assertThat(actual.get(BUCKET_2), nullValue());
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new WindowAggregatorFunctionSupplier(new DerivDoubleAggregatorFunctionSupplier(false), Duration.ofMinutes(5));
    }

    @Override
    protected String expectedToStringOfSimpleAggregator() {
        return "Window[agg=DerivDoubleGroupingAggregatorFunction[channels=[2, 3]], window=PT5M]";
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "Window[agg=deriv of doubles, window=PT5M]";
    }

    @Override
    protected int inputCount() {
        return 2;
    }
}
