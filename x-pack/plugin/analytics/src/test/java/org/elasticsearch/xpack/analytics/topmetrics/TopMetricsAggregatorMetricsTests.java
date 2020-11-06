/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.sort.SortValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.analytics.topmetrics.InternalTopMetrics.MetricValue;
import org.elasticsearch.xpack.analytics.topmetrics.InternalTopMetrics.TopMetric;
import org.elasticsearch.xpack.analytics.topmetrics.TopMetricsAggregator.MetricSource;
import org.elasticsearch.xpack.analytics.topmetrics.TopMetricsAggregator.Metrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notANumber;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TopMetricsAggregatorMetricsTests extends ESTestCase {
    public void testUnmapped() throws IOException {
        withMetric(null, (m, source) -> {
            // Load from doc is a noop
            m.loader(null).loadFromDoc(0, randomInt());
            assertNullMetric(m, source, randomInt());
        });
    }

    public void testEmptyLong() throws IOException {
        SortedNumericDocValues values = mock(SortedNumericDocValues.class);
        when(values.advanceExact(0)).thenReturn(false);
        withMetric(valuesSource(values), (m, source) -> {
            m.loader(null).loadFromDoc(0, 0);
            assertNullMetric(m, source, 0);
        });
    }

    public void testEmptyDouble() throws IOException {
        SortedNumericDoubleValues values = mock(SortedNumericDoubleValues.class);
        when(values.advanceExact(0)).thenReturn(false);
        withMetric(valuesSource(values), (m, source) -> {
            m.loader(null).loadFromDoc(0, 0);
            assertNullMetric(m, source, 0);
        });
    }

    public void testLoadLong() throws IOException {
        long value = randomLong();
        SortedNumericDocValues values = mock(SortedNumericDocValues.class);
        when(values.advanceExact(0)).thenReturn(true);
        when(values.docValueCount()).thenReturn(1);
        when(values.nextValue()).thenReturn(value);
        withMetric(valuesSource(values), (m, source) -> {
            m.loader(null).loadFromDoc(0, 0);
            assertMetricValue(m, 0, source, SortValue.from(value));
        });
    }

    public void testLoadDouble() throws IOException {
        double value = randomDouble();
        SortedNumericDoubleValues values = mock(SortedNumericDoubleValues.class);
        when(values.advanceExact(0)).thenReturn(true);
        when(values.docValueCount()).thenReturn(1);
        when(values.nextValue()).thenReturn(value);
        withMetric(valuesSource(values), (m, source) -> {
            m.loader(null).loadFromDoc(0, 0);
            assertMetricValue(m, 0, source, SortValue.from(value));
        });
    }

    public void testLoadAndSwapLong() throws IOException {
        long firstValue = randomLong();
        long secondValue = randomLong();
        SortedNumericDocValues values = mock(SortedNumericDocValues.class);
        when(values.advanceExact(0)).thenReturn(true);
        when(values.advanceExact(1)).thenReturn(true);
        when(values.docValueCount()).thenReturn(1);
        when(values.nextValue()).thenReturn(firstValue, secondValue);
        withMetric(valuesSource(values), (m, source) -> {
            assertLoadTwoAndSwap(m, source, SortValue.from(firstValue), SortValue.from(secondValue));
        });
    }

    public void testLoadAndSwapDouble() throws IOException {
        double firstValue = randomDouble();
        double secondValue = randomDouble();
        SortedNumericDoubleValues values = mock(SortedNumericDoubleValues.class);
        when(values.advanceExact(0)).thenReturn(true);
        when(values.advanceExact(1)).thenReturn(true);
        when(values.docValueCount()).thenReturn(1);
        when(values.nextValue()).thenReturn(firstValue, secondValue);
        withMetric(valuesSource(values), (m, source) -> {
            assertLoadTwoAndSwap(m, source, SortValue.from(firstValue), SortValue.from(secondValue));
        });
    }

    public void testManyValues() throws IOException {
        long[] values = IntStream.range(0, between(2, 100)).mapToLong(i -> randomLong()).toArray();
        List<ValuesSource.Numeric> valuesSources = Arrays.stream(values)
                .mapToObj(v -> {
                    try {
                        SortedNumericDocValues docValues = mock(SortedNumericDocValues.class);
                        when(docValues.advanceExact(0)).thenReturn(true);
                        when(docValues.docValueCount()).thenReturn(1);
                        when(docValues.nextValue()).thenReturn(v);
                        return valuesSource(docValues);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                })
                .collect(toList());
        withMetrics(valuesSources, (m, sources) -> {
            m.loader(null).loadFromDoc(0, 0);
            TopMetric metric = m.resultBuilder(DocValueFormat.RAW).build(0, SortValue.from(1));
            assertThat(metric.getMetricValues(), hasSize(values.length));
            for (int i = 0; i < values.length; i++) {
                MetricSource source = sources.get(i);
                assertThat(m.metric(source.getName(), 0), equalTo((double) values[i]));
                assertThat(metric.getMetricValues(),
                        hasItem(new MetricValue(source.getFormat(), SortValue.from(values[i]))));
            }
        });
    }

    private ValuesSource.Numeric valuesSource(SortedNumericDocValues values) throws IOException {
        ValuesSource.Numeric source = mock(ValuesSource.Numeric.class);
        when(source.isFloatingPoint()).thenReturn(false);
        when(source.longValues(null)).thenReturn(values);
        return source;
    }

    private ValuesSource.Numeric valuesSource(SortedNumericDoubleValues values) throws IOException {
        ValuesSource.Numeric source = mock(ValuesSource.Numeric.class);
        when(source.isFloatingPoint()).thenReturn(true);
        when(source.doubleValues(null)).thenReturn(values);
        return source;
    }

    private void withMetric(ValuesSource.Numeric valuesSource,
            CheckedBiConsumer<Metrics, MetricSource, IOException> consumer) throws IOException {
        withMetrics(singletonList(valuesSource), (m, sources) -> consumer.accept(m, sources.get(0)));
    }

    private void withMetrics(List<ValuesSource.Numeric> valuesSources,
            CheckedBiConsumer<Metrics, List<MetricSource>, IOException> consumer) throws IOException {
        Set<String> names = new HashSet<>();
        List<MetricSource> sources = new ArrayList<>(valuesSources.size());
        for (ValuesSource.Numeric valuesSource : valuesSources) {
            String name = randomValueOtherThanMany(names::contains, () -> randomAlphaOfLength(5));
            names.add(name);
            sources.add(new MetricSource(name, randomDocValueFormat(), valuesSource));
        }
        try (Metrics m = new Metrics(1, BigArrays.NON_RECYCLING_INSTANCE, sources)) {
            consumer.accept(m, sources);
        }
    }

    private void assertNullMetric(Metrics m, MetricSource source, long index) {
        DocValueFormat sortFormat = randomDocValueFormat();
        assertThat(m.metric(source.getName(), index), notANumber());
        TopMetric metric = m.resultBuilder(sortFormat).build(index, SortValue.from(1));
        assertThat(metric.getSortFormat(), sameInstance(sortFormat));
        assertThat(metric.getMetricValues(), equalTo(singletonList(null)));
    }

    private void assertMetricValue(Metrics m, long index, MetricSource source, SortValue value) {
        DocValueFormat sortFormat = randomDocValueFormat();
        assertThat(m.metric(source.getName(), index), equalTo(value.numberValue().doubleValue()));
        TopMetric metric = m.resultBuilder(sortFormat).build(index, SortValue.from(1));
        assertThat(metric.getSortValue(), equalTo(SortValue.from(1)));
        assertThat(metric.getSortFormat(), sameInstance(sortFormat));
        assertThat(metric.getMetricValues(), equalTo(singletonList(new MetricValue(source.getFormat(), value))));
    }

    private void assertLoadTwoAndSwap(Metrics m, MetricSource source, SortValue firstValue, SortValue secondValue) throws IOException {
        m.loader(null).loadFromDoc(0, 0);
        m.loader(null).loadFromDoc(1, 1);
        assertMetricValue(m, 0, source, firstValue);
        assertMetricValue(m, 1, source, secondValue);
        m.swap(0, 1);
        assertMetricValue(m, 0, source, secondValue);
        assertMetricValue(m, 1, source, firstValue);
        m.loader(null).loadFromDoc(2, 2); // 2 is empty
        assertNullMetric(m, source, 2);
        m.swap(0, 2);
        assertNullMetric(m, source, 0);
        assertMetricValue(m, 2, source, secondValue);
    }

    private DocValueFormat randomDocValueFormat() {
        return randomFrom(DocValueFormat.RAW, DocValueFormat.BINARY, DocValueFormat.BOOLEAN);
    }
}
