/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.SortValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.analytics.topmetrics.InternalTopMetrics.MetricValue;
import org.elasticsearch.xpack.analytics.topmetrics.InternalTopMetrics.TopMetric;
import org.elasticsearch.xpack.analytics.topmetrics.TopMetricsAggregator.MetricValues;
import org.elasticsearch.xpack.analytics.topmetrics.TopMetricsAggregator.Metrics;

import java.io.IOException;
import java.time.ZoneOffset;
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
    private static final ValuesSourceRegistry REGISTRY;
    static {
        ValuesSourceRegistry.Builder registry = new ValuesSourceRegistry.Builder();
        TopMetricsAggregationBuilder.registerAggregators(registry);
        REGISTRY = registry.build();
    }

    public void testNoNumbers() throws IOException {
        assertNoValues(toConfig(null, CoreValuesSourceType.NUMERIC, DocValueFormat.RAW, false));
    }

    public void testNoDates() throws IOException {
        assertNoValues(toConfig(null, CoreValuesSourceType.DATE, DocValueFormat.RAW, false));
    }

    public void testNoStrings() throws IOException {
        assertNoValues(toConfig(null, CoreValuesSourceType.KEYWORD, DocValueFormat.RAW, false));
    }

    private void assertNoValues(ValuesSourceConfig config) throws IOException {
        withMetric(config, m -> {
            // Load from doc is a noop
            m.loader(null).loadFromDoc(0, randomInt());
            assertNullMetric(m, config, randomInt(), true);
        });
    }

    public void testEmptyLong() throws IOException {
        SortedNumericDocValues values = mock(SortedNumericDocValues.class);
        when(values.advanceExact(0)).thenReturn(false);
        ValuesSourceConfig config = toConfig(values);
        withMetric(config, m -> {
            m.loader(null).loadFromDoc(0, 0);
            assertNullMetric(m, config, 0, true);
        });
    }

    public void testEmptyDouble() throws IOException {
        SortedNumericDoubleValues values = mock(SortedNumericDoubleValues.class);
        when(values.advanceExact(0)).thenReturn(false);
        ValuesSourceConfig config = toConfig(values);
        withMetric(config, m -> {
            m.loader(null).loadFromDoc(0, 0);
            assertNullMetric(m, config, 0, true);
        });
    }

    public void testLoadLong() throws IOException {
        long value = randomLong();
        SortedNumericDocValues values = mock(SortedNumericDocValues.class);
        when(values.advanceExact(0)).thenReturn(true);
        when(values.docValueCount()).thenReturn(1);
        when(values.nextValue()).thenReturn(value);
        ValuesSourceConfig config = toConfig(values);
        withMetric(config, m -> {
            m.loader(null).loadFromDoc(0, 0);
            assertMetricValue(m, 0, config, SortValue.from(value), true);
        });
    }

    public void testLoadDouble() throws IOException {
        double value = randomDouble();
        SortedNumericDoubleValues values = mock(SortedNumericDoubleValues.class);
        when(values.advanceExact(0)).thenReturn(true);
        when(values.docValueCount()).thenReturn(1);
        when(values.nextValue()).thenReturn(value);
        ValuesSourceConfig config = toConfig(values);
        withMetric(config, m -> {
            m.loader(null).loadFromDoc(0, 0);
            assertMetricValue(m, 0, config, SortValue.from(value), true);
        });
    }

    public void testLoadString() throws IOException {
        BytesRef value = new BytesRef(randomAlphaOfLength(5));
        SortedSetDocValues values = mock(SortedSetDocValues.class);
        when(values.advanceExact(0)).thenReturn(true);
        when(values.getValueCount()).thenReturn(1L);
        when(values.nextOrd()).thenReturn(0L);
        when(values.lookupOrd(0L)).thenReturn(value);
        ValuesSourceConfig config = toConfig(values);
        withMetric(config, m -> {
            m.loader(null).loadFromDoc(0, 0);
            assertMetricValue(m, 0, config, SortValue.from(value), false);
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
        ValuesSourceConfig config = toConfig(values);
        withMetric(config, m -> assertLoadTwoAndSwap(m, config, SortValue.from(firstValue), SortValue.from(secondValue), true));
    }

    public void testLoadAndSwapDouble() throws IOException {
        double firstValue = randomDouble();
        double secondValue = randomDouble();
        SortedNumericDoubleValues values = mock(SortedNumericDoubleValues.class);
        when(values.advanceExact(0)).thenReturn(true);
        when(values.advanceExact(1)).thenReturn(true);
        when(values.docValueCount()).thenReturn(1);
        when(values.nextValue()).thenReturn(firstValue, secondValue);
        ValuesSourceConfig config = toConfig(values);
        withMetric(config, m -> assertLoadTwoAndSwap(m, config, SortValue.from(firstValue), SortValue.from(secondValue), true));
    }

    public void testLoadAndSwapString() throws IOException {
        BytesRef firstValue = new BytesRef(randomAlphaOfLength(5));
        BytesRef secondValue = new BytesRef(randomAlphaOfLength(5));
        SortedSetDocValues values = mock(SortedSetDocValues.class);
        when(values.advanceExact(0)).thenReturn(true);
        when(values.advanceExact(1)).thenReturn(true);
        when(values.getValueCount()).thenReturn(1L);
        when(values.nextOrd()).thenReturn(0L, 1L);
        when(values.lookupOrd(0L)).thenReturn(firstValue);
        when(values.lookupOrd(1L)).thenReturn(secondValue);
        ValuesSourceConfig config = toConfig(values);
        withMetric(config, m -> assertLoadTwoAndSwap(m, config, SortValue.from(firstValue), SortValue.from(secondValue), false));
    }

    public void testManyValues() throws IOException {
        long[] values = IntStream.range(0, between(2, 100)).mapToLong(i -> randomLong()).toArray();
        List<ValuesSourceConfig> configs = Arrays.stream(values).mapToObj(v -> {
            try {
                SortedNumericDocValues docValues = mock(SortedNumericDocValues.class);
                when(docValues.advanceExact(0)).thenReturn(true);
                when(docValues.docValueCount()).thenReturn(1);
                when(docValues.nextValue()).thenReturn(v);
                return toConfig(docValues);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }).collect(toList());
        withMetrics(configs, m -> {
            m.loader(null).loadFromDoc(0, 0);
            TopMetric metric = m.resultBuilder(DocValueFormat.RAW).build(0, SortValue.from(1));
            assertThat(metric.getMetricValues(), hasSize(values.length));
            for (int i = 0; i < values.length; i++) {
                ValuesSourceConfig config = configs.get(i);
                assertThat(m.metric(config.fieldContext().field(), 0), equalTo((double) values[i]));
                assertThat(metric.getMetricValues(), hasItem(new MetricValue(config.format(), SortValue.from(values[i]))));
            }
        });
    }

    private ValuesSourceConfig toConfig(SortedNumericDocValues values) throws IOException {
        ValuesSource.Numeric source = mock(ValuesSource.Numeric.class);
        when(source.isFloatingPoint()).thenReturn(false);
        when(source.longValues(null)).thenReturn(values);
        if (randomBoolean()) {
            return toConfig(source, CoreValuesSourceType.NUMERIC, randomWholeNumberDocValuesFormat(), true);
        }
        DocValueFormat dateFormatter = new DocValueFormat.DateTime(
            DateFormatter.forPattern(randomDateFormatterPattern()),
            ZoneOffset.UTC,
            DateFieldMapper.Resolution.MILLISECONDS
        );
        return toConfig(source, CoreValuesSourceType.DATE, randomFrom(DocValueFormat.RAW, dateFormatter), true);
    }

    private ValuesSourceConfig toConfig(SortedNumericDoubleValues values) throws IOException {
        ValuesSource.Numeric source = mock(ValuesSource.Numeric.class);
        when(source.isFloatingPoint()).thenReturn(true);
        when(source.doubleValues(null)).thenReturn(values);
        return toConfig(
            source,
            CoreValuesSourceType.NUMERIC,
            randomFrom(DocValueFormat.RAW, new DocValueFormat.Decimal("####.####")),
            true
        );
    }

    private ValuesSourceConfig toConfig(SortedSetDocValues values) throws IOException {
        ValuesSource.Bytes.WithOrdinals source = mock(ValuesSource.Bytes.WithOrdinals.class);
        when(source.ordinalsValues(null)).thenReturn(values);
        ValuesSourceConfig config = toConfig(source, CoreValuesSourceType.KEYWORD, DocValueFormat.RAW, true);
        when(config.hasOrdinals()).thenReturn(true);
        return config;
    }

    private final Set<String> names = new HashSet<>();

    private ValuesSourceConfig toConfig(ValuesSource source, ValuesSourceType type, DocValueFormat format, boolean hasValues) {
        String name = randomValueOtherThanMany(names::contains, () -> randomAlphaOfLength(5));
        names.add(name);
        ValuesSourceConfig config = mock(ValuesSourceConfig.class);
        when(config.fieldContext()).thenReturn(new FieldContext(name, null, null));
        when(config.valueSourceType()).thenReturn(type);
        when(config.format()).thenReturn(format);
        when(config.getValuesSource()).thenReturn(source);
        when(config.hasValues()).thenReturn(hasValues);
        return config;
    }

    private void withMetric(ValuesSourceConfig config, CheckedConsumer<Metrics, IOException> consumer) throws IOException {
        withMetrics(singletonList(config), consumer);
    }

    private void withMetrics(List<ValuesSourceConfig> configs, CheckedConsumer<Metrics, IOException> consumer) throws IOException {
        MetricValues[] values = new MetricValues[configs.size()];
        for (int i = 0; i < configs.size(); i++) {
            values[i] = TopMetricsAggregator.buildMetricValues(
                REGISTRY,
                BigArrays.NON_RECYCLING_INSTANCE,
                1,
                configs.get(i).fieldContext().field(),
                configs.get(i)
            );
        }
        try (Metrics m = new Metrics(values)) {
            consumer.accept(m);
        }
    }

    private void assertNullMetric(Metrics m, ValuesSourceConfig config, long index, boolean assertSortValue) throws IOException {
        if (assertSortValue) {
            assertThat(m.metric(config.fieldContext().field(), index), notANumber());
        }
        DocValueFormat sortFormat = randomWholeNumberDocValuesFormat();
        TopMetric metric = m.resultBuilder(sortFormat).build(index, SortValue.from(1));
        assertThat(metric.getSortFormat(), sameInstance(sortFormat));
        assertThat(metric.getMetricValues(), equalTo(singletonList(null)));
    }

    private void assertMetricValue(Metrics m, long index, ValuesSourceConfig config, SortValue value, boolean assertSortValue)
        throws IOException {
        if (assertSortValue) {
            assertThat(m.metric(config.fieldContext().field(), index), equalTo(value.numberValue().doubleValue()));
        }
        DocValueFormat sortFormat = randomWholeNumberDocValuesFormat();
        TopMetric metric = m.resultBuilder(sortFormat).build(index, SortValue.from(1));
        assertThat(metric.getSortValue(), equalTo(SortValue.from(1)));
        assertThat(metric.getSortFormat(), sameInstance(sortFormat));
        assertThat(metric.getMetricValues(), equalTo(singletonList(new MetricValue(config.format(), value))));
    }

    private void assertLoadTwoAndSwap(
        Metrics m,
        ValuesSourceConfig config,
        SortValue firstValue,
        SortValue secondValue,
        boolean assertSortValue
    ) throws IOException {
        m.loader(null).loadFromDoc(0, 0);
        m.loader(null).loadFromDoc(1, 1);
        assertMetricValue(m, 0, config, firstValue, assertSortValue);
        assertMetricValue(m, 1, config, secondValue, assertSortValue);
        m.swap(0, 1);
        assertMetricValue(m, 0, config, secondValue, assertSortValue);
        assertMetricValue(m, 1, config, firstValue, assertSortValue);
        m.loader(null).loadFromDoc(2, 2); // 2 is empty
        assertNullMetric(m, config, 2, assertSortValue);
        m.swap(0, 2);
        assertNullMetric(m, config, 0, assertSortValue);
        assertMetricValue(m, 2, config, secondValue, assertSortValue);
    }

    private DocValueFormat randomWholeNumberDocValuesFormat() {
        return randomFrom(DocValueFormat.RAW, new DocValueFormat.Decimal("####"));
    }
}
