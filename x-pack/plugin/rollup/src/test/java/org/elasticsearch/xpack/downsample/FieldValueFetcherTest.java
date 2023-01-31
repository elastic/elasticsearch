/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.xpack.analytics.mapper.HistogramFieldMapper;

import java.util.Collections;

public class FieldValueFetcherTest extends AggregatorTestCase {

    public void testCounterMetricFieldValueFetcher() {
        final MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
            "number",
            NumberFieldMapper.NumberType.LONG,
            false,
            false,
            true,
            false,
            0,
            Collections.emptyMap(),
            null,
            false,
            TimeSeriesParams.MetricType.COUNTER
        );
        final IndexFieldData<?> fieldData = fieldType.fielddataBuilder(FieldDataContext.noRuntimeFields("test")).build(null, null);
        final FieldValueFetcher fetcher = new FieldValueFetcher(fieldType, fieldData);
        assertTrue(fetcher.rollupFieldProducer() instanceof MetricFieldProducer.CounterMetricFieldProducer);
        assertEquals("number", fetcher.name());
    }

    public void testGaugeMetricFieldValueFetcher() {
        final MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
            "number",
            NumberFieldMapper.NumberType.LONG,
            false,
            false,
            true,
            false,
            0,
            Collections.emptyMap(),
            null,
            false,
            TimeSeriesParams.MetricType.GAUGE
        );
        final IndexFieldData<?> fieldData = fieldType.fielddataBuilder(FieldDataContext.noRuntimeFields("test")).build(null, null);
        final FieldValueFetcher fetcher = new FieldValueFetcher(fieldType, fieldData);
        assertTrue(fetcher.rollupFieldProducer() instanceof MetricFieldProducer.GaugeMetricFieldProducer);
        assertEquals("number", fetcher.name());
    }

    public void testNumericLabelFieldValueFetcher() {
        final MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
            "number",
            NumberFieldMapper.NumberType.LONG,
            false,
            false,
            true,
            false,
            0,
            Collections.emptyMap(),
            null,
            false,
            null
        );
        final IndexFieldData<?> fieldData = fieldType.fielddataBuilder(FieldDataContext.noRuntimeFields("test")).build(null, null);
        final FieldValueFetcher fetcher = new FieldValueFetcher(fieldType, fieldData);
        assertTrue(fetcher.rollupFieldProducer() instanceof LabelFieldProducer.LabelLastValueFieldProducer);
        assertEquals("number", fetcher.name());
    }

    public void testHistogramLabelFieldValueFetcher() {
        final MappedFieldType fieldType = new HistogramFieldMapper.HistogramFieldType("histogram", Collections.emptyMap());
        final IndexFieldData<?> fieldData = fieldType.fielddataBuilder(FieldDataContext.noRuntimeFields("test")).build(null, null);
        final FieldValueFetcher fetcher = new FieldValueFetcher(fieldType, fieldData);
        assertTrue(fetcher.rollupFieldProducer() instanceof LabelFieldProducer.HistogramLabelFieldProducer);
        assertEquals("histogram", fetcher.name());
    }
}
