/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Utility class used for fetching field values by reading field data
 */
class FieldValueFetcher {

    protected final MappedFieldType fieldType;
    protected final IndexFieldData<?> fieldData;
    private final AbstractRollupFieldProducer rollupFieldProducer;

    private static class RollupFieldProducersFactory {

        static AbstractRollupFieldProducer createProducer(final MappedFieldType fieldType, final String name) {
            if (fieldType.getMetricType() == null) {
                return createLabelFieldProducer(fieldType, name);
            }
            return createMetricFieldProducer(fieldType, name);
        }

        private static AbstractRollupFieldProducer createMetricFieldProducer(final MappedFieldType fieldType, final String name) {
            return switch (fieldType.getMetricType()) {
                case GAUGE -> new MetricFieldProducer.GaugeMetricFieldProducer(fieldType, name);
                case COUNTER -> new MetricFieldProducer.CounterMetricFieldProducer(fieldType, name);
            };
        }

        private static AbstractRollupFieldProducer createLabelFieldProducer(final MappedFieldType fieldType, final String name) {
            if (isHistogramField(fieldType)) {
                return new LabelFieldProducer.HistogramLabelFieldProducer(fieldType, name);
            }
            return new LabelFieldProducer.LabelLastValueFieldProducer(fieldType, name);
        }

        private static boolean isHistogramField(final MappedFieldType fieldType) {
            return "histogram".equals(fieldType.typeName().toLowerCase(Locale.ROOT));
        }
    }

    protected FieldValueFetcher(MappedFieldType fieldType, IndexFieldData<?> fieldData) {
        this.fieldType = fieldType;
        this.fieldData = fieldData;
        this.rollupFieldProducer = RollupFieldProducersFactory.createProducer(fieldType, name());
    }

    public String name() {
        return fieldType().name();
    }

    public MappedFieldType fieldType() {
        return fieldType;
    }

    public LeafFieldData getLeafFieldData(LeafReaderContext context) {
        return fieldData.load(context);
    }

    public AbstractRollupFieldProducer rollupFieldProducer() {
        return rollupFieldProducer;
    }

    /**
     * Retrieve field value fetchers for a list of fields.
     */
    static List<FieldValueFetcher> create(SearchExecutionContext context, String[] fields) {
        List<FieldValueFetcher> fetchers = new ArrayList<>();
        for (String field : fields) {
            MappedFieldType fieldType = context.getFieldType(field);
            assert fieldType != null : "Unknown field type for field: [" + field + "]";

            if (fieldType instanceof AggregateDoubleMetricFieldMapper.AggregateDoubleMetricFieldType aggMetricFieldType) {
                // If the field is an aggregate_metric_double field, we should load all its subfields
                // This is a rollup-of-rollup case
                for (NumberFieldMapper.NumberFieldType metricSubField : aggMetricFieldType.getMetricFields().values()) {
                    if (context.fieldExistsInIndex(metricSubField.name())) {
                        IndexFieldData<?> fieldData = context.getForField(metricSubField, MappedFieldType.FielddataOperation.SEARCH);
                        fetchers.add(new AggregateMetricFieldValueFetcher(metricSubField, aggMetricFieldType, fieldData));
                    }
                }
            } else {
                if (context.fieldExistsInIndex(field)) {
                    IndexFieldData<?> fieldData = context.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
                    fetchers.add(new FieldValueFetcher(fieldType, fieldData));
                }
            }
        }
        return Collections.unmodifiableList(fetchers);
    }
}
