/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility class used for fetching field values by reading field data
 */
class FieldValueFetcher {

    private final MappedFieldType fieldType;
    private final IndexFieldData<?> fieldData;
    private final AbstractRollupFieldProducer rollupFieldProducer;

    protected FieldValueFetcher(MappedFieldType fieldType, IndexFieldData<?> fieldData) {
        this.fieldType = fieldType;
        this.fieldData = fieldData;
        this.rollupFieldProducer = createRollupFieldProducer();
    }

    public String name() {
        return fieldType().name();
    }

    public MappedFieldType fieldType() {
        return fieldType;
    }

    public FormattedDocValues getLeaf(LeafReaderContext context) {
        DocValueFormat format = fieldType.docValueFormat(null, null);
        return fieldData.load(context).getFormattedValues(format);
    }

    public AbstractRollupFieldProducer rollupFieldProducer() {
        return rollupFieldProducer;
    }

    private AbstractRollupFieldProducer createRollupFieldProducer() {
        if (fieldType.getMetricType() != null) {
            return switch (fieldType.getMetricType()) {
                case GAUGE -> new MetricFieldProducer.GaugeMetricFieldProducer(name());
                case COUNTER -> new MetricFieldProducer.CounterMetricFieldProducer(name());
            };
        } else {
            // If field is not a metric, we downsample it as a label
            if ("histogram".equals(fieldType.typeName())) {
                return new LabelFieldProducer.HistogramLastLabelFieldProducer(name());
            }
            return new LabelFieldProducer.LabelLastValueFieldProducer(name());
        }
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
