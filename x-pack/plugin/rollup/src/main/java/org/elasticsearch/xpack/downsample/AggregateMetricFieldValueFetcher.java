/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper.AggregateDoubleMetricFieldType;

public class AggregateMetricFieldValueFetcher extends FieldValueFetcher {

    private final AggregateDoubleMetricFieldType aggMetricFieldType;

    private final AbstractDownsampleFieldProducer rollupFieldProducer;

    protected AggregateMetricFieldValueFetcher(
        MappedFieldType fieldType,
        AggregateDoubleMetricFieldType aggMetricFieldType,
        IndexFieldData<?> fieldData
    ) {
        super(fieldType.name(), fieldType, fieldData);
        this.aggMetricFieldType = aggMetricFieldType;
        this.rollupFieldProducer = createRollupFieldProducer();
    }

    public AbstractDownsampleFieldProducer rollupFieldProducer() {
        return rollupFieldProducer;
    }

    private AbstractDownsampleFieldProducer createRollupFieldProducer() {
        AggregateDoubleMetricFieldMapper.Metric metric = null;
        for (var e : aggMetricFieldType.getMetricFields().entrySet()) {
            NumberFieldMapper.NumberFieldType metricSubField = e.getValue();
            if (metricSubField.name().equals(name())) {
                metric = e.getKey();
                break;
            }
        }
        assert metric != null : "Cannot resolve metric type for field " + name();

        if (aggMetricFieldType.getMetricType() != null) {
            // If the field is an aggregate_metric_double field, we should use the correct subfields
            // for each aggregation. This is a rollup-of-rollup case
            MetricFieldProducer.Metric metricOperation = switch (metric) {
                case max -> new MetricFieldProducer.Max();
                case min -> new MetricFieldProducer.Min();
                case sum -> new MetricFieldProducer.Sum();
                // To compute value_count summary, we must sum all field values
                case value_count -> new MetricFieldProducer.Sum(AggregateDoubleMetricFieldMapper.Metric.value_count.name());
            };
            return new MetricFieldProducer.GaugeMetricFieldProducer(aggMetricFieldType.name(), metricOperation);
        } else {
            // If field is not a metric, we downsample it as a label
            return new LabelFieldProducer.AggregateMetricFieldProducer.AggregateMetricFieldProducer(aggMetricFieldType.name(), metric);
        }
    }
}
