/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper.AggregateMetricDoubleFieldType;

public final class AggregateMetricFieldValueFetcher extends FieldValueFetcher {

    private final AggregateMetricDoubleFieldType aggMetricFieldType;

    private final AbstractDownsampleFieldProducer fieldProducer;

    AggregateMetricFieldValueFetcher(
        MappedFieldType fieldType,
        AggregateMetricDoubleFieldType aggMetricFieldType,
        IndexFieldData<?> fieldData,
        DownsampleConfig.SamplingMethod samplingMethod
    ) {
        super(fieldType.name(), fieldType, fieldData, samplingMethod);
        this.aggMetricFieldType = aggMetricFieldType;
        this.fieldProducer = createFieldProducer(samplingMethod);
    }

    public AbstractDownsampleFieldProducer fieldProducer() {
        return fieldProducer;
    }

    private AbstractDownsampleFieldProducer createFieldProducer(DownsampleConfig.SamplingMethod samplingMethod) {
        AggregateMetricDoubleFieldMapper.Metric metric = null;
        for (var e : aggMetricFieldType.getMetricFields().entrySet()) {
            NumberFieldMapper.NumberFieldType metricSubField = e.getValue();
            if (metricSubField.name().equals(name())) {
                metric = e.getKey();
                break;
            }
        }
        assert metric != null : "Cannot resolve metric type for field " + name();

        if (aggMetricFieldType.getMetricType() != null) {
            if (samplingMethod != DownsampleConfig.SamplingMethod.LAST_VALUE) {
                // If the field is an aggregate_metric_double field, we should use the correct subfields
                // for each aggregation. This is a downsample-of-downsample case
                return new MetricFieldProducer.AggregatePreAggregatedMetricFieldProducer(aggMetricFieldType.name(), metric);
            } else {
                return new MetricFieldProducer.LastValueScalarMetricFieldProducer(aggMetricFieldType.name(), metric);
            }
        } else {
            // If field is not a metric, we downsample it as a label
            return new LabelFieldProducer.AggregateMetricFieldProducer.AggregateMetricFieldProducer(aggMetricFieldType.name(), metric);
        }
    }
}
