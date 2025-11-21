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
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper.AggregateMetricDoubleFieldType;

import java.util.ArrayList;
import java.util.List;

/**
 * This {@link FieldValueFetcher} is responsible for fetching the values of aggregate metric double fields.
 * For performance reasons, we load an aggregate metric double by loading its sub-metrics separately.
 */
public final class AggregateSubMetricFieldValueFetcher extends FieldValueFetcher {

    private final AggregateMetricDoubleFieldType aggMetricFieldType;

    private final AbstractDownsampleFieldProducer fieldProducer;

    AggregateSubMetricFieldValueFetcher(
        MappedFieldType fieldType,
        AggregateMetricDoubleFieldType aggMetricFieldType,
        IndexFieldData<?> fieldData,
        DownsampleConfig.SamplingMethod samplingMethod
    ) {
        super(fieldType.name(), fieldType, fieldData, samplingMethod);
        this.aggMetricFieldType = aggMetricFieldType;
        this.fieldProducer = createFieldProducer(samplingMethod);
    }

    @Override
    public AbstractDownsampleFieldProducer fieldProducer() {
        return fieldProducer;
    }

    /**
     * For aggregate_metric_double fields we create separate fetchers for each sub-metric. This is usually a downsample-of-downsample case.
     */
    static List<AggregateSubMetricFieldValueFetcher> create(
        SearchExecutionContext context,
        AggregateMetricDoubleFieldMapper.AggregateMetricDoubleFieldType aggMetricFieldType,
        DownsampleConfig.SamplingMethod samplingMethod
    ) {
        List<AggregateSubMetricFieldValueFetcher> fetchers = new ArrayList<>();
        // If the field is an aggregate_metric_double field, we should load all its subfields
        // This is usually a downsample-of-downsample case
        for (NumberFieldMapper.NumberFieldType metricSubField : aggMetricFieldType.getMetricFields().values()) {
            if (context.fieldExistsInIndex(metricSubField.name())) {
                IndexFieldData<?> fieldData = context.getForField(metricSubField, MappedFieldType.FielddataOperation.SEARCH);
                fetchers.add(new AggregateSubMetricFieldValueFetcher(metricSubField, aggMetricFieldType, fieldData, samplingMethod));
            }
        }
        return fetchers;
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
                return new MetricFieldProducer.AggregateSubMetricFieldProducer(aggMetricFieldType.name(), metric);
            } else {
                return LastValueFieldProducer.createForAggregateSubMetricMetric(aggMetricFieldType.name(), metric);
            }
        } else {
            // If a field is not a metric, we downsample it as a label
            return LastValueFieldProducer.createForAggregateSubMetricLabel(aggMetricFieldType.name(), metric);
        }
    }
}
