/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.aggregatemetric.aggregations.support;

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.UnsupportedAggregationOnDownsampledIndex;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.aggregatemetric.fielddata.IndexAggregateDoubleMetricFieldData;

import java.util.Locale;

public enum AggregateMetricsValuesSourceType implements ValuesSourceType {

    AGGREGATE_METRIC() {

        @Override
        public RuntimeException getUnregisteredException(String message) {
            return new UnsupportedAggregationOnDownsampledIndex(message);
        }

        @Override
        public ValuesSource getEmpty() {
            throw new IllegalArgumentException("Can't deal with unmapped AggregateMetricsValuesSource type " + this.value());
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            throw new AggregationExecutionException("Value source of type [" + this.value() + "] is not supported by scripts");
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            final IndexFieldData<?> indexFieldData = fieldContext.indexFieldData();

            if ((indexFieldData instanceof IndexAggregateDoubleMetricFieldData) == false) {
                throw new IllegalArgumentException(
                    "Expected aggregate_metric_double type on field ["
                        + fieldContext.field()
                        + "], but got ["
                        + fieldContext.fieldType().typeName()
                        + "]"
                );
            }
            return new AggregateMetricsValuesSource.AggregateDoubleMetric.Fielddata((IndexAggregateDoubleMetricFieldData) indexFieldData);
        }

        @Override
        public ValuesSource replaceMissing(
            ValuesSource valuesSource,
            Object rawMissing,
            DocValueFormat docValueFormat,
            AggregationContext context
        ) {
            throw new IllegalArgumentException("Can't apply missing values on a " + valuesSource.getClass());
        }
    };

    @Override
    public String typeName() {
        return value();
    }

    public static ValuesSourceType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }
}
