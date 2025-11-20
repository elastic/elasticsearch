/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.exponentialhistogram.aggregations.support;

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationErrors;
import org.elasticsearch.search.aggregations.UnsupportedAggregationOnDownsampledIndex;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.exponentialhistogram.fielddata.IndexExponentialHistogramFieldData;

import java.util.Locale;
import java.util.function.LongSupplier;

import static org.elasticsearch.xpack.exponentialhistogram.ExponentialHistogramFieldMapper.CONTENT_TYPE;

public enum ExponentialHistogramValuesSourceType implements ValuesSourceType {

    EXPONENTIAL_HISTOGRAM() {

        @Override
        public RuntimeException getUnregisteredException(String message) {
            return new UnsupportedAggregationOnDownsampledIndex(message);
        }

        @Override
        public ValuesSource getEmpty() {
            throw new IllegalArgumentException("Can't deal with unmapped ExponentialHistogram type " + this.value());
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            throw AggregationErrors.valuesSourceDoesNotSupportScritps(this.value());
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            final IndexFieldData<?> indexFieldData = fieldContext.indexFieldData();

            if ((indexFieldData instanceof IndexExponentialHistogramFieldData) == false) {
                throw new IllegalArgumentException(
                    "Expected "
                        + CONTENT_TYPE
                        + " type on field ["
                        + fieldContext.field()
                        + "], but got ["
                        + fieldContext.fieldType().typeName()
                        + "]"
                );
            }
            return new ExponentialHistogramValuesSource.ExponentialHistogram.Fielddata((IndexExponentialHistogramFieldData) indexFieldData);
        }

        @Override
        public ValuesSource replaceMissing(
            ValuesSource valuesSource,
            Object rawMissing,
            DocValueFormat docValueFormat,
            LongSupplier nowInMillis
        ) {
            throw new IllegalArgumentException("Can't apply missing values on a " + valuesSource.getClass());
        }
    };

    @Override
    public String typeName() {
        return value();
    }

    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }
}
