/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.aggregations.support;

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexHistogramFieldData;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationErrors;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.util.Locale;
import java.util.function.LongSupplier;

public enum AnalyticsValuesSourceType implements ValuesSourceType {
    HISTOGRAM() {
        @Override
        public ValuesSource getEmpty() {
            // TODO: Is this the correct exception type here?
            throw new IllegalArgumentException("Can't deal with unmapped HistogramValuesSource type " + this.value());
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            throw AggregationErrors.valuesSourceDoesNotSupportScritps(this.value());
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            final IndexFieldData<?> indexFieldData = fieldContext.indexFieldData();

            if ((indexFieldData instanceof IndexHistogramFieldData) == false) {
                throw new IllegalArgumentException(
                    "Expected histogram type on field [" + fieldContext.field() + "], but got [" + fieldContext.fieldType().typeName() + "]"
                );
            }
            return new HistogramValuesSource.Histogram.Fielddata((IndexHistogramFieldData) indexFieldData);
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

    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }

    @Override
    public String typeName() {
        return value();
    }
}
