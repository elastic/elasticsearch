/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationErrors;

import java.util.Locale;
import java.util.function.LongSupplier;

import static org.elasticsearch.search.aggregations.support.CoreValuesSourceType.GEOPOINT;

/**
 * Holds {@link ValuesSourceType} implementations for time series fields
 */
public enum TimeSeriesValuesSourceType implements ValuesSourceType {

    COUNTER {
        @Override
        public ValuesSource getEmpty() {
            throw new IllegalArgumentException("Cannot use unmapped counter field");
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            throw new IllegalArgumentException("Cannot use scripts for time-series counters");
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            if (script != null) {
                throw new IllegalArgumentException("Cannot use scripts for time-series counters");
            }
            if (fieldContext.indexFieldData() instanceof IndexNumericFieldData fieldData) {
                return new ValuesSource.Numeric.FieldData(fieldData);
            }
            throw new IllegalArgumentException(
                "Expected numeric type on field [" + fieldContext.field() + "], but got [" + fieldContext.fieldType().typeName() + "]"
            );
        }

        @Override
        public ValuesSource replaceMissing(
            ValuesSource valuesSource,
            Object rawMissing,
            DocValueFormat docValueFormat,
            LongSupplier nowInMillis
        ) {
            throw new IllegalArgumentException("Cannot replace missing values for time-series counters");
        }
    },
    POSITION {
        @Override
        public ValuesSource getEmpty() {
            return ValuesSource.GeoPoint.EMPTY;
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            throw AggregationErrors.valuesSourceDoesNotSupportScritps(this.value());
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            return GEOPOINT.getField(fieldContext, script);
        }

        @Override
        public ValuesSource replaceMissing(
            ValuesSource valuesSource,
            Object rawMissing,
            DocValueFormat docValueFormat,
            LongSupplier nowInMillis
        ) {
            return GEOPOINT.replaceMissing(valuesSource, rawMissing, docValueFormat, nowInMillis);
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
