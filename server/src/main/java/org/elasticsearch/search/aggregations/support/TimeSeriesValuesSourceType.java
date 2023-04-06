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

import java.util.Locale;

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
            AggregationContext context
        ) {
            throw new IllegalArgumentException("Cannot replace missing values for time-series counters");
        }
    };

    public static ValuesSourceType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }

    @Override
    public String typeName() {
        return value();
    }
}
