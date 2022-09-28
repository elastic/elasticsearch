/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries;

import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.util.Locale;

public enum TimeSeriesValuesSourceType implements ValuesSourceType {
    COUNTER {
        @Override
        public ValuesSource getEmpty() {
            throw new AggregationExecutionException("Cannot use unmapped counter field");
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            throw new AggregationExecutionException("Cannot use scripts for time-series counters");
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script, AggregationContext context) {
            if (script != null) {
                throw new AggregationExecutionException("Cannot use scripts for time-series counters");
            }
            if (fieldContext.indexFieldData()instanceof IndexNumericFieldData fieldData) {
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
            throw new AggregationExecutionException("Cannot replace missing values for time-series counters");
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
