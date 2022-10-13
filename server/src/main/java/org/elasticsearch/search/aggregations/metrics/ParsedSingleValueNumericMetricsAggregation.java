/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;

public abstract class ParsedSingleValueNumericMetricsAggregation extends ParsedAggregation
    implements
        NumericMetricsAggregation.SingleValue {

    protected double value;
    protected String valueAsString;

    @Override
    public String getValueAsString() {
        if (valueAsString != null) {
            return valueAsString;
        } else {
            return Double.toString(value);
        }
    }

    @Override
    public double value() {
        return value;
    }

    protected void setValue(double value) {
        this.value = value;
    }

    protected void setValueAsString(String valueAsString) {
        this.valueAsString = valueAsString;
    }

    protected static void declareSingleValueFields(
        ObjectParser<? extends ParsedSingleValueNumericMetricsAggregation, Void> objectParser,
        double defaultNullValue
    ) {
        declareAggregationFields(objectParser);
        objectParser.declareField(
            ParsedSingleValueNumericMetricsAggregation::setValue,
            (parser, context) -> parseDouble(parser, defaultNullValue),
            CommonFields.VALUE,
            ValueType.DOUBLE_OR_NULL
        );
        objectParser.declareString(ParsedSingleValueNumericMetricsAggregation::setValueAsString, CommonFields.VALUE_AS_STRING);
    }
}
