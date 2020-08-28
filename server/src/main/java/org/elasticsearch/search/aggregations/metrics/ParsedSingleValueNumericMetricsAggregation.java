/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.search.aggregations.ParsedAggregation;

public abstract class ParsedSingleValueNumericMetricsAggregation extends ParsedAggregation
        implements NumericMetricsAggregation.SingleValue {

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

    protected static void declareSingleValueFields(ObjectParser<? extends ParsedSingleValueNumericMetricsAggregation, Void> objectParser,
            double defaultNullValue) {
        declareAggregationFields(objectParser);
        objectParser.declareField(ParsedSingleValueNumericMetricsAggregation::setValue,
                (parser, context) -> parseDouble(parser, defaultNullValue), CommonFields.VALUE, ValueType.DOUBLE_OR_NULL);
        objectParser.declareString(ParsedSingleValueNumericMetricsAggregation::setValueAsString, CommonFields.VALUE_AS_STRING);
    }
}
