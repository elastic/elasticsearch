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

package org.elasticsearch.search.aggregations.pipeline.derivative;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.pipeline.ParsedSimpleValue;

import java.io.IOException;

public class ParsedDerivative extends ParsedSimpleValue implements Derivative {

    private double normalizedValue;
    private String normalizedAsString;
    private boolean hasNormalizationFactor;
    private static final ParseField NORMALIZED_AS_STRING = new ParseField("normalized_value_as_string");
    private static final ParseField NORMALIZED = new ParseField("normalized_value");

    @Override
    public double normalizedValue() {
        return this.normalizedValue;
    }

    @Override
    public String getType() {
        return DerivativePipelineAggregationBuilder.NAME;
    }

    private static final ObjectParser<ParsedDerivative, Void> PARSER = new ObjectParser<>(ParsedDerivative.class.getSimpleName(), true,
            ParsedDerivative::new);

    static {
        declareSingleValueFields(PARSER, Double.NaN);
        PARSER.declareField((agg, normalized) -> {
            agg.normalizedValue = normalized;
            agg.hasNormalizationFactor = true;
        }, (parser, context) -> parseDouble(parser, Double.NaN), NORMALIZED, ValueType.DOUBLE_OR_NULL);
        PARSER.declareString((agg, normalAsString) -> agg.normalizedAsString = normalAsString, NORMALIZED_AS_STRING);
    }

    public static ParsedDerivative fromXContent(XContentParser parser, final String name) {
        ParsedDerivative derivative = PARSER.apply(parser, null);
        derivative.setName(name);
        return derivative;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        super.doXContentBody(builder, params);
        if (hasNormalizationFactor) {
            boolean hasValue = Double.isNaN(normalizedValue) == false;
            builder.field(NORMALIZED.getPreferredName(), hasValue ? normalizedValue : null);
            if (hasValue && normalizedAsString != null) {
                builder.field(NORMALIZED_AS_STRING.getPreferredName(), normalizedAsString);
            }
        }
        return builder;
    }
}