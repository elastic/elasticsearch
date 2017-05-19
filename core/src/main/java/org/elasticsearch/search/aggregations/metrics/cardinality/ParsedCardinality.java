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

package org.elasticsearch.search.aggregations.metrics.cardinality;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.ParsedAggregation;

import java.io.IOException;

public class ParsedCardinality extends ParsedAggregation implements Cardinality {

    private long cardinalityValue;

    @Override
    public String getValueAsString() {
        return Double.toString((double) cardinalityValue);
    }

    @Override
    public double value() {
        return getValue();
    }

    @Override
    public long getValue() {
        return cardinalityValue;
    }

    @Override
    public String getType() {
        return CardinalityAggregationBuilder.NAME;
    }

    private static final ObjectParser<ParsedCardinality, Void> PARSER = new ObjectParser<>(
            ParsedCardinality.class.getSimpleName(), true, ParsedCardinality::new);

    static {
        declareAggregationFields(PARSER);
        PARSER.declareLong((agg, value) -> agg.cardinalityValue = value, CommonFields.VALUE);
    }

    public static ParsedCardinality fromXContent(XContentParser parser, final String name) {
        ParsedCardinality cardinality = PARSER.apply(parser, null);
        cardinality.setName(name);
        return cardinality;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params)
            throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), cardinalityValue);
        return builder;
    }
}