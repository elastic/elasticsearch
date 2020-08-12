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
package org.elasticsearch.search.aggregations.metrics.datasketches.hll;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.ParsedAggregation;

import java.io.IOException;

public class ParsedHllSketchAggregation extends ParsedAggregation implements HllSketchAggregation {
    private long resultValue;
    private static final ObjectParser<ParsedHllSketchAggregation, Void> PARSER = new ObjectParser<>(
        ParsedHllSketchAggregation.class.getSimpleName(),
        true,
        ParsedHllSketchAggregation::new
    );

    /**
     * declare value
     */
    static {
        declareAggregationFields(PARSER);
        PARSER.declareLong((agg, value) -> { agg.resultValue = value; }, CommonFields.VALUE);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), this.resultValue);
        return builder;
    }

    /**
     * value of this aggregation
     * @return the value
     */
    @Override
    public long getValue() {
        return this.resultValue;
    }

    /**
     * value of this aggregation
     * @return the value
     */
    @Override
    public double value() {
        return (double) this.getValue();
    }

    /**
     * String value of this aggregation
     * @return the value as String
     */
    @Override
    public String getValueAsString() {
        return Double.toString((double) this.resultValue);
    }

    /**
     * type of this aggregation
     * @return the type
     */
    @Override
    public String getType() {
        return HllSketchAggregationBuilder.NAME;
    }

}
