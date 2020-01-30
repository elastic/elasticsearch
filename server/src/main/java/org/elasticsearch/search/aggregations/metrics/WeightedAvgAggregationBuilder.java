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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceParseHelper;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class WeightedAvgAggregationBuilder extends MultiValuesSourceAggregationBuilder.LeafOnly<Numeric, WeightedAvgAggregationBuilder> {
    public static final String NAME = "weighted_avg";
    public static final ParseField VALUE_FIELD = new ParseField("value");
    public static final ParseField WEIGHT_FIELD = new ParseField("weight");

    private static final ObjectParser<WeightedAvgAggregationBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(WeightedAvgAggregationBuilder.NAME);
        MultiValuesSourceParseHelper.declareCommon(PARSER, true, ValueType.NUMERIC);
        MultiValuesSourceParseHelper.declareField(VALUE_FIELD.getPreferredName(), PARSER, true, false);
        MultiValuesSourceParseHelper.declareField(WEIGHT_FIELD.getPreferredName(), PARSER, true, false);
    }

    public static AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new WeightedAvgAggregationBuilder(aggregationName), null);
    }

    public WeightedAvgAggregationBuilder(String name) {
        super(name, ValueType.NUMERIC);
    }

    public WeightedAvgAggregationBuilder(WeightedAvgAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
    }

    public WeightedAvgAggregationBuilder value(MultiValuesSourceFieldConfig valueConfig) {
        valueConfig = Objects.requireNonNull(valueConfig, "Configuration for field [" + VALUE_FIELD + "] cannot be null");
        field(VALUE_FIELD.getPreferredName(), valueConfig);
        return this;
    }

    public WeightedAvgAggregationBuilder weight(MultiValuesSourceFieldConfig weightConfig) {
        weightConfig = Objects.requireNonNull(weightConfig, "Configuration for field [" + WEIGHT_FIELD + "] cannot be null");
        field(WEIGHT_FIELD.getPreferredName(), weightConfig);
        return this;
    }

    /**
     * Read from a stream.
     */
    public WeightedAvgAggregationBuilder(StreamInput in) throws IOException {
        super(in, ValueType.NUMERIC);
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metaData) {
        return new WeightedAvgAggregationBuilder(this, factoriesBuilder, metaData);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) {
        // Do nothing, no extra state to write to stream
    }

    @Override
    protected MultiValuesSourceAggregatorFactory<Numeric> innerBuild(QueryShardContext queryShardContext,
                                                                     Map<String, ValuesSourceConfig<Numeric>> configs,
                                                                     DocValueFormat format,
                                                                     AggregatorFactory parent,
                                                                     Builder subFactoriesBuilder) throws IOException {
        return new WeightedAvgAggregatorFactory(name, configs, format, queryShardContext, parent, subFactoriesBuilder, metaData);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, ToXContent.Params params) throws IOException {
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }
}
