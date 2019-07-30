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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParserHelper;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

public class SumAggregationBuilder extends ValuesSourceAggregationBuilder.LeafOnly<ValuesSource.Numeric, SumAggregationBuilder> {
    public static final String NAME = "sum";

    private static final ObjectParser<SumAggregationBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(SumAggregationBuilder.NAME);
        ValuesSourceParserHelper.declareNumericFields(PARSER, true, true, false);
    }

    public static AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new SumAggregationBuilder(aggregationName), null);
    }

    public SumAggregationBuilder(String name) {
        super(name, ValuesSourceType.NUMERIC, ValueType.NUMERIC);
    }

    protected SumAggregationBuilder(SumAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metaData) {
        return new SumAggregationBuilder(this, factoriesBuilder, metaData);
    }

    /**
     * Read from a stream.
     */
    public SumAggregationBuilder(StreamInput in) throws IOException {
        super(in, ValuesSourceType.NUMERIC, ValueType.NUMERIC);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) {
        // Do nothing, no extra state to write to stream
    }

    @Override
    protected SumAggregatorFactory innerBuild(SearchContext context, ValuesSourceConfig<Numeric> config,
            AggregatorFactory parent, Builder subFactoriesBuilder) throws IOException {
        return new SumAggregatorFactory(name, config, context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }
}
