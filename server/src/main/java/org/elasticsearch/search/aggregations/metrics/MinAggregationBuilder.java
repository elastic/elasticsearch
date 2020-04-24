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
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;

public class MinAggregationBuilder extends ValuesSourceAggregationBuilder.LeafOnly<ValuesSource.Numeric, MinAggregationBuilder> {
    public static final String NAME = "min";

    public static final ObjectParser<MinAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(NAME, MinAggregationBuilder::new);
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);
    }

    public MinAggregationBuilder(String name) {
        super(name);
    }

    protected MinAggregationBuilder(MinAggregationBuilder clone,
                                    AggregatorFactories.Builder factoriesBuilder,
                                    Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new MinAggregationBuilder(this, factoriesBuilder, metadata);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        MinAggregatorFactory.registerAggregators(builder);
    }

    /**
     * Read from a stream.
     */
    public MinAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) {
        // Do nothing, no extra state to write to stream
    }

    @Override
    protected MinAggregatorFactory innerBuild(QueryShardContext queryShardContext, ValuesSourceConfig config,
                                              AggregatorFactory parent,
                                              AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        return new MinAggregatorFactory(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
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
