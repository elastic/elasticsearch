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

package org.elasticsearch.search.aggregations.metrics.valuecount;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;

public class ValueCountAggregationBuilder extends ValuesSourceAggregationBuilder.LeafOnly<ValuesSource, ValueCountAggregationBuilder> {
    public static final String NAME = InternalValueCount.TYPE.name();
    public static final ParseField AGGREGATION_NAME_FIELD = new ParseField(NAME);

    public ValueCountAggregationBuilder(String name, ValueType targetValueType) {
        super(name, InternalValueCount.TYPE, ValuesSourceType.ANY, targetValueType);
    }

    /**
     * Read from a stream.
     */
    public ValueCountAggregationBuilder(StreamInput in) throws IOException {
        super(in, InternalValueCount.TYPE, ValuesSourceType.ANY);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) {
        // Do nothing, no extra state to write to stream
    }

    @Override
    protected boolean serializeTargetValueType() {
        return true;
    }

    @Override
    protected ValueCountAggregatorFactory innerBuild(AggregationContext context, ValuesSourceConfig<ValuesSource> config,
            AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        return new ValueCountAggregatorFactory(name, type, config, context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    protected int innerHashCode() {
        return 0;
    }

    @Override
    protected boolean innerEquals(Object obj) {
        return true;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
