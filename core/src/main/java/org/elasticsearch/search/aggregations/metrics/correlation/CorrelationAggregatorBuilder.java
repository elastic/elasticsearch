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
package org.elasticsearch.search.aggregations.metrics.correlation;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.*;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;

import java.io.IOException;
import java.util.Map;

/**
 */
public class CorrelationAggregatorBuilder
    extends MultiValuesSourceAggregatorBuilder.LeafOnly<ValuesSource.Numeric, CorrelationAggregatorBuilder> {

    static final CorrelationAggregatorBuilder PROTOTYPE = new CorrelationAggregatorBuilder("");

    public CorrelationAggregatorBuilder(String name) {
        super(name, InternalCorrelation.TYPE, ValuesSourceType.NUMERIC, ValueType.NUMERIC);
    }

    @Override
    protected CorrelationAggregatorFactory innerBuild(AggregationContext context, Map<String, ValuesSourceConfig<Numeric>> configs,
                                                      AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        return new CorrelationAggregatorFactory(name, type, configs, context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    protected MultiValuesSourceAggregatorBuilder<Numeric, CorrelationAggregatorBuilder> innerReadFrom(String name,
                                                                                                      ValuesSourceType valuesSourceType, ValueType targetValueType, StreamInput in) {
        return new CorrelationAggregatorBuilder(name);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) {
        // Do nothing, no extra state to write to stream
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, ToXContent.Params params) throws IOException {
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
}
