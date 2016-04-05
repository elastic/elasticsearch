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

package org.elasticsearch.search.aggregations.bucket.missing;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueSourceParser;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;

public class MissingAggregatorBuilder extends ValuesSourceAggregatorBuilder<ValuesSource, MissingAggregatorBuilder> {
    public MissingAggregatorBuilder(String name, ValueType targetValueType) {
        super(name, InternalMissing.TYPE, ValuesSourceType.ANY, targetValueType);
    }

    private MissingAggregatorBuilder(String name) {
        this(name, null);
    }

    /**
     * Read from a stream.
     */
    public MissingAggregatorBuilder(StreamInput in) throws IOException {
        super(in, InternalMissing.TYPE, ValuesSourceType.ANY, in.readOptionalWriteable(ValueType::readFromStream));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(getTargetValueType());
        super.writeTo(out);
    }

    @Override
    protected ValuesSourceAggregatorFactory<ValuesSource, ?> innerBuild(AggregationContext context,
            ValuesSourceConfig<ValuesSource> config, AggregatorFactory<?> parent, Builder subFactoriesBuilder) throws IOException {
        return new MissingAggregatorFactory(name, type, config, context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    public static final Aggregator.Parser PARSER = ValueSourceParser.build(MissingAggregatorBuilder::new, InternalMissing.TYPE);

    @Override
    protected int innerHashCode() {
        return 0;
    }

    @Override
    protected boolean innerEquals(Object obj) {
        return true;
    }
}