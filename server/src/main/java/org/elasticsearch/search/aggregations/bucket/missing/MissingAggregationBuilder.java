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

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;

public class MissingAggregationBuilder extends ValuesSourceAggregationBuilder<MissingAggregationBuilder> {
    public static final String NAME = "missing";
    public static final ValuesSourceRegistry.RegistryKey<MissingAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        MissingAggregatorSupplier.class
    );

    public static final ObjectParser<MissingAggregationBuilder, String> PARSER =
            ObjectParser.fromBuilder(NAME, MissingAggregationBuilder::new);
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        MissingAggregatorFactory.registerAggregators(builder);
    }

    public MissingAggregationBuilder(String name) {
        super(name);
    }

    protected MissingAggregationBuilder(MissingAggregationBuilder clone,
                                        AggregatorFactories.Builder factoriesBuilder,
                                        Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new MissingAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public MissingAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) {
        // Do nothing, no extra state to write to stream
    }

    @Override
    protected boolean serializeTargetValueType(Version version) {
        return true;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.ONE;
    }

    protected ValuesSourceAggregatorFactory innerBuild(QueryShardContext queryShardContext,
                                                       ValuesSourceConfig config,
                                                       AggregatorFactory parent,
                                                       AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        return new MissingAggregatorFactory(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        return REGISTRY_KEY;
    }
}
