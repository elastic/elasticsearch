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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder.LeafOnly;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class MedianAbsoluteDeviationAggregationBuilder extends LeafOnly<ValuesSource.Numeric, MedianAbsoluteDeviationAggregationBuilder> {

    public static final String NAME = "median_absolute_deviation";
    public static final ValuesSourceRegistry.RegistryKey<MedianAbsoluteDeviationAggregatorSupplier> REGISTRY_KEY =
        new ValuesSourceRegistry.RegistryKey<>(NAME, MedianAbsoluteDeviationAggregatorSupplier.class);

    private static final ParseField COMPRESSION_FIELD = new ParseField("compression");

    public static final ObjectParser<MedianAbsoluteDeviationAggregationBuilder, String> PARSER =
            ObjectParser.fromBuilder(NAME, MedianAbsoluteDeviationAggregationBuilder::new);
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);
        PARSER.declareDouble(MedianAbsoluteDeviationAggregationBuilder::compression, COMPRESSION_FIELD);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        MedianAbsoluteDeviationAggregatorFactory.registerAggregators(builder);
    }

    private double compression = 1000d;

    public MedianAbsoluteDeviationAggregationBuilder(String name) {
        super(name);
    }

    public MedianAbsoluteDeviationAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        compression = in.readDouble();
    }

    protected MedianAbsoluteDeviationAggregationBuilder(MedianAbsoluteDeviationAggregationBuilder clone,
                                                        AggregatorFactories.Builder factoriesBuilder,
                                                        Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.compression = clone.compression;
    }

    /**
     * Returns the compression factor of the t-digest sketches used
     */
    public double compression() {
        return compression;
    }

    /**
     * Set the compression factor of the t-digest sketches used
     */
    public MedianAbsoluteDeviationAggregationBuilder compression(double compression) {
        if (compression <= 0d) {
            throw new IllegalArgumentException(
                "[" + COMPRESSION_FIELD.getPreferredName() + "] must be greater than 0. Found [" + compression + "] in [" + name + "]");
        }
        this.compression = compression;
        return this;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new MedianAbsoluteDeviationAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(compression);
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(QueryShardContext queryShardContext,
                                                       ValuesSourceConfig config,
                                                       AggregatorFactory parent,
                                                       AggregatorFactories.Builder subFactoriesBuilder)
        throws IOException {
        return new MedianAbsoluteDeviationAggregatorFactory(name, config, queryShardContext,
            parent, subFactoriesBuilder, metadata, compression);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(COMPRESSION_FIELD.getPreferredName(), compression);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), compression);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        MedianAbsoluteDeviationAggregationBuilder other = (MedianAbsoluteDeviationAggregationBuilder) obj;
        return Objects.equals(compression, other.compression);
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
