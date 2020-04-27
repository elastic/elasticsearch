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

import org.elasticsearch.Version;
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
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public final class CardinalityAggregationBuilder
    extends ValuesSourceAggregationBuilder.LeafOnly<ValuesSource, CardinalityAggregationBuilder> {

    public static final String NAME = "cardinality";

    private static final ParseField REHASH = new ParseField("rehash").withAllDeprecated("no replacement - values will always be rehashed");
    public static final ParseField PRECISION_THRESHOLD_FIELD = new ParseField("precision_threshold");

    public static final ObjectParser<CardinalityAggregationBuilder, String> PARSER =
            ObjectParser.fromBuilder(NAME, CardinalityAggregationBuilder::new);
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, false, false);
        PARSER.declareLong(CardinalityAggregationBuilder::precisionThreshold, CardinalityAggregationBuilder.PRECISION_THRESHOLD_FIELD);
        PARSER.declareLong((b, v) -> {/*ignore*/}, REHASH);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        CardinalityAggregatorFactory.registerAggregators(builder);
    }

    private Long precisionThreshold = null;

    public CardinalityAggregationBuilder(String name) {
        super(name);
    }

    public CardinalityAggregationBuilder(CardinalityAggregationBuilder clone,
                                         AggregatorFactories.Builder factoriesBuilder,
                                         Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.precisionThreshold = clone.precisionThreshold;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    /**
     * Read from a stream.
     */
    public CardinalityAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            precisionThreshold = in.readLong();
        }
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new CardinalityAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        boolean hasPrecisionThreshold = precisionThreshold != null;
        out.writeBoolean(hasPrecisionThreshold);
        if (hasPrecisionThreshold) {
            out.writeLong(precisionThreshold);
        }
    }

    @Override
    protected boolean serializeTargetValueType(Version version) {
        return true;
    }

    /**
     * Set a precision threshold. Higher values improve accuracy but also
     * increase memory usage.
     */
    public CardinalityAggregationBuilder precisionThreshold(long precisionThreshold) {
        if (precisionThreshold < 0) {
            throw new IllegalArgumentException(
                    "[precisionThreshold] must be greater than or equal to 0. Found [" + precisionThreshold + "] in [" + name + "]");
        }
        this.precisionThreshold = precisionThreshold;
        return this;
    }

    /**
     * Get the precision threshold. Higher values improve accuracy but also
     * increase memory usage. Will return <code>null</code> if the
     * precisionThreshold has not been set yet.
     */
    public Long precisionThreshold() {
        return precisionThreshold;
    }

    @Override
    protected CardinalityAggregatorFactory innerBuild(QueryShardContext queryShardContext, ValuesSourceConfig config,
                                                      AggregatorFactory parent,
                                                      AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        return new CardinalityAggregatorFactory(name, config, precisionThreshold, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (precisionThreshold != null) {
            builder.field(PRECISION_THRESHOLD_FIELD.getPreferredName(), precisionThreshold);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precisionThreshold);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        CardinalityAggregationBuilder other = (CardinalityAggregationBuilder) obj;
        return Objects.equals(precisionThreshold, other.precisionThreshold);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
