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

package org.elasticsearch.search.aggregations.metrics.cardinality;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Objects;

public final class CardinalityAggregationBuilder
    extends ValuesSourceAggregationBuilder.LeafOnly<ValuesSource, CardinalityAggregationBuilder> {

    public static final String NAME = InternalCardinality.TYPE.name();
    public static final ParseField AGGREGATION_NAME_FIELD = new ParseField(NAME);

    public static final ParseField PRECISION_THRESHOLD_FIELD = new ParseField("precision_threshold");

    private Long precisionThreshold = null;

    public CardinalityAggregationBuilder(String name, ValueType targetValueType) {
        super(name, InternalCardinality.TYPE, ValuesSourceType.ANY, targetValueType);
    }

    /**
     * Read from a stream.
     */
    public CardinalityAggregationBuilder(StreamInput in) throws IOException {
        super(in, InternalCardinality.TYPE, ValuesSourceType.ANY);
        if (in.readBoolean()) {
            precisionThreshold = in.readLong();
        }
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
    protected boolean serializeTargetValueType() {
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

    /**
     * @deprecated no replacement - values will always be rehashed
     */
    @Deprecated
    public void rehash(boolean rehash) {
        // Deprecated all values are already rehashed so do nothing
    }

    @Override
    protected CardinalityAggregatorFactory innerBuild(AggregationContext context, ValuesSourceConfig<ValuesSource> config,
            AggregatorFactory<?> parent, Builder subFactoriesBuilder) throws IOException {
        return new CardinalityAggregatorFactory(name, type, config, precisionThreshold, context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (precisionThreshold != null) {
            builder.field(PRECISION_THRESHOLD_FIELD.getPreferredName(), precisionThreshold);
        }
        return builder;
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(precisionThreshold);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        CardinalityAggregationBuilder other = (CardinalityAggregationBuilder) obj;
        return Objects.equals(precisionThreshold, other.precisionThreshold);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
