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
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueSourceParser;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import java.io.IOException;
import java.util.Objects;

public final class CardinalityAggregatorBuilder extends ValuesSourceAggregatorBuilder.LeafOnly<ValuesSource, CardinalityAggregatorBuilder> {
    private static final ParseField PRECISION_THRESHOLD_FIELD = new ParseField("precision_threshold");

    private Long precisionThreshold = null;

    public CardinalityAggregatorBuilder(String name, ValueType targetValueType) {
        super(name, InternalCardinality.TYPE, ValuesSourceType.ANY, targetValueType);
    }

    private CardinalityAggregatorBuilder(String name) {
        this(name, null);
    }

    /**
     * Read from a stream.
     */
    public CardinalityAggregatorBuilder(StreamInput in) throws IOException {
        super(in, InternalCardinality.TYPE, ValuesSourceType.ANY, in.readOptionalWriteable(ValueType::readFromStream));
        if (in.readBoolean()) {
            precisionThreshold = in.readLong();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(getTargetValueType());
        super.writeTo(out);
        boolean hasPrecisionThreshold = precisionThreshold != null;
        out.writeBoolean(hasPrecisionThreshold);
        if (hasPrecisionThreshold) {
            out.writeLong(precisionThreshold);
        }
    }

    /**
     * Set a precision threshold. Higher values improve accuracy but also
     * increase memory usage.
     */
    public CardinalityAggregatorBuilder precisionThreshold(long precisionThreshold) {
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

    public static final Aggregator.Parser PARSER = ValueSourceParser.builder(CardinalityAggregatorBuilder::new, InternalCardinality.TYPE)
            .custom((p) -> {
                p.declareLong(CardinalityAggregatorBuilder::precisionThreshold, PRECISION_THRESHOLD_FIELD);
            }).build();

    @Override
    protected int innerHashCode() {
        return Objects.hash(precisionThreshold);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        CardinalityAggregatorBuilder other = (CardinalityAggregatorBuilder) obj;
        return Objects.equals(precisionThreshold, other.precisionThreshold);
    }

}
