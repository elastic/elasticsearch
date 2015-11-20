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
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class CardinalityAggregatorFactory<VS extends ValuesSource> extends ValuesSourceAggregatorFactory.LeafOnly<VS> {

    public static final ParseField PRECISION_THRESHOLD_FIELD = new ParseField("precision_threshold");

    private Long precisionThreshold = null;

    public CardinalityAggregatorFactory(String name, ValuesSourceType valuesSourceType, ValueType valueType) {
        super(name, InternalCardinality.TYPE, valuesSourceType, valueType);
    }

    /**
     * Set a precision threshold. Higher values improve accuracy but also
     * increase memory usage.
     */
    public void precisionThreshold(long precisionThreshold) {
        this.precisionThreshold = precisionThreshold;
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

    private int precision(Aggregator parent) {
        return precisionThreshold == null ? defaultPrecision(parent) : HyperLogLogPlusPlus.precisionFromThreshold(precisionThreshold);
    }

    @Override
    protected Aggregator createUnmapped(AggregationContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException {
        return new CardinalityAggregator(name, null, precision(parent), config.formatter(), context, parent, pipelineAggregators, metaData);
    }

    @Override
    protected Aggregator doCreateInternal(VS valuesSource, AggregationContext context, Aggregator parent,
            boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        return new CardinalityAggregator(name, valuesSource, precision(parent), config.formatter(), context, parent, pipelineAggregators,
                metaData);
    }

    @Override
    protected ValuesSourceAggregatorFactory<VS> innerReadFrom(String name, ValuesSourceType valuesSourceType,
            ValueType targetValueType, StreamInput in) throws IOException {
        CardinalityAggregatorFactory<VS> factory = new CardinalityAggregatorFactory<>(name, valuesSourceType, targetValueType);
        if (in.readBoolean()) {
            factory.precisionThreshold = in.readLong();
        }
        return factory;
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
        CardinalityAggregatorFactory<ValuesSource> other = (CardinalityAggregatorFactory<ValuesSource>) obj;
        return Objects.equals(precisionThreshold, other.precisionThreshold);
    }

    /*
     * If one of the parent aggregators is a MULTI_BUCKET one, we might want to lower the precision
     * because otherwise it might be memory-intensive. On the other hand, for top-level aggregators
     * we try to focus on accuracy.
     */
    private static int defaultPrecision(Aggregator parent) {
        int precision = HyperLogLogPlusPlus.DEFAULT_PRECISION;
        while (parent != null) {
            if (parent instanceof SingleBucketAggregator == false) {
                // if the parent creates buckets, we substract 5 to the precision,
                // which will effectively divide the memory usage of each counter by 32
                precision -= 5;
            }
            parent = parent.parent();
        }

        return Math.max(precision, HyperLogLogPlusPlus.MIN_PRECISION);
    }

}
