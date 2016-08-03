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

package org.elasticsearch.search.aggregations.bucket.histogram;

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
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Objects;

/**
 * A builder for histograms on numeric fields.
 */
public class HistogramAggregationBuilder
        extends ValuesSourceAggregationBuilder<ValuesSource.Numeric, HistogramAggregationBuilder> {
    public static final String NAME = InternalHistogram.TYPE.name();
    public static final ParseField AGGREGATION_NAME_FIELD = new ParseField(NAME);

    private double interval;
    private double offset = 0;
    private double minBound = Double.MAX_VALUE;
    private double maxBound = Double.MIN_VALUE;
    private InternalOrder order = (InternalOrder) Histogram.Order.KEY_ASC;
    private boolean keyed = false;
    private long minDocCount = 0;

    /** Create a new builder with the given name. */
    public HistogramAggregationBuilder(String name) {
        super(name, InternalHistogram.TYPE, ValuesSourceType.NUMERIC, ValueType.DOUBLE);
    }

    /** Read from a stream, for internal use only. */
    public HistogramAggregationBuilder(StreamInput in) throws IOException {
        super(in, InternalHistogram.TYPE, ValuesSourceType.NUMERIC, ValueType.DOUBLE);
        if (in.readBoolean()) {
            order = InternalOrder.Streams.readOrder(in);
        }
        keyed = in.readBoolean();
        minDocCount = in.readVLong();
        interval = in.readDouble();
        offset = in.readDouble();
        minBound = in.readDouble();
        maxBound = in.readDouble();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        boolean hasOrder = order != null;
        out.writeBoolean(hasOrder);
        if (hasOrder) {
            InternalOrder.Streams.writeOrder(order, out);
        }
        out.writeBoolean(keyed);
        out.writeVLong(minDocCount);
        out.writeDouble(interval);
        out.writeDouble(offset);
        out.writeDouble(minBound);
        out.writeDouble(maxBound);
    }

    /** Get the current interval that is set on this builder. */
    public double interval() {
        return interval;
    }

    /** Set the interval on this builder, and return the builder so that calls can be chained. */
    public HistogramAggregationBuilder interval(double interval) {
        if (interval <= 0) {
            throw new IllegalArgumentException("[interval] must be >0 for histogram aggregation [" + name + "]");
        }
        this.interval = interval;
        return this;
    }

    /** Get the current offset that is set on this builder. */
    public double offset() {
        return offset;
    }

    /** Set the offset on this builder, and return the builder so that calls can be chained. */
    public HistogramAggregationBuilder offset(double offset) {
        this.offset = offset;
        return this;
    }

    /** Get the current minimum bound that is set on this builder. */
    public double minBound() {
        return minBound;
    }

    /** Get the current maximum bound that is set on this builder. */
    public double maxBound() {
        return maxBound;
    }

    /** Set extended bounds on this builder: buckets between {@code minBound}
     *  and {@code maxBound} will be created even if no documents fell into
     *  these buckets. It is possible to create half-open bounds by providing
     *  {@link Double#POSITIVE_INFINITY} as a {@code minBound} or 
     *  {@link Double#NEGATIVE_INFINITY} as a {@code maxBound}. */
    public HistogramAggregationBuilder extendedBounds(double minBound, double maxBound) {
        if (minBound == Double.NEGATIVE_INFINITY) {
            throw new IllegalArgumentException("minBound must not be -Infinity, got: " + minBound);
        }
        if (maxBound == Double.POSITIVE_INFINITY) {
            throw new IllegalArgumentException("maxBound must not be +Infinity, got: " + maxBound);
        }
        this.minBound = minBound;
        this.maxBound = maxBound;
        return this;
    }

    /** Return the order to use to sort buckets of this histogram. */
    public Histogram.Order order() {
        return order;
    }

    /** Set a new order on this builder and return the builder so that calls
     *  can be chained. */
    public HistogramAggregationBuilder order(Histogram.Order order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null: [" + name + "]");
        }
        this.order = (InternalOrder) order;
        return this;
    }

    /** Return whether buckets should be returned as a hash. In case
     *  {@code keyed} is false, buckets will be returned as an array. */
    public boolean keyed() {
        return keyed;
    }

    /** Set whether to return buckets as a hash or as an array, and return the
     *  builder so that calls can be chained. */
    public HistogramAggregationBuilder keyed(boolean keyed) {
        this.keyed = keyed;
        return this;
    }

    /** Return the minimum count of documents that buckets need to have in order
     *  to be included in the response. */
    public long minDocCount() {
        return minDocCount;
    }

    /** Set the minimum count of matching documents that buckets need to have
     *  and return this builder so that calls can be chained. */
    public HistogramAggregationBuilder minDocCount(long minDocCount) {
        if (minDocCount < 0) {
            throw new IllegalArgumentException(
                    "[minDocCount] must be greater than or equal to 0. Found [" + minDocCount + "] in [" + name + "]");
        }
        this.minDocCount = minDocCount;
        return this;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {

        builder.field(Histogram.INTERVAL_FIELD.getPreferredName(), interval);
        builder.field(Histogram.OFFSET_FIELD.getPreferredName(), offset);

        if (order != null) {
            builder.field(Histogram.ORDER_FIELD.getPreferredName());
            order.toXContent(builder, params);
        }

        builder.field(Histogram.KEYED_FIELD.getPreferredName(), keyed);

        builder.field(Histogram.MIN_DOC_COUNT_FIELD.getPreferredName(), minDocCount);

        if (Double.isFinite(minBound) || Double.isFinite(maxBound)) {
            builder.startObject(Histogram.EXTENDED_BOUNDS_FIELD.getPreferredName());
            if (Double.isFinite(minBound)) {
                builder.field("min", minBound);
            }
            if (Double.isFinite(maxBound)) {
                builder.field("max", maxBound);
            }
            builder.endObject();
        }

        return builder;
    }

    @Override
    public String getWriteableName() {
        return InternalHistogram.TYPE.name();
    }

    @Override
    protected ValuesSourceAggregatorFactory<Numeric, ?> innerBuild(AggregationContext context, ValuesSourceConfig<Numeric> config,
            AggregatorFactory<?> parent, Builder subFactoriesBuilder) throws IOException {
        return new HistogramAggregatorFactory(name, type, config, interval, offset, order, keyed, minDocCount, minBound, maxBound,
                context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(order, keyed, minDocCount, interval, offset, minBound, maxBound);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        HistogramAggregationBuilder other = (HistogramAggregationBuilder) obj;
        return Objects.equals(order, other.order)
                && Objects.equals(keyed, other.keyed)
                && Objects.equals(minDocCount, other.minDocCount)
                && Objects.equals(interval, other.interval)
                && Objects.equals(offset, other.offset)
                && Objects.equals(minBound, other.minBound)
                && Objects.equals(maxBound, other.maxBound);
    }
}
