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
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.InternalOrder.CompoundOrder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParserHelper;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A builder for histograms on numeric fields.  This builder can operate on either base numeric fields, or numeric range fields.  IP range
 * fields are unsupported, and will throw at the factory layer.
 */
public class HistogramAggregationBuilder extends ValuesSourceAggregationBuilder<ValuesSource, HistogramAggregationBuilder>
        implements MultiBucketAggregationBuilder {
    public static final String NAME = "histogram";

    private static final ObjectParser<double[], Void> EXTENDED_BOUNDS_PARSER = new ObjectParser<>(
            Histogram.EXTENDED_BOUNDS_FIELD.getPreferredName(),
            () -> new double[]{ Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY });
    static {
        EXTENDED_BOUNDS_PARSER.declareDouble((bounds, d) -> bounds[0] = d, new ParseField("min"));
        EXTENDED_BOUNDS_PARSER.declareDouble((bounds, d) -> bounds[1] = d, new ParseField("max"));
    }

    private static final ObjectParser<HistogramAggregationBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(HistogramAggregationBuilder.NAME);
        ValuesSourceParserHelper.declareAnyFields(PARSER, true, true);

        PARSER.declareDouble(HistogramAggregationBuilder::interval, Histogram.INTERVAL_FIELD);

        PARSER.declareDouble(HistogramAggregationBuilder::offset, Histogram.OFFSET_FIELD);

        PARSER.declareBoolean(HistogramAggregationBuilder::keyed, Histogram.KEYED_FIELD);

        PARSER.declareLong(HistogramAggregationBuilder::minDocCount, Histogram.MIN_DOC_COUNT_FIELD);

        PARSER.declareField((histogram, extendedBounds) -> {
            histogram.extendedBounds(extendedBounds[0], extendedBounds[1]);
        }, parser -> EXTENDED_BOUNDS_PARSER.apply(parser, null), ExtendedBounds.EXTENDED_BOUNDS_FIELD, ObjectParser.ValueType.OBJECT);

        PARSER.declareObjectArray(HistogramAggregationBuilder::order, (p, c) -> InternalOrder.Parser.parseOrderParam(p),
            Histogram.ORDER_FIELD);
    }

    public static HistogramAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new HistogramAggregationBuilder(aggregationName), null);
    }

    private double interval;
    private double offset = 0;
    private double minBound = Double.POSITIVE_INFINITY;
    private double maxBound = Double.NEGATIVE_INFINITY;
    private BucketOrder order = BucketOrder.key(true);
    private boolean keyed = false;
    private long minDocCount = 0;

    @Override
    protected ValuesSourceType resolveScriptAny(Script script) {
        // TODO: No idea how we'd support Range scripts here.
        return ValuesSourceType.NUMERIC;
    }

    /** Create a new builder with the given name. */
    public HistogramAggregationBuilder(String name) {
        super(name, ValuesSourceType.ANY, ValueType.NUMERIC);
    }

    protected HistogramAggregationBuilder(HistogramAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
        this.interval = clone.interval;
        this.offset = clone.offset;
        this.minBound = clone.minBound;
        this.maxBound = clone.maxBound;
        this.order = clone.order;
        this.keyed = clone.keyed;
        this.minDocCount = clone.minDocCount;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metaData) {
        return new HistogramAggregationBuilder(this, factoriesBuilder, metaData);
    }

    /** Read from a stream, for internal use only. */
    public HistogramAggregationBuilder(StreamInput in) throws IOException {
        super(in, ValuesSourceType.ANY, ValueType.NUMERIC);
        order = InternalOrder.Streams.readHistogramOrder(in);
        keyed = in.readBoolean();
        minDocCount = in.readVLong();
        interval = in.readDouble();
        offset = in.readDouble();
        minBound = in.readDouble();
        maxBound = in.readDouble();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        InternalOrder.Streams.writeHistogramOrder(order, out);
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

    /**
     * Set extended bounds on this builder: buckets between {@code minBound} and
     * {@code maxBound} will be created even if no documents fell into these
     * buckets.
     *
     * @throws IllegalArgumentException
     *             if maxBound is less that minBound, or if either of the bounds
     *             are not finite.
     */
    public HistogramAggregationBuilder extendedBounds(double minBound, double maxBound) {
        if (Double.isFinite(minBound) == false) {
            throw new IllegalArgumentException("minBound must be finite, got: " + minBound);
        }
        if (Double.isFinite(maxBound) == false) {
            throw new IllegalArgumentException("maxBound must be finite, got: " + maxBound);
        }
        if (maxBound < minBound) {
            throw new IllegalArgumentException("maxBound [" + maxBound + "] must be greater than minBound [" + minBound + "]");
        }
        this.minBound = minBound;
        this.maxBound = maxBound;
        return this;
    }

    /** Return the order to use to sort buckets of this histogram. */
    public BucketOrder order() {
        return order;
    }

    /** Set a new order on this builder and return the builder so that calls
     *  can be chained. A tie-breaker may be added to avoid non-deterministic ordering. */
    public HistogramAggregationBuilder order(BucketOrder order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null: [" + name + "]");
        }
        if(order instanceof CompoundOrder || InternalOrder.isKeyOrder(order)) {
            this.order = order; // if order already contains a tie-breaker we are good to go
        } else { // otherwise add a tie-breaker by using a compound order
            this.order = BucketOrder.compound(order);
        }
        return this;
    }

    /**
     * Sets the order in which the buckets will be returned. A tie-breaker may be added to avoid non-deterministic
     * ordering.
     */
    public HistogramAggregationBuilder order(List<BucketOrder> orders) {
        if (orders == null) {
            throw new IllegalArgumentException("[orders] must not be null: [" + name + "]");
        }
        // if the list only contains one order use that to avoid inconsistent xcontent
        order(orders.size() > 1 ? BucketOrder.compound(orders) : orders.get(0));
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
    public String getType() {
        return NAME;
    }

    @Override
    protected ValuesSourceAggregatorFactory<ValuesSource> innerBuild(SearchContext context, ValuesSourceConfig<ValuesSource> config,
            AggregatorFactory parent, Builder subFactoriesBuilder) throws IOException {
        return new HistogramAggregatorFactory(name, config, interval, offset, order, keyed, minDocCount, minBound, maxBound,
                context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), order, keyed, minDocCount, interval, offset, minBound, maxBound);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
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
