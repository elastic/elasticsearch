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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Objects;

public abstract class AbstractHistogramBuilder<AB extends AbstractHistogramBuilder<AB>>
        extends ValuesSourceAggregationBuilder<ValuesSource.Numeric, AB> {

    protected long interval;
    protected long offset = 0;
    protected InternalOrder order = (InternalOrder) Histogram.Order.KEY_ASC;
    protected boolean keyed = false;
    protected long minDocCount = 0;
    protected ExtendedBounds extendedBounds;

    protected AbstractHistogramBuilder(String name, InternalHistogram.Factory<?> histogramFactory) {
        super(name, histogramFactory.type(), ValuesSourceType.NUMERIC, histogramFactory.valueType());
    }

    /**
     * Read from a stream.
     */
    protected AbstractHistogramBuilder(StreamInput in, InternalHistogram.Factory<?> histogramFactory) throws IOException {
        super(in, histogramFactory.type(), ValuesSourceType.NUMERIC, histogramFactory.valueType());
        interval = in.readVLong();
        offset = in.readLong();
        if (in.readBoolean()) {
            order = InternalOrder.Streams.readOrder(in);
        }
        keyed = in.readBoolean();
        minDocCount = in.readVLong();
        if (in.readBoolean()) {
            extendedBounds = new ExtendedBounds(in);
        }
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeVLong(interval);
        out.writeLong(offset);
        boolean hasOrder = order != null;
        out.writeBoolean(hasOrder);
        if (hasOrder) {
            InternalOrder.Streams.writeOrder(order, out);
        }
        out.writeBoolean(keyed);
        out.writeVLong(minDocCount);
        boolean hasExtendedBounds = extendedBounds != null;
        out.writeBoolean(hasExtendedBounds);
        if (hasExtendedBounds) {
            extendedBounds.writeTo(out);
        }
    }

    public long interval() {
        return interval;
    }

    @SuppressWarnings("unchecked")
    public AB interval(long interval) {
        if (interval < 1) {
            throw new IllegalArgumentException("[interval] must be 1 or greater for histogram aggregation [" + name + "]");
        }
        this.interval = interval;
        return (AB) this;
    }

    public long offset() {
        return offset;
    }

    @SuppressWarnings("unchecked")
    public AB offset(long offset) {
        this.offset = offset;
        return (AB) this;
    }

    public Histogram.Order order() {
        return order;
    }

    @SuppressWarnings("unchecked")
    public AB order(Histogram.Order order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null: [" + name + "]");
        }
        this.order = (InternalOrder) order;
        return (AB) this;
    }

    public boolean keyed() {
        return keyed;
    }

    @SuppressWarnings("unchecked")
    public AB keyed(boolean keyed) {
        this.keyed = keyed;
        return (AB) this;
    }

    public long minDocCount() {
        return minDocCount;
    }

    @SuppressWarnings("unchecked")
    public AB minDocCount(long minDocCount) {
        if (minDocCount < 0) {
            throw new IllegalArgumentException(
                    "[minDocCount] must be greater than or equal to 0. Found [" + minDocCount + "] in [" + name + "]");
        }
        this.minDocCount = minDocCount;
        return (AB) this;
    }

    public ExtendedBounds extendedBounds() {
        return extendedBounds;
    }

    @SuppressWarnings("unchecked")
    public AB extendedBounds(ExtendedBounds extendedBounds) {
        if (extendedBounds == null) {
            throw new IllegalArgumentException("[extendedBounds] must not be null: [" + name + "]");
        }
        this.extendedBounds = extendedBounds;
        return (AB) this;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {

        builder.field(Rounding.Interval.INTERVAL_FIELD.getPreferredName());
        doXContentInterval(builder, params);
        builder.field(Rounding.OffsetRounding.OFFSET_FIELD.getPreferredName(), offset);

        if (order != null) {
            builder.field(HistogramAggregator.ORDER_FIELD.getPreferredName());
            order.toXContent(builder, params);
        }

        builder.field(HistogramAggregator.KEYED_FIELD.getPreferredName(), keyed);

        builder.field(HistogramAggregator.MIN_DOC_COUNT_FIELD.getPreferredName(), minDocCount);

        if (extendedBounds != null) {
            extendedBounds.toXContent(builder, params);
        }

        return builder;
    }

    protected XContentBuilder doXContentInterval(XContentBuilder builder, Params params) throws IOException {
        builder.value(interval);
        return builder;
    }

    @Override
    public String getWriteableName() {
        return InternalHistogram.TYPE.name();
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(interval, offset, order, keyed, minDocCount, extendedBounds);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        AbstractHistogramBuilder<?> other = (AbstractHistogramBuilder<?>) obj;
        return Objects.equals(interval, other.interval)
                && Objects.equals(offset, other.offset)
                && Objects.equals(order, other.order)
                && Objects.equals(keyed, other.keyed)
                && Objects.equals(minDocCount, other.minDocCount)
                && Objects.equals(extendedBounds, other.extendedBounds);
    }
}
