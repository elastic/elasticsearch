/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.SetBackedScalingCuckooFilter;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class InternalRareTerms<A extends InternalRareTerms<A, B>, B extends InternalRareTerms.Bucket<B>> extends
    InternalMultiBucketAggregation<A, B>
    implements
        RareTerms {

    public abstract static class Bucket<B extends Bucket<B>> extends InternalMultiBucketAggregation.InternalBucket
        implements
            RareTerms.Bucket,
            KeyComparable<B> {
        /**
         * Reads a bucket. Should be a constructor reference.
         */
        @FunctionalInterface
        public interface Reader<B extends Bucket<B>> {
            B read(StreamInput in, DocValueFormat format) throws IOException;
        }

        long bucketOrd;

        protected long docCount;
        protected InternalAggregations aggregations;
        protected final DocValueFormat format;

        protected Bucket(long docCount, InternalAggregations aggregations, DocValueFormat formatter) {
            this.format = formatter;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        /**
         * Read from a stream.
         */
        protected Bucket(StreamInput in, DocValueFormat formatter) throws IOException {
            this.format = formatter;
            docCount = in.readVLong();
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public final void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(getDocCount());
            aggregations.writeTo(out);
            writeTermTo(out);
        }

        protected abstract void writeTermTo(StreamOutput out) throws IOException;

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public InternalAggregations getAggregations() {
            return aggregations;
        }

        @Override
        public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            keyToXContent(builder);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        protected abstract XContentBuilder keyToXContent(XContentBuilder builder) throws IOException;

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Bucket<?> that = (Bucket<?>) obj;
            return Objects.equals(docCount, that.docCount) && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), docCount, aggregations);
        }
    }

    protected final BucketOrder order;
    protected final long maxDocCount;

    protected InternalRareTerms(String name, BucketOrder order, long maxDocCount, Map<String, Object> metadata) {
        super(name, metadata);
        this.order = order;
        this.maxDocCount = maxDocCount;
    }

    /**
     * Read from a stream.
     */
    protected InternalRareTerms(StreamInput in) throws IOException {
        super(in);
        order = InternalOrder.Streams.readOrder(in);
        maxDocCount = in.readVLong();
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        order.writeTo(out);
        out.writeVLong(maxDocCount);
        writeTermTypeInfoTo(out);
    }

    protected abstract void writeTermTypeInfoTo(StreamOutput out) throws IOException;

    @Override
    public abstract List<B> getBuckets();

    abstract B createBucket(long docCount, InternalAggregations aggs, B prototype);

    protected abstract A createWithFilter(String name, List<B> buckets, SetBackedScalingCuckooFilter filter);

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        InternalRareTerms<?, ?> that = (InternalRareTerms<?, ?>) obj;
        return Objects.equals(maxDocCount, that.maxDocCount) && Objects.equals(order, that.order);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), maxDocCount, order);
    }

    protected static XContentBuilder doXContentCommon(XContentBuilder builder, Params params, List<? extends Bucket<?>> buckets)
        throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (Bucket<?> bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }
}
