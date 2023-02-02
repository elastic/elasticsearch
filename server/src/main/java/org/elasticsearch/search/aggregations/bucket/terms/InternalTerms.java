/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class InternalTerms<A extends InternalTerms<A, B>, B extends InternalTerms.Bucket<B>> extends AbstractInternalTerms<A, B>
    implements
        Terms {

    public static final ParseField DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME = new ParseField("doc_count_error_upper_bound");
    public static final ParseField SUM_OF_OTHER_DOC_COUNTS = new ParseField("sum_other_doc_count");

    public abstract static class Bucket<B extends Bucket<B>> extends AbstractTermsBucket implements Terms.Bucket, KeyComparable<B> {
        /**
         * Reads a bucket. Should be a constructor reference.
         */
        @FunctionalInterface
        public interface Reader<B extends Bucket<B>> {
            B read(StreamInput in, DocValueFormat format, boolean showDocCountError) throws IOException;
        }

        long bucketOrd;

        protected long docCount;
        protected long docCountError;
        protected InternalAggregations aggregations;
        protected final boolean showDocCountError;
        protected final DocValueFormat format;

        protected Bucket(
            long docCount,
            InternalAggregations aggregations,
            boolean showDocCountError,
            long docCountError,
            DocValueFormat formatter
        ) {
            this.showDocCountError = showDocCountError;
            this.format = formatter;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.docCountError = docCountError;
        }

        /**
         * Read from a stream.
         */
        protected Bucket(StreamInput in, DocValueFormat formatter, boolean showDocCountError) throws IOException {
            this.showDocCountError = showDocCountError;
            this.format = formatter;
            docCount = in.readVLong();
            docCountError = -1;
            if (showDocCountError) {
                docCountError = in.readLong();
            }
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public final void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(getDocCount());
            if (showDocCountError) {
                out.writeLong(docCountError);
            }
            aggregations.writeTo(out);
            writeTermTo(out);
        }

        protected abstract void writeTermTo(StreamOutput out) throws IOException;

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public long getDocCountError() {
            if (showDocCountError == false) {
                throw new IllegalStateException("show_terms_doc_count_error is false");
            }
            return docCountError;
        }

        @Override
        protected void setDocCountError(long docCountError) {
            this.docCountError = docCountError;
        }

        @Override
        protected void updateDocCountError(long docCountErrorDiff) {
            this.docCountError += docCountErrorDiff;
        }

        @Override
        protected boolean getShowDocCountError() {
            return showDocCountError;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            keyToXContent(builder);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
            if (showDocCountError) {
                builder.field(InternalTerms.DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME.getPreferredName(), getDocCountError());
            }
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
            if (showDocCountError && docCountError != that.docCountError) {
                /*
                 * docCountError doesn't matter if not showing it and
                 * serialization sets it to -1 no matter what it was
                 * before.
                 */
                return false;
            }
            return Objects.equals(docCount, that.docCount)
                && Objects.equals(showDocCountError, that.showDocCountError)
                && Objects.equals(format, that.format)
                && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), docCount, format, showDocCountError, showDocCountError ? docCountError : -1, aggregations);
        }
    }

    protected final BucketOrder reduceOrder;
    protected final BucketOrder order;
    protected final int requiredSize;
    protected final long minDocCount;

    /**
     * Creates a new {@link InternalTerms}
     * @param name The name of the aggregation
     * @param reduceOrder The {@link BucketOrder} that should be used to merge shard results.
     * @param order The {@link BucketOrder} that should be used to sort the final reduce.
     * @param requiredSize The number of top buckets.
     * @param minDocCount The minimum number of documents allowed per bucket.
     * @param metadata The metadata associated with the aggregation.
     */
    protected InternalTerms(
        String name,
        BucketOrder reduceOrder,
        BucketOrder order,
        int requiredSize,
        long minDocCount,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.reduceOrder = reduceOrder;
        this.order = order;
        this.requiredSize = requiredSize;
        this.minDocCount = minDocCount;
    }

    /**
     * Read from a stream.
     */
    protected InternalTerms(StreamInput in) throws IOException {
        super(in);
        reduceOrder = InternalOrder.Streams.readOrder(in);
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_7_10_0)) {
            order = InternalOrder.Streams.readOrder(in);
        } else {
            order = reduceOrder;
        }
        requiredSize = readSize(in);
        minDocCount = in.readVLong();
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_10_0)) {
            reduceOrder.writeTo(out);
        }
        order.writeTo(out);
        writeSize(requiredSize, out);
        out.writeVLong(minDocCount);
        writeTermTypeInfoTo(out);
    }

    protected abstract void writeTermTypeInfoTo(StreamOutput out) throws IOException;

    @Override
    public abstract List<B> getBuckets();

    @Override
    public abstract B getBucketByKey(String term);

    @Override
    protected BucketOrder getReduceOrder() {
        return reduceOrder;
    }

    @Override
    protected BucketOrder getOrder() {
        return order;
    }

    @Override
    protected long getMinDocCount() {
        return minDocCount;
    }

    @Override
    protected int getRequiredSize() {
        return requiredSize;
    }

    protected abstract void setDocCountError(long docCountError);

    protected abstract int getShardSize();

    protected abstract A create(String name, List<B> buckets, BucketOrder reduceOrder, long docCountError, long otherDocCount);

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalTerms<?, ?> that = (InternalTerms<?, ?>) obj;
        return Objects.equals(minDocCount, that.minDocCount)
            && Objects.equals(reduceOrder, that.reduceOrder)
            && Objects.equals(order, that.order)
            && Objects.equals(requiredSize, that.requiredSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), minDocCount, reduceOrder, order, requiredSize);
    }
}
