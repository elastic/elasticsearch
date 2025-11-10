/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public abstract class InternalTerms<A extends InternalTerms<A, B>, B extends InternalTerms.Bucket<B>> extends AbstractInternalTerms<A, B>
    implements
        Terms {

    public static final ParseField DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME = new ParseField("doc_count_error_upper_bound");
    public static final ParseField SUM_OF_OTHER_DOC_COUNTS = new ParseField("sum_other_doc_count");

    public abstract static class Bucket<B extends Bucket<B>> extends AbstractTermsBucket<B> implements Terms.Bucket {
        /**
         * Reads a bucket. Should be a constructor reference.
         */
        @FunctionalInterface
        public interface Reader<B extends Bucket<B>> {
            B read(StreamInput in, DocValueFormat format, boolean showDocCountError) throws IOException;
        }

        protected long docCount;
        private long docCountError;
        protected InternalAggregations aggregations;
        protected final DocValueFormat format;

        protected Bucket(
            long docCount,
            InternalAggregations aggregations,
            boolean showDocCountError,
            long docCountError,
            DocValueFormat formatter
        ) {
            this.format = formatter;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.docCountError = showDocCountError ? docCountError : -1;
        }

        /**
         * Read from a stream.
         */
        protected Bucket(StreamInput in, DocValueFormat formatter, boolean showDocCountError) throws IOException {
            this.format = formatter;
            docCount = in.readVLong();
            docCountError = showDocCountError ? in.readLong() : -1;
            aggregations = InternalAggregations.readFrom(in);
        }

        final void writeTo(StreamOutput out, boolean showDocCountError) throws IOException {
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

        public void setDocCount(long docCount) {
            this.docCount = docCount;
        }

        @Override
        public long getDocCountError() {
            return docCountError;
        }

        @Override
        public void setDocCountError(long docCountError) {
            this.docCountError = docCountError;
        }

        @Override
        protected void updateDocCountError(long docCountErrorDiff) {
            this.docCountError += docCountErrorDiff;
        }

        @Override
        public InternalAggregations getAggregations() {
            return aggregations;
        }

        public void setAggregations(InternalAggregations aggregations) {
            this.aggregations = aggregations;
        }

        @Override
        public final void bucketToXContent(XContentBuilder builder, Params params, boolean showDocCountError) throws IOException {
            builder.startObject();
            keyToXContent(builder);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
            if (showDocCountError) {
                builder.field(InternalTerms.DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME.getPreferredName(), getDocCountError());
            }
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
        }

        protected abstract XContentBuilder keyToXContent(XContentBuilder builder) throws IOException;

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Bucket<?> that = (Bucket<?>) obj;
            return docCountError == that.docCountError
                && docCount == that.docCount
                && Objects.equals(format, that.format)
                && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), docCount, format, docCountError, aggregations);
        }
    }

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
        super(name, metadata, reduceOrder, order, requiredSize, minDocCount);
    }

    /**
     * Read from a stream.
     */
    protected InternalTerms(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
        writeTermTypeInfoTo(out);
    }

    protected abstract void writeTermTypeInfoTo(StreamOutput out) throws IOException;

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
