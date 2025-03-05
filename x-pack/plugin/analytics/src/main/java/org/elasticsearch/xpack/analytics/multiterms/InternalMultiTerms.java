/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.multiterms;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationErrors;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.bucket.terms.AbstractInternalTerms;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.search.aggregations.bucket.terms.InternalTerms.DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME;

public class InternalMultiTerms extends AbstractInternalTerms<InternalMultiTerms, InternalMultiTerms.Bucket> {

    public static final TermsComparator TERMS_COMPARATOR = new TermsComparator();

    public static class Bucket extends AbstractInternalTerms.AbstractTermsBucket<Bucket> {
        protected long docCount;
        protected InternalAggregations aggregations;
        private long docCountError;
        protected final List<DocValueFormat> formats;
        protected List<Object> terms;
        protected List<KeyConverter> keyConverters;

        public Bucket(
            List<Object> terms,
            long docCount,
            InternalAggregations aggregations,
            boolean showDocCountError,
            long docCountError,
            List<DocValueFormat> formats,
            List<KeyConverter> keyConverters
        ) {
            this.terms = terms;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.docCountError = showDocCountError ? docCountError : -1;
            this.formats = formats;
            this.keyConverters = keyConverters;
        }

        protected Bucket(StreamInput in, List<DocValueFormat> formats, List<KeyConverter> keyConverters, boolean showDocCountError)
            throws IOException {
            terms = in.readCollectionAsList(StreamInput::readGenericValue);
            docCount = in.readVLong();
            aggregations = InternalAggregations.readFrom(in);
            docCountError = -1;
            if (showDocCountError) {
                docCountError = in.readLong();
            }
            this.formats = formats;
            this.keyConverters = keyConverters;
        }

        private void writeTo(StreamOutput out, boolean showDocCountError) throws IOException {
            out.writeCollection(terms, StreamOutput::writeGenericValue);
            out.writeVLong(docCount);
            aggregations.writeTo(out);
            if (showDocCountError) {
                out.writeLong(docCountError);
            }
        }

        @Override
        public List<Object> getKey() {
            List<Object> keys = new ArrayList<>(terms.size());
            for (int i = 0; i < terms.size(); i++) {
                keys.add(keyConverters.get(i).convert(formats.get(i), terms.get(i)));
            }
            return keys;
        }

        @Override
        public String getKeyAsString() {
            StringBuilder keys = new StringBuilder();
            for (int i = 0; i < terms.size(); i++) {
                if (i != 0) {
                    keys.append('|');
                }
                keys.append(keyConverters.get(i).convert(formats.get(i), terms.get(i)).toString());
            }
            return keys.toString();
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public InternalAggregations getAggregations() {
            return aggregations;
        }

        @Override
        public void bucketToXContent(XContentBuilder builder, Params params, boolean showDocCountError) throws IOException {
            builder.startObject();
            builder.field(CommonFields.KEY.getPreferredName(), getKey());
            builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), getKeyAsString());
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
            if (showDocCountError) {
                builder.field(DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME.getPreferredName(), getDocCountError());
            }
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
        }

        @Override
        public long getDocCountError() {
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
        public int compareKey(Bucket other) {
            return TERMS_COMPARATOR.compare(terms, other.terms);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Bucket other = (Bucket) obj;
            return docCount == other.docCount
                && aggregations.equals(other.aggregations)
                && docCountError == other.docCountError
                && terms.equals(other.terms)
                && keyConverters.equals(other.keyConverters);
        }

        @Override
        public int hashCode() {
            return Objects.hash(docCount, aggregations, docCountError, terms, keyConverters);
        }
    }

    /**
     * Compares buckets with compound keys
     */
    static class TermsComparator implements Comparator<List<Object>> {
        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public int compare(List<Object> thisTerms, List<Object> otherTerms) {
            if (thisTerms.size() != otherTerms.size()) {
                // Not clear on how this can happen.
                throw new AggregationExecutionException(
                    "Merging/Reducing the multi_term aggregations failed due to different term list sizes"
                );
            }
            for (int i = 0; i < thisTerms.size(); i++) {
                final int res;
                try {
                    res = ((Comparable) thisTerms.get(i)).compareTo(otherTerms.get(i));
                } catch (ClassCastException ex) {
                    throw AggregationErrors.reduceTypeMismatch("MultiTerms", Optional.empty());
                }
                if (res != 0) {
                    return res;
                }
            }
            return 0;
        }
    }

    /**
     * Specifies how the key from the internal representation should be converted to user representation
     */
    public enum KeyConverter {
        UNSIGNED_LONG {
            @Override
            public Object convert(DocValueFormat format, Object obj) {
                return format.format((Long) obj).toString();
            }

            @Override
            public double toDouble(DocValueFormat format, Object obj) {
                return ((Number) format.format((Long) obj)).doubleValue();
            }
        },

        LONG {
            @Override
            public Object convert(DocValueFormat format, Object obj) {
                return format.format((Long) obj);
            }

            @Override
            public double toDouble(DocValueFormat format, Object obj) {
                return ((Long) obj).doubleValue();
            }
        },

        DOUBLE {
            @Override
            public Object convert(DocValueFormat format, Object obj) {
                return format.format((Double) obj);
            }

            @Override
            public double toDouble(DocValueFormat format, Object obj) {
                return (Double) obj;
            }
        },

        STRING {
            @Override
            public Object convert(DocValueFormat format, Object obj) {
                return format.format((BytesRef) obj);
            }
        },

        IP {
            @Override
            public Object convert(DocValueFormat format, Object obj) {
                return format.format((BytesRef) obj);
            }
        };

        public Object convert(DocValueFormat format, Object obj) {
            throw new UnsupportedOperationException();
        }

        public double toDouble(DocValueFormat format, Object obj) {
            throw new UnsupportedOperationException();
        }
    }

    protected final BucketOrder reduceOrder;
    protected final BucketOrder order;
    protected final int requiredSize;
    protected final long minDocCount;
    protected final List<DocValueFormat> formats;
    protected final List<KeyConverter> keyConverters;
    protected final int shardSize;
    protected final boolean showTermDocCountError;
    protected final long otherDocCount;
    protected final List<Bucket> buckets;
    protected long docCountError;

    public InternalMultiTerms(
        String name,
        BucketOrder reduceOrder,
        BucketOrder order,
        int requiredSize,
        long minDocCount,
        int shardSize,
        boolean showTermDocCountError,
        long otherDocCount,
        List<Bucket> buckets,
        long docCountError,
        List<DocValueFormat> formats,
        List<KeyConverter> keyConverters,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.reduceOrder = reduceOrder;
        this.order = order;
        this.requiredSize = requiredSize;
        this.minDocCount = minDocCount;
        this.shardSize = shardSize;
        this.showTermDocCountError = showTermDocCountError;
        this.otherDocCount = otherDocCount;
        this.buckets = buckets;
        this.docCountError = docCountError;
        this.formats = formats;
        this.keyConverters = keyConverters;
    }

    public InternalMultiTerms(StreamInput in) throws IOException {
        super(in);
        reduceOrder = InternalOrder.Streams.readOrder(in);
        order = InternalOrder.Streams.readOrder(in);
        requiredSize = readSize(in);
        minDocCount = in.readVLong();
        docCountError = in.readZLong();
        shardSize = readSize(in);
        showTermDocCountError = in.readBoolean();
        otherDocCount = in.readVLong();
        formats = in.readCollectionAsList(in1 -> in1.readNamedWriteable(DocValueFormat.class));
        keyConverters = in.readCollectionAsList(in1 -> in1.readEnum(KeyConverter.class));
        buckets = in.readCollectionAsList(stream -> new Bucket(stream, formats, keyConverters, showTermDocCountError));
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        reduceOrder.writeTo(out);
        order.writeTo(out);
        writeSize(requiredSize, out);
        out.writeVLong(minDocCount);
        out.writeZLong(docCountError);
        writeSize(shardSize, out);
        out.writeBoolean(showTermDocCountError);
        out.writeVLong(otherDocCount);
        out.writeNamedWriteableCollection(formats);
        out.writeCollection(keyConverters, StreamOutput::writeEnum);
        out.writeVInt(buckets.size());
        for (var bucket : buckets) {
            bucket.writeTo(out, showTermDocCountError);
        }
    }

    @Override
    @SuppressWarnings("HiddenField")
    protected InternalMultiTerms create(
        String name,
        List<Bucket> buckets,
        BucketOrder reduceOrder,
        long docCountError,
        long otherDocCount
    ) {
        return new InternalMultiTerms(
            name,
            reduceOrder,
            order,
            requiredSize,
            minDocCount,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            buckets,
            docCountError,
            formats,
            keyConverters,
            getMetadata()
        );
    }

    @Override
    protected int getShardSize() {
        return shardSize;
    }

    @Override
    protected BucketOrder getReduceOrder() {
        return reduceOrder;
    }

    @Override
    protected BucketOrder getOrder() {
        return order;
    }

    @Override
    protected long getSumOfOtherDocCounts() {
        return otherDocCount;
    }

    @Override
    protected Long getDocCountError() {
        return docCountError;
    }

    @Override
    protected void setDocCountError(long docCountError) {
        this.docCountError = docCountError;
    }

    @Override
    protected long getMinDocCount() {
        return minDocCount;
    }

    @Override
    protected int getRequiredSize() {
        return requiredSize;
    }

    @Override
    protected boolean getShowDocCountError() {
        return showTermDocCountError;
    }

    @Override
    @SuppressWarnings("HiddenField")
    protected Bucket createBucket(long docCount, InternalAggregations aggs, long docCountError, Bucket prototype) {
        return new Bucket(prototype.terms, docCount, aggs, showTermDocCountError, docCountError, formats, keyConverters);
    }

    @Override
    @SuppressWarnings("HiddenField")
    public InternalMultiTerms create(List<Bucket> buckets) {
        return new InternalMultiTerms(
            name,
            reduceOrder,
            order,
            requiredSize,
            minDocCount,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            buckets,
            docCountError,
            formats,
            keyConverters,
            getMetadata()
        );
    }

    private InternalAggregation promoteToDouble(InternalAggregation aggregation, boolean[] needsPromotion) {
        InternalMultiTerms multiTerms = (InternalMultiTerms) aggregation;
        List<Bucket> multiTermsBuckets = multiTerms.getBuckets();
        List<List<Object>> newKeys = new ArrayList<>();
        for (InternalMultiTerms.Bucket bucket : multiTermsBuckets) {
            newKeys.add(new ArrayList<>(bucket.terms.size()));
        }

        List<KeyConverter> newKeyConverters = new ArrayList<>(multiTerms.keyConverters.size());
        for (int i = 0; i < needsPromotion.length; i++) {
            KeyConverter converter = multiTerms.keyConverters.get(i);
            DocValueFormat format = formats.get(i);
            if (needsPromotion[i]) {
                newKeyConverters.add(KeyConverter.DOUBLE);
                for (int j = 0; j < multiTermsBuckets.size(); j++) {
                    newKeys.get(j).add(converter.toDouble(format, multiTermsBuckets.get(j).terms.get(i)));
                }
            } else {
                newKeyConverters.add(converter);
                for (int j = 0; j < multiTermsBuckets.size(); j++) {
                    newKeys.get(j).add(multiTermsBuckets.get(j).terms.get(i));
                }
            }
        }

        List<Bucket> newBuckets = new ArrayList<>(multiTermsBuckets.size());
        for (int i = 0; i < multiTermsBuckets.size(); i++) {
            Bucket oldBucket = multiTermsBuckets.get(i);
            newBuckets.add(
                new Bucket(
                    newKeys.get(i),
                    oldBucket.docCount,
                    oldBucket.aggregations,
                    showTermDocCountError,
                    oldBucket.docCountError,
                    formats,
                    newKeyConverters
                )
            );
        }

        // During promotion we might have changed the keys by promoting longs to doubles and loosing precision
        // that might have caused some keys to now in a wrong order. So we need to resort.
        newBuckets.sort(reduceOrder.comparator());

        return new InternalMultiTerms(
            multiTerms.name,
            multiTerms.reduceOrder,
            multiTerms.order,
            multiTerms.requiredSize,
            multiTerms.minDocCount,
            multiTerms.shardSize,
            multiTerms.showTermDocCountError,
            multiTerms.otherDocCount,
            newBuckets,
            multiTerms.docCountError,
            multiTerms.formats,
            newKeyConverters,
            multiTerms.metadata
        );
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {

            private List<InternalAggregation> aggregations = new ArrayList<>(size);

            @Override
            public void accept(InternalAggregation aggregation) {
                aggregations.add(aggregation);
            }

            private List<InternalAggregation> getProcessedAggs(List<InternalAggregation> aggregations, boolean[] needsPromotionToDouble) {
                if (needsPromotionToDouble != null) {
                    aggregations.replaceAll(agg -> promoteToDouble(agg, needsPromotionToDouble));
                }
                return aggregations;
            }

            /**
             * Checks if any keys need to be promoted to double from long or unsigned_long
             */
            private boolean[] needsPromotionToDouble(List<InternalAggregation> aggregations) {
                if (aggregations.size() < 2) {
                    return null;
                }
                boolean[] promotions = null;

                for (int i = 0; i < keyConverters.size(); i++) {
                    boolean hasLong = false;
                    boolean hasUnsignedLong = false;
                    boolean hasDouble = false;
                    boolean hasNonNumber = false;
                    for (InternalAggregation aggregation : aggregations) {
                        InternalMultiTerms agg = (InternalMultiTerms) aggregation;
                        KeyConverter keyConverter = agg.keyConverters.get(i);
                        switch (keyConverter) {
                            case DOUBLE -> hasDouble = true;
                            case LONG -> hasLong = true;
                            case UNSIGNED_LONG -> hasUnsignedLong = true;
                            default -> hasNonNumber = true;
                        }
                    }
                    if (hasNonNumber && (hasDouble || hasUnsignedLong || hasLong)) {
                        throw AggregationErrors.reduceTypeMismatch(name, Optional.of(i + 1));
                    }
                    // Promotion to double is required if at least 2 of these 3 conditions are true.
                    if ((hasDouble ? 1 : 0) + (hasUnsignedLong ? 1 : 0) + (hasLong ? 1 : 0) > 1) {
                        if (promotions == null) {
                            promotions = new boolean[keyConverters.size()];
                        }
                        promotions[i] = true;
                    }
                }
                return promotions;
            }

            @Override
            public InternalAggregation get() {
                final boolean[] needsPromotionToDouble = needsPromotionToDouble(aggregations);
                if (needsPromotionToDouble != null) {
                    aggregations.replaceAll(agg -> promoteToDouble(agg, needsPromotionToDouble));
                }
                try (
                    AggregatorReducer processor = ((AbstractInternalTerms<?, ?>) aggregations.get(0)).termsAggregationReducer(
                        reduceContext,
                        size
                    )
                ) {
                    aggregations.forEach(processor::accept);
                    aggregations = null; // release memory
                    return processor.get();
                }
            }
        };
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.terms, prototype.docCount, aggregations, showTermDocCountError, docCountError, formats, keyConverters);
    }

    @Override
    public List<Bucket> getBuckets() {
        return buckets;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return doXContentCommon(builder, params, showTermDocCountError, docCountError, otherDocCount, buckets);
    }

    @Override
    public String getWriteableName() {
        return MultiTermsAggregationBuilder.NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        InternalMultiTerms that = (InternalMultiTerms) o;
        return requiredSize == that.requiredSize
            && minDocCount == that.minDocCount
            && shardSize == that.shardSize
            && showTermDocCountError == that.showTermDocCountError
            && otherDocCount == that.otherDocCount
            && docCountError == that.docCountError
            && Objects.equals(reduceOrder, that.reduceOrder)
            && Objects.equals(order, that.order)
            && Objects.equals(formats, that.formats)
            && Objects.equals(keyConverters, that.keyConverters)
            && Objects.equals(buckets, that.buckets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            reduceOrder,
            order,
            requiredSize,
            minDocCount,
            formats,
            keyConverters,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            buckets,
            docCountError
        );
    }

}
