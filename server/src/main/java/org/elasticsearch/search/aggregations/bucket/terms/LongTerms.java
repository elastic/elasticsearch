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
import org.elasticsearch.core.Predicates;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Result of the {@link TermsAggregator} when the field is some kind of whole number like a integer, long, or a date.
 */
public class LongTerms extends InternalMappedTerms<LongTerms, LongTerms.Bucket> {
    public static final String NAME = "lterms";

    public static class Bucket extends InternalTerms.Bucket<Bucket> {
        long term;

        public Bucket(
            long term,
            long docCount,
            InternalAggregations aggregations,
            boolean showDocCountError,
            long docCountError,
            DocValueFormat format
        ) {
            super(docCount, aggregations, showDocCountError, docCountError, format);
            this.term = term;
        }

        /**
         * Read from a stream.
         */
        public Bucket(StreamInput in, DocValueFormat format, boolean showDocCountError) throws IOException {
            super(in, format, showDocCountError);
            term = in.readLong();
        }

        @Override
        protected void writeTermTo(StreamOutput out) throws IOException {
            out.writeLong(term);
        }

        @Override
        public String getKeyAsString() {
            return format.format(term).toString();
        }

        @Override
        public Object getKey() {
            if (format == DocValueFormat.UNSIGNED_LONG_SHIFTED) {
                return format.format(term);
            } else {
                return term;
            }
        }

        @Override
        public Number getKeyAsNumber() {
            if (format == DocValueFormat.UNSIGNED_LONG_SHIFTED) {
                return (Number) format.format(term);
            } else {
                return term;
            }
        }

        @Override
        public int compareKey(Bucket other) {
            return Long.compare(term, other.term);
        }

        @Override
        protected final XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            if (format == DocValueFormat.UNSIGNED_LONG_SHIFTED) {
                builder.field(CommonFields.KEY.getPreferredName(), format.format(term));
            } else {
                builder.field(CommonFields.KEY.getPreferredName(), term);
            }
            if (format != DocValueFormat.RAW && format != DocValueFormat.UNSIGNED_LONG_SHIFTED) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), format.format(term).toString());
            }
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj) && Objects.equals(term, ((Bucket) obj).term);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), term);
        }
    }

    public LongTerms(
        String name,
        BucketOrder reduceOrder,
        BucketOrder order,
        int requiredSize,
        long minDocCount,
        Map<String, Object> metadata,
        DocValueFormat format,
        int shardSize,
        boolean showTermDocCountError,
        long otherDocCount,
        List<Bucket> buckets,
        Long docCountError
    ) {
        super(
            name,
            reduceOrder,
            order,
            requiredSize,
            minDocCount,
            metadata,
            format,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            buckets,
            docCountError
        );
    }

    /**
     * Read from a stream.
     */
    public LongTerms(StreamInput in) throws IOException {
        super(in, Bucket::new);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public LongTerms create(List<Bucket> buckets) {
        return new LongTerms(
            name,
            reduceOrder,
            order,
            requiredSize,
            minDocCount,
            metadata,
            format,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            buckets,
            docCountError
        );
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(
            prototype.term,
            prototype.docCount,
            aggregations,
            prototype.showDocCountError,
            prototype.docCountError,
            prototype.format
        );
    }

    @Override
    protected LongTerms create(String name, List<Bucket> buckets, BucketOrder reduceOrder, long docCountError, long otherDocCount) {
        return new LongTerms(
            name,
            reduceOrder,
            order,
            requiredSize,
            minDocCount,
            getMetadata(),
            format,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            buckets,
            docCountError
        );
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        final Predicate<DocValueFormat> needsPromoting;
        if (format == DocValueFormat.RAW) {
            needsPromoting = docFormat -> docFormat == DocValueFormat.UNSIGNED_LONG_SHIFTED;
        } else if (format == DocValueFormat.UNSIGNED_LONG_SHIFTED) {
            needsPromoting = docFormat -> docFormat == DocValueFormat.RAW;
        } else {
            needsPromoting = Predicates.never();
        }
        return new AggregatorReducer() {

            final List<InternalAggregation> aggregations = new ArrayList<>(size);
            boolean isPromotedToDouble = false;

            @Override
            public void accept(InternalAggregation aggregation) {
                if (aggregation instanceof DoubleTerms doubleTerms) {
                    if (isPromotedToDouble == false) {
                        promoteToDouble(aggregations);
                        isPromotedToDouble = true;
                    }
                    aggregations.add(doubleTerms);
                } else if (aggregation instanceof LongTerms longTerms) {
                    if (isPromotedToDouble || needsPromoting.test(longTerms.format)) {
                        if (isPromotedToDouble == false) {
                            promoteToDouble(aggregations);
                            isPromotedToDouble = true;
                        }
                        aggregations.add(LongTerms.convertLongTermsToDouble(longTerms, format));
                    } else {
                        aggregations.add(aggregation);
                    }
                }
            }

            private void promoteToDouble(List<InternalAggregation> aggregations) {
                aggregations.replaceAll(aggregation -> LongTerms.convertLongTermsToDouble((LongTerms) aggregation, format));
            }

            @Override
            public InternalAggregation get() {
                return ((AbstractInternalTerms<?, ?>) aggregations.get(0)).doReduce(aggregations, reduceContext);
            }
        };
    }

    @Override
    protected Bucket createBucket(long docCount, InternalAggregations aggs, long docCountError, LongTerms.Bucket prototype) {
        return new Bucket(prototype.term, docCount, aggs, prototype.showDocCountError, docCountError, format);
    }

    /**
     * Converts a {@link LongTerms} into a {@link DoubleTerms}, returning the value of the specified long terms as doubles.
     */
    public static DoubleTerms convertLongTermsToDouble(LongTerms longTerms, DocValueFormat decimalFormat) {
        List<LongTerms.Bucket> buckets = longTerms.getBuckets();
        List<DoubleTerms.Bucket> newBuckets = new ArrayList<>();
        for (Terms.Bucket bucket : buckets) {
            newBuckets.add(
                new DoubleTerms.Bucket(
                    bucket.getKeyAsNumber().doubleValue(),
                    bucket.getDocCount(),
                    bucket.getAggregations(),
                    longTerms.showTermDocCountError,
                    longTerms.showTermDocCountError ? bucket.getDocCountError() : 0,
                    decimalFormat
                )
            );
        }
        return new DoubleTerms(
            longTerms.getName(),
            longTerms.reduceOrder,
            longTerms.order,
            longTerms.requiredSize,
            longTerms.minDocCount,
            longTerms.metadata,
            longTerms.format,
            longTerms.shardSize,
            longTerms.showTermDocCountError,
            longTerms.otherDocCount,
            newBuckets,
            longTerms.docCountError
        );
    }
}
