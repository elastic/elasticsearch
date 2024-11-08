/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregationErrors;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.DelayedBucket;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.search.aggregations.TopBucketBuilder;
import org.elasticsearch.search.aggregations.bucket.IteratorAndCurrent;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static org.elasticsearch.search.aggregations.InternalOrder.isKeyAsc;
import static org.elasticsearch.search.aggregations.InternalOrder.isKeyOrder;
import static org.elasticsearch.search.aggregations.bucket.terms.InternalTerms.DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME;
import static org.elasticsearch.search.aggregations.bucket.terms.InternalTerms.SUM_OF_OTHER_DOC_COUNTS;

/**
 * Base class for terms and multi_terms aggregation that handles common reduce logic
 */
public abstract class AbstractInternalTerms<A extends AbstractInternalTerms<A, B>, B extends AbstractInternalTerms.AbstractTermsBucket<B>>
    extends InternalMultiBucketAggregation<A, B> {

    public AbstractInternalTerms(String name, Map<String, Object> metadata) {
        super(name, metadata);
    }

    protected AbstractInternalTerms(StreamInput in) throws IOException {

        super(in);
    }

    public abstract static class AbstractTermsBucket<B extends AbstractTermsBucket<B>> extends InternalMultiBucketAggregation.InternalBucket
        implements
            KeyComparable<B> {

        protected abstract void updateDocCountError(long docCountErrorDiff);

        protected abstract void setDocCountError(long docCountError);

        protected abstract boolean getShowDocCountError();

        protected abstract long getDocCountError();
    }

    /**
     * Creates InternalTerms at the end of the merge
     */
    protected abstract A create(String name, List<B> buckets, BucketOrder reduceOrder, long docCountError, long otherDocCount);

    protected abstract int getShardSize();

    protected abstract BucketOrder getReduceOrder();

    protected abstract BucketOrder getOrder();

    protected abstract long getSumOfOtherDocCounts();

    protected abstract Long getDocCountError();

    protected abstract void setDocCountError(long docCountError);

    protected abstract long getMinDocCount();

    protected abstract int getRequiredSize();

    protected abstract B createBucket(long docCount, InternalAggregations aggs, long docCountError, B prototype);

    private B reduceBucket(List<B> buckets, AggregationReduceContext context) {
        assert buckets.isEmpty() == false;
        long docCount = 0;
        // For the per term doc count error we add up the errors from the
        // shards that did not respond with the term. To do this we add up
        // the errors from the shards that did respond with the terms and
        // subtract that from the sum of the error from all shards
        long docCountError = 0;
        for (B bucket : buckets) {
            docCount += bucket.getDocCount();
            if (docCountError != -1) {
                if (bucket.getShowDocCountError() == false || bucket.getDocCountError() == -1) {
                    docCountError = -1;
                } else {
                    docCountError += bucket.getDocCountError();
                }
            }
        }
        final List<InternalAggregations> aggregations = new BucketAggregationList<>(buckets);
        final InternalAggregations aggs = InternalAggregations.reduce(aggregations, context);
        return createBucket(docCount, aggs, docCountError, buckets.get(0));
    }

    private long getDocCountError(A terms) {
        int size = terms.getBuckets().size();
        if (size == 0 || size < terms.getShardSize() || isKeyOrder(terms.getOrder())) {
            return 0;
        } else if (InternalOrder.isCountDesc(terms.getOrder())) {
            if (terms.getDocCountError() != null) {
                // If there is an existing docCountError for this agg then
                // use this as the error for this aggregation
                return terms.getDocCountError();
            } else {
                // otherwise use the doc count of the last term in the
                // aggregation
                return terms.getBuckets().stream().mapToLong(AbstractTermsBucket::getDocCount).min().getAsLong();
            }
        } else {
            return -1;
        }
    }

    /**
     * Reduce the buckets of sub-aggregations.
     * @param sink Handle the reduced buckets. Returns false if we should stop iterating the buckets, true if we should continue.
     * @return the order we used to reduce the buckets
     */
    private BucketOrder reduceBuckets(List<List<B>> bucketsList, BucketOrder thisReduceOrder, Consumer<DelayedBucket<B>> sink) {
        if (isKeyOrder(thisReduceOrder)) {
            // extract the primary sort in case this is a compound order.
            thisReduceOrder = InternalOrder.key(isKeyAsc(thisReduceOrder));
            reduceMergeSort(bucketsList, thisReduceOrder, sink);
        } else {
            reduceLegacy(bucketsList, sink);
        }
        return thisReduceOrder;
    }

    private void reduceMergeSort(List<List<B>> bucketsList, BucketOrder thisReduceOrder, Consumer<DelayedBucket<B>> sink) {
        assert isKeyOrder(thisReduceOrder);
        final Comparator<Bucket> cmp = thisReduceOrder.comparator();
        final PriorityQueue<IteratorAndCurrent<B>> pq = new PriorityQueue<>(bucketsList.size()) {
            @Override
            protected boolean lessThan(IteratorAndCurrent<B> a, IteratorAndCurrent<B> b) {
                return cmp.compare(a.current(), b.current()) < 0;
            }
        };
        for (List<B> buckets : bucketsList) {
            pq.add(new IteratorAndCurrent<>(buckets.iterator()));
        }
        // list of buckets coming from different shards that have the same key
        ArrayList<B> sameTermBuckets = new ArrayList<>();
        B lastBucket = null;
        while (pq.size() > 0) {
            final IteratorAndCurrent<B> top = pq.top();
            assert lastBucket == null || cmp.compare(top.current(), lastBucket) >= 0;
            if (lastBucket != null && cmp.compare(top.current(), lastBucket) != 0) {
                // the key changed so bundle up the last key's worth of buckets
                sameTermBuckets.trimToSize();
                sink.accept(new DelayedBucket<>(sameTermBuckets));
                sameTermBuckets = new ArrayList<>();
            }
            lastBucket = top.current();
            sameTermBuckets.add(top.current());
            if (top.hasNext()) {
                top.next();
                /*
                 * Typically the bucket keys are strictly increasing, but when we merge aggs from two different indices
                 * we can promote long and unsigned long keys to double, which can cause 2 long keys to be promoted into
                 * the same double key.
                 */
                assert cmp.compare(top.current(), lastBucket) >= 0 : "shards must return data sorted by key";
                pq.updateTop();
            } else {
                pq.pop();
            }
        }

        if (sameTermBuckets.isEmpty() == false) {
            sameTermBuckets.trimToSize();
            sink.accept(new DelayedBucket<>(sameTermBuckets));
        }
    }

    private void reduceLegacy(List<List<B>> bucketsList, Consumer<DelayedBucket<B>> sink) {
        final Map<Object, ArrayList<B>> bucketMap = new HashMap<>();
        for (List<B> buckets : bucketsList) {
            for (B bucket : buckets) {
                bucketMap.computeIfAbsent(bucket.getKey(), k -> new ArrayList<>()).add(bucket);
            }
        }
        for (ArrayList<B> sameTermBuckets : bucketMap.values()) {
            sameTermBuckets.trimToSize();
            sink.accept(new DelayedBucket<>(sameTermBuckets));
        }
    }

    public final AggregatorReducer termsAggregationReducer(AggregationReduceContext reduceContext, int size) {
        return new TermsAggregationReducer(reduceContext, size);
    }

    private class TermsAggregationReducer implements AggregatorReducer {
        private final List<List<B>> bucketsList;
        private final AggregationReduceContext reduceContext;
        private final int size;

        private long sumDocCountError = 0;
        private final long[] otherDocCount = new long[] { 0 };
        private A referenceTerms = null;
        /*
         * Buckets returned by a partial reduce or a shard response are sorted by key since {@link Version#V_7_10_0}.
         * That allows to perform a merge sort when reducing multiple aggregations together.
         * For backward compatibility, we disable the merge sort and use ({@link #reduceLegacy} if any of
         * the provided aggregations use a different {@link #reduceOrder}.
         */
        private BucketOrder thisReduceOrder = null;

        private TermsAggregationReducer(AggregationReduceContext reduceContext, int size) {
            bucketsList = new ArrayList<>(size);
            this.reduceContext = reduceContext;
            this.size = size;
        }

        @Override
        public void accept(InternalAggregation aggregation) {
            if (aggregation.canLeadReduction() == false) {
                return;
            }
            @SuppressWarnings("unchecked")
            A terms = (A) aggregation;
            if (referenceTerms == null) {
                referenceTerms = terms;
            } else if (referenceTerms.getClass().equals(terms.getClass()) == false) {
                // control gets into this loop when the same field name against which the query is executed
                // is of different types in different indices.
                throw AggregationErrors.reduceTypeMismatch(referenceTerms.getName(), Optional.empty());
            }
            if (thisReduceOrder == null) {
                thisReduceOrder = terms.getReduceOrder();
            } else if (thisReduceOrder != getOrder() && thisReduceOrder.equals(terms.getReduceOrder()) == false) {
                thisReduceOrder = getOrder();
            }
            otherDocCount[0] += terms.getSumOfOtherDocCounts();
            final long thisAggDocCountError = getDocCountError(terms);
            if (sumDocCountError != -1) {
                if (thisAggDocCountError == -1) {
                    sumDocCountError = -1;
                } else {
                    sumDocCountError += thisAggDocCountError;
                }
            }
            setDocCountError(thisAggDocCountError);
            for (B bucket : terms.getBuckets()) {
                // If there is already a doc count error for this bucket
                // subtract this aggs doc count error from it to make the
                // new value for the bucket. This then means that when the
                // final error for the bucket is calculated below we account
                // for the existing error calculated in a previous reduce.
                // Note that if the error is unbounded (-1) this will be fixed
                // later in this method.
                bucket.updateDocCountError(-thisAggDocCountError);
            }
            if (terms.getBuckets().isEmpty() == false) {
                bucketsList.add(terms.getBuckets());
            }
        }

        @Override
        public InternalAggregation get() {
            BucketOrder thisReduceOrder;
            List<B> result;
            if (isKeyOrder(getOrder()) && getMinDocCount() <= 1) {
                /*
                 * the aggregation is order by key and not filtered on doc count. The results come in key order
                 * so we can just have an optimize collection.
                 */
                result = new ArrayList<>();
                thisReduceOrder = reduceBuckets(bucketsList, getThisReduceOrder(), bucket -> {
                    if (result.size() < getRequiredSize()) {
                        reduceContext.consumeBucketsAndMaybeBreak(1);
                        result.add(bucket.reduced(AbstractInternalTerms.this::reduceBucket, reduceContext));
                    } else {
                        otherDocCount[0] += bucket.getDocCount();
                    }
                });
            } else if (reduceContext.isFinalReduce()) {
                TopBucketBuilder<B> top = TopBucketBuilder.build(
                    getRequiredSize(),
                    getOrder(),
                    removed -> otherDocCount[0] += removed.getDocCount(),
                    AbstractInternalTerms.this::reduceBucket,
                    reduceContext
                );
                thisReduceOrder = reduceBuckets(bucketsList, getThisReduceOrder(), bucket -> {
                    if (bucket.getDocCount() >= getMinDocCount()) {
                        top.add(bucket);
                    }
                });
                result = top.build();
            } else {
                result = new ArrayList<>();
                thisReduceOrder = reduceBuckets(bucketsList, getThisReduceOrder(), bucket -> {
                    reduceContext.consumeBucketsAndMaybeBreak(1);
                    result.add(bucket.reduced(AbstractInternalTerms.this::reduceBucket, reduceContext));
                });
            }
            for (B r : result) {
                if (sumDocCountError == -1) {
                    r.setDocCountError(-1);
                } else {
                    r.updateDocCountError(sumDocCountError);
                }
            }
            long docCountError;
            if (sumDocCountError == -1) {
                docCountError = -1;
            } else {
                docCountError = size == 1 ? 0 : sumDocCountError;
            }
            return create(name, result, reduceContext.isFinalReduce() ? getOrder() : thisReduceOrder, docCountError, otherDocCount[0]);
        }

        private BucketOrder getThisReduceOrder() {
            return thisReduceOrder == null ? getOrder() : thisReduceOrder;
        }
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return create(
            name,
            getBuckets().stream()
                .map(
                    b -> createBucket(
                        samplingContext.scaleUp(b.getDocCount()),
                        InternalAggregations.finalizeSampling(b.getAggregations(), samplingContext),
                        b.getShowDocCountError() ? samplingContext.scaleUp(b.getDocCountError()) : 0,
                        b
                    )
                )
                .toList(),
            getOrder(),
            samplingContext.scaleUp(getDocCountError()),
            samplingContext.scaleUp(getSumOfOtherDocCounts())
        );
    }

    protected static XContentBuilder doXContentCommon(
        XContentBuilder builder,
        Params params,
        Long docCountError,
        long otherDocCount,
        List<? extends AbstractTermsBucket<?>> buckets
    ) throws IOException {
        builder.field(DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME.getPreferredName(), docCountError);
        builder.field(SUM_OF_OTHER_DOC_COUNTS.getPreferredName(), otherDocCount);
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (AbstractTermsBucket<?> bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

}
