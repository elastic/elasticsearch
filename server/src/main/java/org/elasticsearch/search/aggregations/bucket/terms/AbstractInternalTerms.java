/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.DelayedBucket;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.TopBucketBuilder;
import org.elasticsearch.search.aggregations.bucket.IteratorAndCurrent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.search.aggregations.InternalOrder.isKeyAsc;
import static org.elasticsearch.search.aggregations.InternalOrder.isKeyOrder;
import static org.elasticsearch.search.aggregations.bucket.terms.InternalTerms.DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME;
import static org.elasticsearch.search.aggregations.bucket.terms.InternalTerms.SUM_OF_OTHER_DOC_COUNTS;

/**
 * Base class for terms and multi_terms aggregation that handles common reduce logic
 */
public abstract class AbstractInternalTerms<
    A extends AbstractInternalTerms<A, B>,
    B extends AbstractInternalTerms.AbstractTermsBucket
    > extends InternalMultiBucketAggregation<A, B> {

    public AbstractInternalTerms(String name,
                                 Map<String, Object> metadata) {
        super(name, metadata);
    }

    protected AbstractInternalTerms(StreamInput in) throws IOException {

        super(in);
    }

    public abstract static class AbstractTermsBucket extends InternalMultiBucketAggregation.InternalBucket {

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

    @Override
    public B reduceBucket(List<B> buckets, ReduceContext context) {
        assert buckets.size() > 0;
        long docCount = 0;
        // For the per term doc count error we add up the errors from the
        // shards that did not respond with the term. To do this we add up
        // the errors from the shards that did respond with the terms and
        // subtract that from the sum of the error from all shards
        long docCountError = 0;
        List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
        for (B bucket : buckets) {
            docCount += bucket.getDocCount();
            if (docCountError != -1) {
                if (bucket.getShowDocCountError() == false || bucket.getDocCountError() == -1) {
                    docCountError = -1;
                } else {
                    docCountError += bucket.getDocCountError();
                }
            }
            aggregationsList.add((InternalAggregations) bucket.getAggregations());
        }
        InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
        return createBucket(docCount, aggs, docCountError, buckets.get(0));
    }

    private BucketOrder getReduceOrder(List<InternalAggregation> aggregations) {
        BucketOrder thisReduceOrder = null;
        for (InternalAggregation aggregation : aggregations) {
            @SuppressWarnings("unchecked")
            A terms = (A) aggregation;
            if (terms.getBuckets().size() == 0) {
                continue;
            }
            if (thisReduceOrder == null) {
                thisReduceOrder = terms.getReduceOrder();
            } else if (thisReduceOrder.equals(terms.getReduceOrder()) == false) {
                return getOrder();
            }
        }
        return thisReduceOrder != null ? thisReduceOrder : getOrder();
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
    private BucketOrder reduceBuckets(
        List<InternalAggregation> aggregations,
        InternalAggregation.ReduceContext reduceContext,
        Function<DelayedBucket<B>, Boolean> sink
    ) {
        /*
         * Buckets returned by a partial reduce or a shard response are sorted by key since {@link Version#V_7_10_0}.
         * That allows to perform a merge sort when reducing multiple aggregations together.
         * For backward compatibility, we disable the merge sort and use ({@link #reduceLegacy} if any of
         * the provided aggregations use a different {@link #reduceOrder}.
         */
        BucketOrder thisReduceOrder = getReduceOrder(aggregations);
        if (isKeyOrder(thisReduceOrder)) {
            // extract the primary sort in case this is a compound order.
            thisReduceOrder = InternalOrder.key(isKeyAsc(thisReduceOrder));
            reduceMergeSort(aggregations, thisReduceOrder, reduceContext, sink);
        } else {
            reduceLegacy(aggregations, reduceContext, sink);
        }
        return thisReduceOrder;
    }

    private void reduceMergeSort(
        List<InternalAggregation> aggregations,
        BucketOrder thisReduceOrder,
        InternalAggregation.ReduceContext reduceContext,
        Function<DelayedBucket<B>, Boolean> sink
    ) {
        assert isKeyOrder(thisReduceOrder);
        final Comparator<Bucket> cmp = thisReduceOrder.comparator();
        final PriorityQueue<IteratorAndCurrent<B>> pq = new PriorityQueue<IteratorAndCurrent<B>>(aggregations.size()) {
            @Override
            protected boolean lessThan(IteratorAndCurrent<B> a, IteratorAndCurrent<B> b) {
                return cmp.compare(a.current(), b.current()) < 0;
            }
        };
        for (InternalAggregation aggregation : aggregations) {
            @SuppressWarnings("unchecked")
            A terms = (A) aggregation;
            if (terms.getBuckets().isEmpty() == false) {
                pq.add(new IteratorAndCurrent<>(terms.getBuckets().iterator()));
            }
        }
        // list of buckets coming from different shards that have the same key
        List<B> sameTermBuckets = new ArrayList<>();
        B lastBucket = null;
        while (pq.size() > 0) {
            final IteratorAndCurrent<B> top = pq.top();
            assert lastBucket == null || cmp.compare(top.current(), lastBucket) >= 0;
            if (lastBucket != null && cmp.compare(top.current(), lastBucket) != 0) {
                // the key changed so bundle up the last key's worth of buckets
                boolean shouldContinue = sink.apply(
                    new DelayedBucket<B>(AbstractInternalTerms.this::reduceBucket, reduceContext, sameTermBuckets)
                );
                if (false == shouldContinue) {
                    return;
                }
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
            sink.apply(new DelayedBucket<B>(AbstractInternalTerms.this::reduceBucket, reduceContext, sameTermBuckets));
        }
    }

    private void reduceLegacy(
        List<InternalAggregation> aggregations,
        InternalAggregation.ReduceContext reduceContext,
        Function<DelayedBucket<B>, Boolean> sink
    ) {
        Map<Object, List<B>> bucketMap = new HashMap<>();
        for (InternalAggregation aggregation : aggregations) {
            @SuppressWarnings("unchecked")
            A terms = (A) aggregation;
            if (terms.getBuckets().isEmpty() == false) {
                for (B bucket : terms.getBuckets()) {
                    bucketMap.computeIfAbsent(bucket.getKey(), k -> new ArrayList<>()).add(bucket);
                }
            }
        }
        for (List<B> sameTermBuckets : bucketMap.values()) {
            boolean shouldContinue = sink.apply(
                new DelayedBucket<B>(AbstractInternalTerms.this::reduceBucket, reduceContext, sameTermBuckets)
            );
            if (false == shouldContinue) {
                return;
            }
        }
    }

    public InternalAggregation reduce(List<InternalAggregation> aggregations, InternalAggregation.ReduceContext reduceContext) {
        long sumDocCountError = 0;
        long[] otherDocCount = new long[] {0};
        A referenceTerms = null;
        for (InternalAggregation aggregation : aggregations) {
            @SuppressWarnings("unchecked")
            A terms = (A) aggregation;
            if (referenceTerms == null && terms.isMapped()) {
                referenceTerms = terms;
            }
            if (referenceTerms != null && referenceTerms.getClass().equals(terms.getClass()) == false && terms.isMapped()) {
                // control gets into this loop when the same field name against which the query is executed
                // is of different types in different indices.
                throw new AggregationExecutionException("Merging/Reducing the aggregations failed when computing the aggregation ["
                    + referenceTerms.getName() + "] because the field you gave in the aggregation query existed as two different "
                    + "types in two different indices");
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
        }

        BucketOrder thisReduceOrder;
        List<B> result;
        if (reduceContext.isFinalReduce()) {
            TopBucketBuilder<B> top = new TopBucketBuilder<>(getRequiredSize(), getOrder(), removed -> {
                otherDocCount[0] += removed.getDocCount();
            });
            thisReduceOrder = reduceBuckets(aggregations, reduceContext, bucket -> {
                if (bucket.getDocCount() >= getMinDocCount()) {
                    top.add(bucket);
                }
                return true;
            });
            result = top.build();
        } else {
            /*
             * We can prune the list on partial reduce if the aggregation is ordered
             * by key and not filtered on doc count. The results come in key order
             * so we can just stop iteration early.
             */
            boolean canPrune = isKeyOrder(getOrder()) && getMinDocCount() == 0;
            result = new ArrayList<>();
            thisReduceOrder = reduceBuckets(aggregations, reduceContext, bucket -> {
                result.add(bucket.reduced());
                return false == canPrune || result.size() < getRequiredSize();
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
            docCountError = aggregations.size() == 1 ? 0 : sumDocCountError;
        }
        return create(name, result, reduceContext.isFinalReduce() ? getOrder() : thisReduceOrder, docCountError, otherDocCount[0]);
    }

    protected static XContentBuilder doXContentCommon(XContentBuilder builder,
                                                      Params params,
                                                      Long docCountError,
                                                      long otherDocCount,
                                                      List<? extends AbstractTermsBucket> buckets) throws IOException {
        builder.field(DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME.getPreferredName(), docCountError);
        builder.field(SUM_OF_OTHER_DOC_COUNTS.getPreferredName(), otherDocCount);
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (AbstractTermsBucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

}
