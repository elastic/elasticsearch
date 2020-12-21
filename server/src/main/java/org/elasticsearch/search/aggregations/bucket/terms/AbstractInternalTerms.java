/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.bucket.IteratorAndCurrent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.InternalOrder.isKeyAsc;
import static org.elasticsearch.search.aggregations.InternalOrder.isKeyOrder;

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
     * Create an array to hold some buckets. Used in collecting the results.
     */
    protected abstract B[] createBucketsArray(int size);

    /**
     * Creates InternalTerms at the end of the merge
     */
    protected abstract A create(String name, List<B> buckets, BucketOrder reduceOrder, long docCountError, long otherDocCount);

    protected abstract int getShardSize();

    protected abstract BucketOrder getReduceOrder();

    protected abstract BucketOrder getOrder();

    protected abstract long getSumOfOtherDocCounts();

    protected abstract long getDocCountError();

    protected abstract void setDocCountError(long docCountError);

    protected abstract long getMinDocCount();

    protected abstract int getRequiredSize();

    abstract B createBucket(long docCount, InternalAggregations aggs, long docCountError, B prototype);

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
            if (terms.getDocCountError() > 0) {
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

    private List<B> reduceMergeSort(List<InternalAggregation> aggregations,
                                    BucketOrder thisReduceOrder, InternalAggregation.ReduceContext reduceContext) {
        assert isKeyOrder(thisReduceOrder);
        final Comparator<Bucket> cmp = thisReduceOrder.comparator();
        final PriorityQueue<IteratorAndCurrent<B>> pq = new PriorityQueue<>(aggregations.size()) {
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
        List<B> reducedBuckets = new ArrayList<>();
        // list of buckets coming from different shards that have the same key
        List<B> currentBuckets = new ArrayList<>();
        B lastBucket = null;
        while (pq.size() > 0) {
            final IteratorAndCurrent<B> top = pq.top();
            assert lastBucket == null || cmp.compare(top.current(), lastBucket) >= 0;
            if (lastBucket != null && cmp.compare(top.current(), lastBucket) != 0) {
                // the key changes, reduce what we already buffered and reset the buffer for current buckets
                final B reduced = reduceBucket(currentBuckets, reduceContext);
                reducedBuckets.add(reduced);
                currentBuckets.clear();
            }
            lastBucket = top.current();
            currentBuckets.add(top.current());
            if (top.hasNext()) {
                top.next();
                assert cmp.compare(top.current(), lastBucket) > 0 : "shards must return data sorted by key";
                pq.updateTop();
            } else {
                pq.pop();
            }
        }

        if (currentBuckets.isEmpty() == false) {
            final B reduced = reduceBucket(currentBuckets, reduceContext);
            reducedBuckets.add(reduced);
        }
        return reducedBuckets;
    }

    private List<B> reduceLegacy(List<InternalAggregation> aggregations, InternalAggregation.ReduceContext reduceContext) {
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
        List<B> reducedBuckets = new ArrayList<>();
        for (List<B> sameTermBuckets : bucketMap.values()) {
            final B b = reduceBucket(sameTermBuckets, reduceContext);
            reducedBuckets.add(b);
        }
        return reducedBuckets;
    }

    public InternalAggregation reduce(List<InternalAggregation> aggregations, InternalAggregation.ReduceContext reduceContext) {
        long sumDocCountError = 0;
        long otherDocCount = 0;
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
            otherDocCount += terms.getSumOfOtherDocCounts();
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

        final List<B> reducedBuckets;
        /**
         * Buckets returned by a partial reduce or a shard response are sorted by key since {@link Version#V_7_10_0}.
         * That allows to perform a merge sort when reducing multiple aggregations together.
         * For backward compatibility, we disable the merge sort and use ({@link #reduceLegacy} if any of
         * the provided aggregations use a different {@link #reduceOrder}.
         */
        BucketOrder thisReduceOrder = getReduceOrder(aggregations);
        if (isKeyOrder(thisReduceOrder)) {
            // extract the primary sort in case this is a compound order.
            thisReduceOrder = InternalOrder.key(isKeyAsc(thisReduceOrder));
            reducedBuckets = reduceMergeSort(aggregations, thisReduceOrder, reduceContext);
        } else {
            reducedBuckets = reduceLegacy(aggregations, reduceContext);
        }
        final B[] list;
        if (reduceContext.isFinalReduce()) {
            final int size = Math.min(getRequiredSize(), reducedBuckets.size());
            // final comparator
            final BucketPriorityQueue<B> ordered = new BucketPriorityQueue<>(size, getOrder().comparator());
            for (B bucket : reducedBuckets) {
                if (sumDocCountError == -1) {
                    bucket.setDocCountError(-1);
                } else {
                    bucket.updateDocCountError(sumDocCountError);
                }
                if (bucket.getDocCount() >= getMinDocCount()) {
                    B removed = ordered.insertWithOverflow(bucket);
                    if (removed != null) {
                        otherDocCount += removed.getDocCount();
                        reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(removed));
                    } else {
                        reduceContext.consumeBucketsAndMaybeBreak(1);
                    }
                } else {
                    reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(bucket));
                }
            }
            list = createBucketsArray(ordered.size());
            for (int i = ordered.size() - 1; i >= 0; i--) {
                list[i] = ordered.pop();
            }
        } else {
            // we can prune the list on partial reduce if the aggregation is ordered by key
            // and not filtered (minDocCount == 0)
            int size = isKeyOrder(getOrder()) && getMinDocCount() == 0 ? Math.min(getRequiredSize(), reducedBuckets.size()) :
                reducedBuckets.size();
            list = createBucketsArray(size);
            for (int i = 0; i < size; i++) {
                reduceContext.consumeBucketsAndMaybeBreak(1);
                list[i] = reducedBuckets.get(i);
                if (sumDocCountError == -1) {
                    list[i].setDocCountError(-1);
                } else {
                    list[i].updateDocCountError(sumDocCountError);
                }
            }
        }
        long docCountError;
        if (sumDocCountError == -1) {
            docCountError = -1;
        } else {
            docCountError = aggregations.size() == 1 ? 0 : sumDocCountError;
        }
        return create(name, Arrays.asList(list), reduceContext.isFinalReduce() ? getOrder() : thisReduceOrder, docCountError,
            otherDocCount);
    }

}
