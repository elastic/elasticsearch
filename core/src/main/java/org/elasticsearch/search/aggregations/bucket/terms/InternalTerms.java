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
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.support.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public abstract class InternalTerms<A extends InternalTerms, B extends InternalTerms.Bucket> extends InternalMultiBucketAggregation<A, B>
        implements Terms, ToXContent, Streamable {

    protected static final String DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME = "doc_count_error_upper_bound";
    protected static final String SUM_OF_OTHER_DOC_COUNTS = "sum_other_doc_count";

    public static abstract class Bucket extends Terms.Bucket {

        long bucketOrd;

        protected long docCount;
        protected long docCountError;
        protected InternalAggregations aggregations;
        protected boolean showDocCountError;
        transient final ValueFormatter formatter;

        protected Bucket(ValueFormatter formatter, boolean showDocCountError) {
            // for serialization
            this.showDocCountError = showDocCountError;
            this.formatter = formatter;
        }

        protected Bucket(long docCount, InternalAggregations aggregations, boolean showDocCountError, long docCountError,
                ValueFormatter formatter) {
            this(formatter, showDocCountError);
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.docCountError = docCountError;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public long getDocCountError() {
            if (!showDocCountError) {
                throw new IllegalStateException("show_terms_doc_count_error is false");
            }
            return docCountError;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        abstract Bucket newBucket(long docCount, InternalAggregations aggs, long docCountError);

        public Bucket reduce(List<? extends Bucket> buckets, ReduceContext context) {
            long docCount = 0;
            long docCountError = 0;
            List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
            for (Bucket bucket : buckets) {
                docCount += bucket.docCount;
                if (docCountError != -1) {
                    if (bucket.docCountError == -1) {
                        docCountError = -1;
                    } else {
                        docCountError += bucket.docCountError;
                    }
                }
                aggregationsList.add(bucket.aggregations);
            }
            InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
            return newBucket(docCount, aggs, docCountError);
        }
    }

    protected Terms.Order order;
    protected int requiredSize;
    protected int shardSize;
    protected long minDocCount;
    protected List<? extends Bucket> buckets;
    protected Map<String, Bucket> bucketMap;
    protected long docCountError;
    protected boolean showTermDocCountError;
    protected long otherDocCount;

    protected InternalTerms() {} // for serialization

    protected InternalTerms(String name, Terms.Order order, int requiredSize, int shardSize, long minDocCount,
            List<? extends Bucket> buckets, boolean showTermDocCountError, long docCountError, long otherDocCount, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.order = order;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        this.minDocCount = minDocCount;
        this.buckets = buckets;
        this.showTermDocCountError = showTermDocCountError;
        this.docCountError = docCountError;
        this.otherDocCount = otherDocCount;
    }

    @Override
    public List<Terms.Bucket> getBuckets() {
        Object o = buckets;
        return (List<Terms.Bucket>) o;
    }

    @Override
    public Terms.Bucket getBucketByKey(String term) {
        if (bucketMap == null) {
            bucketMap = new HashMap<>(buckets.size());
            for (Bucket bucket : buckets) {
                bucketMap.put(bucket.getKeyAsString(), bucket);
            }
        }
        return bucketMap.get(term);
    }

    @Override
    public long getDocCountError() {
        return docCountError;
    }

    @Override
    public long getSumOfOtherDocCounts() {
        return otherDocCount;
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {

        Map<Object, List<InternalTerms.Bucket>> buckets = new HashMap<>();
        long sumDocCountError = 0;
        long otherDocCount = 0;
        InternalTerms<A, B> referenceTerms = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalTerms<A, B> terms = (InternalTerms<A, B>) aggregation;
            if (referenceTerms == null && !terms.getClass().equals(UnmappedTerms.class)) {
                referenceTerms = (InternalTerms<A, B>) aggregation;
            }
            if (referenceTerms != null &&
                    !referenceTerms.getClass().equals(terms.getClass()) &&
                    !terms.getClass().equals(UnmappedTerms.class)) {
                // control gets into this loop when the same field name against which the query is executed
                // is of different types in different indices.
                throw new AggregationExecutionException("Merging/Reducing the aggregations failed " +
                                                        "when computing the aggregation [ Name: " +
                                                        referenceTerms.getName() + ", Type: " +
                                                        referenceTerms.type() + " ]" + " because: " +
                                                        "the field you gave in the aggregation query " +
                                                        "existed as two different types " +
                                                        "in two different indices");
            }
            otherDocCount += terms.getSumOfOtherDocCounts();
            final long thisAggDocCountError;
            if (terms.buckets.size() < this.shardSize || this.order == InternalOrder.TERM_ASC || this.order == InternalOrder.TERM_DESC) {
                thisAggDocCountError = 0;
            } else if (InternalOrder.isCountDesc(this.order)) {
                thisAggDocCountError = terms.buckets.get(terms.buckets.size() - 1).docCount;
            } else {
                thisAggDocCountError = -1;
            }
            if (sumDocCountError != -1) {
                if (thisAggDocCountError == -1) {
                    sumDocCountError = -1;
                } else {
                    sumDocCountError += thisAggDocCountError;
                }
            }
            terms.docCountError = thisAggDocCountError;
            for (Bucket bucket : terms.buckets) {
                bucket.docCountError = thisAggDocCountError;
                List<Bucket> bucketList = buckets.get(bucket.getKey());
                if (bucketList == null) {
                    bucketList = new ArrayList<>();
                    buckets.put(bucket.getKey(), bucketList);
                }
                bucketList.add(bucket);
            }
        }

        final int size = Math.min(requiredSize, buckets.size());
        BucketPriorityQueue ordered = new BucketPriorityQueue(size, order.comparator(null));
        for (List<Bucket> sameTermBuckets : buckets.values()) {
            final Bucket b = sameTermBuckets.get(0).reduce(sameTermBuckets, reduceContext);
            if (b.docCountError != -1) {
                if (sumDocCountError == -1) {
                    b.docCountError = -1;
                } else {
                    b.docCountError = sumDocCountError - b.docCountError;
                }
            }
            if (b.docCount >= minDocCount) {
                Terms.Bucket removed = ordered.insertWithOverflow(b);
                if (removed != null) {
                    otherDocCount += removed.getDocCount();
                }
            }
        }
        Bucket[] list = new Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            list[i] = (Bucket) ordered.pop();
        }
        long docCountError;
        if (sumDocCountError == -1) {
            docCountError = -1;
        } else {
            docCountError = aggregations.size() == 1 ? 0 : sumDocCountError;
        }
        return create(name, Arrays.asList(list), docCountError, otherDocCount, this);
    }

    protected abstract A create(String name, List<InternalTerms.Bucket> buckets, long docCountError, long otherDocCount,
            InternalTerms prototype);

}
