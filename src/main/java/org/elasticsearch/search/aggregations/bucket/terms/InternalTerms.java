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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.support.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 */
public abstract class InternalTerms extends InternalMultiBucketAggregation implements Terms, ToXContent, Streamable {

    protected static final String DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME = "doc_count_error_upper_bound";
    protected static final String SUM_OF_OTHER_DOC_COUNTS = "sum_other_doc_count";

    public static abstract class Bucket extends Terms.Bucket {

        long bucketOrd;

        protected long docCount;
        protected long docCountError;
        protected InternalAggregations aggregations;
        protected boolean showDocCountError;
        transient final ValueFormatter formatter;

        protected Bucket(@Nullable ValueFormatter formatter, boolean showDocCountError) {
            // for serialization
            this.showDocCountError = showDocCountError;
            this.formatter = formatter;
        }

        protected Bucket(long docCount, InternalAggregations aggregations, boolean showDocCountError, long docCountError, @Nullable ValueFormatter formatter) {
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
                throw new ElasticsearchIllegalStateException("show_terms_doc_count_error is false");
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
    protected List<Bucket> buckets;
    protected Map<String, Bucket> bucketMap;
    protected long docCountError;
    protected boolean showTermDocCountError;
    protected long otherDocCount;

    protected InternalTerms() {} // for serialization

    protected InternalTerms(String name, Terms.Order order, int requiredSize, int shardSize, long minDocCount, List<Bucket> buckets, boolean showTermDocCountError, long docCountError, long otherDocCount, Map<String, Object> metaData) {
        super(name, metaData);
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
            bucketMap = Maps.newHashMapWithExpectedSize(buckets.size());
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
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {

        Multimap<Object, InternalTerms.Bucket> buckets = ArrayListMultimap.create();
        long sumDocCountError = 0;
        long otherDocCount = 0;
        for (InternalAggregation aggregation : aggregations) {
            InternalTerms terms = (InternalTerms) aggregation;
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
                buckets.put(bucket.getKey(), bucket);
            }
        }

        final int size = Math.min(requiredSize, buckets.size());
        BucketPriorityQueue ordered = new BucketPriorityQueue(size, order.comparator(null));
        for (Collection<Bucket> l : buckets.asMap().values()) {
            List<Bucket> sameTermBuckets = (List<Bucket>) l; // cast is ok according to javadocs
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
        return newAggregation(name, Arrays.asList(list), showTermDocCountError, docCountError, otherDocCount, getMetaData());
    }

    protected abstract InternalTerms newAggregation(String name, List<Bucket> buckets, boolean showTermDocCountError, long docCountError, long otherDocCount, Map<String, Object> metaData);

}
