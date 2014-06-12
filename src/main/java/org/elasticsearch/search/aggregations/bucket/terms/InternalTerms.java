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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.support.BucketPriorityQueue;

import java.util.*;

/**
 *
 */
public abstract class InternalTerms extends InternalAggregation implements Terms, ToXContent, Streamable {

    public static abstract class Bucket extends Terms.Bucket {

        long bucketOrd;

        protected long docCount;
        protected InternalAggregations aggregations;

        protected Bucket(long docCount, InternalAggregations aggregations) {
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        public Bucket reduce(List<? extends Bucket> buckets, BigArrays bigArrays) {
            if (buckets.size() == 1) {
                Bucket bucket = buckets.get(0);
                bucket.aggregations.reduce(bigArrays);
                return bucket;
            }
            Bucket reduced = null;
            List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
            for (Bucket bucket : buckets) {
                if (reduced == null) {
                    reduced = bucket;
                } else {
                    reduced.docCount += bucket.docCount;
                }
                aggregationsList.add(bucket.aggregations);
            }
            reduced.aggregations = InternalAggregations.reduce(aggregationsList, bigArrays);
            return reduced;
        }
    }

    protected InternalOrder order;
    protected int requiredSize;
    protected long minDocCount;
    protected Collection<Bucket> buckets;
    protected Map<String, Bucket> bucketMap;

    protected InternalTerms() {} // for serialization

    protected InternalTerms(String name, InternalOrder order, int requiredSize, long minDocCount, Collection<Bucket> buckets, byte[] metaData) {
        super(name, metaData);
        this.order = order;
        this.requiredSize = requiredSize;
        this.minDocCount = minDocCount;
        this.buckets = buckets;
    }

    @Override
    public Collection<Terms.Bucket> getBuckets() {
        Object o = buckets;
        return (Collection<Terms.Bucket>) o;
    }

    @Override
    public Terms.Bucket getBucketByKey(String term) {
        if (bucketMap == null) {
            bucketMap = Maps.newHashMapWithExpectedSize(buckets.size());
            for (Bucket bucket : buckets) {
                bucketMap.put(bucket.getKey(), bucket);
            }
        }
        return bucketMap.get(term);
    }

    @Override
    public InternalTerms reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        if (aggregations.size() == 1) {
            InternalTerms terms = (InternalTerms) aggregations.get(0);
            terms.trimExcessEntries(reduceContext.bigArrays());
            return terms;
        }

        InternalTerms reduced = null;

        Map<Text, List<InternalTerms.Bucket>> buckets = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalTerms terms = (InternalTerms) aggregation;
            if (terms instanceof UnmappedTerms) {
                continue;
            }
            if (reduced == null) {
                reduced = terms;
            }
            if (buckets == null) {
                buckets = new HashMap<>(terms.buckets.size());
            }
            for (Bucket bucket : terms.buckets) {
                List<Bucket> existingBuckets = buckets.get(bucket.getKeyAsText());
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<>(aggregations.size());
                    buckets.put(bucket.getKeyAsText(), existingBuckets);
                }
                existingBuckets.add(bucket);
            }
        }

        if (reduced == null) {
            // there are only unmapped terms, so we just return the first one (no need to reduce)
            return (UnmappedTerms) aggregations.get(0);
        }

        final int size = Math.min(requiredSize, buckets.size());
        BucketPriorityQueue ordered = new BucketPriorityQueue(size, order.comparator(null));
        for (Map.Entry<Text, List<Bucket>> entry : buckets.entrySet()) {
            List<Bucket> sameTermBuckets = entry.getValue();
            final Bucket b = sameTermBuckets.get(0).reduce(sameTermBuckets, reduceContext.bigArrays());
            if (b.docCount >= minDocCount) {
                ordered.insertWithOverflow(b);
            }
        }
        Bucket[] list = new Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            list[i] = (Bucket) ordered.pop();
        }
        reduced.buckets = Arrays.asList(list);
        return reduced;
    }

    final void trimExcessEntries(BigArrays bigArrays) {
        final List<Bucket> newBuckets = Lists.newArrayList();
        for (Bucket b : buckets) {
            if (newBuckets.size() >= requiredSize) {
                break;
            }
            if (b.docCount >= minDocCount) {
                newBuckets.add(b);
                b.aggregations.reduce(bigArrays);
            }
        }
        buckets = newBuckets;
    }

}
