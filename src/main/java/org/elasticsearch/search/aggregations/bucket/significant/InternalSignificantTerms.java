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
package org.elasticsearch.search.aggregations.bucket.significant;

import com.google.common.collect.Maps;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;

import java.util.*;

/**
 *
 */
public abstract class InternalSignificantTerms extends InternalAggregation implements SignificantTerms, ToXContent, Streamable {

    protected SignificanceHeuristic significanceHeuristic;
    protected int requiredSize;
    protected long minDocCount;
    protected Collection<Bucket> buckets;
    protected Map<String, Bucket> bucketMap;
    protected long subsetSize;
    protected long supersetSize;

    protected InternalSignificantTerms() {} // for serialization

    @SuppressWarnings("PMD.ConstructorCallsOverridableMethod")
    public static abstract class Bucket extends SignificantTerms.Bucket {

        long bucketOrd;
        protected InternalAggregations aggregations;
        double score;

        protected Bucket(long subsetDf, long subsetSize, long supersetDf, long supersetSize, InternalAggregations aggregations) {
            super(subsetDf, subsetSize, supersetDf, supersetSize);
            this.aggregations = aggregations;
        }

        @Override
        public long getSubsetDf() {
            return subsetDf;
        }

        @Override
        public long getSupersetDf() {
            return supersetDf;
        }

        @Override
        public long getSupersetSize() {
            return supersetSize;
        }

        @Override
        public long getSubsetSize() {
            return subsetSize;
        }

        public void updateScore(SignificanceHeuristic significanceHeuristic) {
            score = significanceHeuristic.getScore(subsetDf, subsetSize, supersetDf, supersetSize);
        }

        @Override
        public long getDocCount() {
            return subsetDf;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        public Bucket reduce(List<? extends Bucket> buckets, ReduceContext context) {
            long subsetDf = 0;
            long supersetDf = 0;
            List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
            for (Bucket bucket : buckets) {
                subsetDf += bucket.subsetDf;
                supersetDf += bucket.supersetDf;
                aggregationsList.add(bucket.aggregations);
            }
            InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
            return newBucket(subsetDf, subsetSize, supersetDf, supersetSize, aggs);
        }

        abstract Bucket newBucket(long subsetDf, long subsetSize, long supersetDf, long supersetSize, InternalAggregations aggregations);

        @Override
        public double getSignificanceScore() {
            return score;
        }
    }

    protected InternalSignificantTerms(long subsetSize, long supersetSize, String name, int requiredSize, long minDocCount, SignificanceHeuristic significanceHeuristic, Collection<Bucket> buckets) {
        super(name);
        this.requiredSize = requiredSize;
        this.minDocCount = minDocCount;
        this.buckets = buckets;
        this.subsetSize = subsetSize;
        this.supersetSize = supersetSize;
        this.significanceHeuristic = significanceHeuristic;
    }

    @Override
    public Iterator<SignificantTerms.Bucket> iterator() {
        Object o = buckets.iterator();
        return (Iterator<SignificantTerms.Bucket>) o;
    }

    @Override
    public Collection<SignificantTerms.Bucket> getBuckets() {
        Object o = buckets;
        return (Collection<SignificantTerms.Bucket>) o;
    }

    @Override
    public SignificantTerms.Bucket getBucketByKey(String term) {
        if (bucketMap == null) {
            bucketMap = Maps.newHashMapWithExpectedSize(buckets.size());
            for (Bucket bucket : buckets) {
                bucketMap.put(bucket.getKey(), bucket);
            }
        }
        return bucketMap.get(term);
    }

    @Override
    public InternalAggregation reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();

        long globalSubsetSize = 0;
        long globalSupersetSize = 0;
        // Compute the overall result set size and the corpus size using the
        // top-level Aggregations from each shard
        for (InternalAggregation aggregation : aggregations) {
            InternalSignificantTerms terms = (InternalSignificantTerms) aggregation;
            globalSubsetSize += terms.subsetSize;
            globalSupersetSize += terms.supersetSize;
        }
        Map<String, List<InternalSignificantTerms.Bucket>> buckets = new HashMap<>();
        for (InternalAggregation aggregation : aggregations) {
            InternalSignificantTerms terms = (InternalSignificantTerms) aggregation;
            for (Bucket bucket : terms.buckets) {
                List<Bucket> existingBuckets = buckets.get(bucket.getKey());
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<>(aggregations.size());
                    buckets.put(bucket.getKey(), existingBuckets);
                }
                // Adjust the buckets with the global stats representing the
                // total size of the pots from which the stats are drawn
                existingBuckets.add(bucket.newBucket(bucket.getSubsetDf(), globalSubsetSize, bucket.getSupersetDf(), globalSupersetSize, bucket.aggregations));
            }
        }

        final int size = Math.min(requiredSize, buckets.size());
        BucketSignificancePriorityQueue ordered = new BucketSignificancePriorityQueue(size);
        for (Map.Entry<String, List<Bucket>> entry : buckets.entrySet()) {
            List<Bucket> sameTermBuckets = entry.getValue();
            final Bucket b = sameTermBuckets.get(0).reduce(sameTermBuckets, reduceContext);
            b.updateScore(significanceHeuristic);
            if ((b.score > 0) && (b.subsetDf >= minDocCount)) {
                ordered.insertWithOverflow(b);
            }
        }
        Bucket[] list = new Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            list[i] = (Bucket) ordered.pop();
        }
        return newAggregation(globalSubsetSize, globalSupersetSize, Arrays.asList(list));
    }

    abstract InternalSignificantTerms newAggregation(long subsetSize, long supersetSize, List<Bucket> buckets);

}
