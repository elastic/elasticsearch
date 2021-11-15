/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.confidence;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xpack.ml.aggs.FastOnePoisson;
import org.elasticsearch.xpack.ml.aggs.PCG;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class ConfidenceAggregator extends DeferableBucketAggregator {

    private final int confidenceInterval;
    private final int bucketCount;
    private final double probability;
    private final boolean keyed;
    private final FastOnePoisson fastOnePoisson;
    private final Map<String, String> bucketPaths;

    protected ConfidenceAggregator(
        String name,
        AggregatorFactories factories,
        AggregationContext context,
        Aggregator parent,
        int confidenceInterval,
        double probability,
        boolean keyed,
        int seed,
        Map<String, String> bucketPaths,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, metadata);
        this.confidenceInterval = confidenceInterval;
        double calculatedConfidenceInterval = confidenceInterval / 100.0;
        this.bucketCount = (int) (Math.ceil(2.0 / (1.0 - calculatedConfidenceInterval))) + 1;
        this.probability = probability;
        this.fastOnePoisson = new FastOnePoisson(new PCG(seed));
        this.bucketPaths = bucketPaths;
        this.keyed = keyed;
    }

    @Override
    protected void doClose() {
        super.doClose();
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForFixedBucketCount(owningBucketOrds, bucketCount, InternalConfidenceAggregation.Bucket::new, buckets -> {
            CollectionUtil.introSort(buckets, Comparator.comparingLong(InternalConfidenceAggregation.Bucket::getRawKey));
            return new InternalConfidenceAggregation(
                name,
                confidenceInterval,
                probability,
                keyed,
                bucketPaths,
                metadata(),
                buckets,
                List.of()
            );
        });
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalConfidenceAggregation(name, confidenceInterval, probability, keyed, bucketPaths, metadata());
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                collectBucket(sub, doc, calculateBucket(0, owningBucketOrd));
                for (int i = 1; i < bucketCount; i++) {
                    int iter = fastOnePoisson.next();
                    for (int j = 0; j < iter; j++) {
                        collectBucket(sub, doc, calculateBucket(i, owningBucketOrd));
                    }
                }
            }
        };
    }

    private long calculateBucket(int bucketNum, long owningBucketOrd) {
        return owningBucketOrd * bucketCount + bucketNum;
    }

}
