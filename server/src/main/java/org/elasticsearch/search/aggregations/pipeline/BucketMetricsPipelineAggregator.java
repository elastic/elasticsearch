/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.util.List;
import java.util.Map;

/**
 * A class of sibling pipeline aggregations which calculate metrics across the
 * buckets of a sibling aggregation
 */
public abstract class BucketMetricsPipelineAggregator extends SiblingPipelineAggregator {

    protected final DocValueFormat format;
    protected final GapPolicy gapPolicy;

    BucketMetricsPipelineAggregator(String name, String[] bucketsPaths, GapPolicy gapPolicy, DocValueFormat format,
            Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.gapPolicy = gapPolicy;
        this.format = format;
    }

    @Override
    public final InternalAggregation doReduce(Aggregations aggregations, ReduceContext context) {
        preCollection();
        List<String> bucketsPath = AggregationPath.parse(bucketsPaths()[0]).getPathElementsAsStringList();
        for (Aggregation aggregation : aggregations) {
            if (aggregation.getName().equals(bucketsPath.get(0))) {
                List<String> sublistedPath = bucketsPath.subList(1, bucketsPath.size());
                InternalMultiBucketAggregation<?, ?> multiBucketsAgg = (InternalMultiBucketAggregation<?, ?>) aggregation;
                List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = multiBucketsAgg.getBuckets();
                for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
                    Double bucketValue = BucketHelpers.resolveBucketValue(multiBucketsAgg, bucket, sublistedPath, gapPolicy);
                    if (bucketValue != null && Double.isNaN(bucketValue) == false) {
                        collectBucketValue(bucket.getKeyAsString(), bucketValue);
                    }
                }
            }
        }
        return buildAggregation(metadata());
    }

    /**
     * Called before initial collection and between successive collection runs.
     * A chance to initialize or re-initialize state
     */
    protected void preCollection() {
    }

    /**
     * Called after a collection run is finished to build the aggregation for
     * the collected state.
     *
     * @param metadata
     *            the metadata to add to the resulting aggregation
     */
    protected abstract InternalAggregation buildAggregation(Map<String, Object> metadata);

    /**
     * Called for each bucket with a value so the state can be modified based on
     * the key and metric value for this bucket
     *
     * @param bucketKey
     *            the key for this bucket as a String
     * @param bucketValue
     *            the value of the metric specified in <code>bucketsPath</code>
     *            for this bucket
     */
    protected abstract void collectBucketValue(String bucketKey, Double bucketValue);
}
