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
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.InternalSingleBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.support.AggregationPath.pathElementContainsBucketKey;

/**
 * A class of sibling pipeline aggregations which calculate metrics across the
 * buckets of a sibling aggregation
 */
public abstract class BucketMetricsPipelineAggregator extends SiblingPipelineAggregator {

    protected final DocValueFormat format;
    protected final GapPolicy gapPolicy;

    BucketMetricsPipelineAggregator(
        String name,
        String[] bucketsPaths,
        GapPolicy gapPolicy,
        DocValueFormat format,
        Map<String, Object> metadata
    ) {
        super(name, bucketsPaths, metadata);
        this.gapPolicy = gapPolicy;
        this.format = format;
    }

    @Override
    public final InternalAggregation doReduce(Aggregations aggregations, AggregationReduceContext context) {
        preCollection();
        List<AggregationPath.PathElement> parsedPath = AggregationPath.parse(bucketsPaths()[0]).getPathElements();
        for (Aggregation aggregation : aggregations) {
            // Now that we have found the first agg in the path, resolve to the first non-qualified multi-bucket path
            if (aggregation.getName().equals(parsedPath.get(0).name())) {
                int currElement = 0;
                Aggregation currentAgg = aggregation;
                while (currElement < parsedPath.size() - 1) {
                    if (currentAgg == null) {
                        throw new IllegalArgumentException(
                            "bucket_path ["
                                + bucketsPaths()[0]
                                + "] expected aggregation with name ["
                                + parsedPath.get(currElement).name()
                                + "] but was missing in search response"
                        );
                    }
                    if (currentAgg instanceof InternalSingleBucketAggregation singleBucketAggregation) {
                        currentAgg = singleBucketAggregation.getAggregations().get(parsedPath.get(++currElement).name());
                    } else if (pathElementContainsBucketKey(parsedPath.get(currElement))) {
                        if (currentAgg instanceof InternalMultiBucketAggregation<?, ?> multiBucketAggregation) {
                            InternalMultiBucketAggregation.InternalBucket bucket =
                                (InternalMultiBucketAggregation.InternalBucket) multiBucketAggregation.getProperty(
                                    parsedPath.get(currElement).key()
                                );
                            if (bucket == null) {
                                throw new AggregationExecutionException(
                                    "missing bucket ["
                                        + parsedPath.get(currElement).key()
                                        + "] for agg ["
                                        + currentAgg.getName()
                                        + "] while extracting bucket path ["
                                        + bucketsPaths()[0]
                                        + "]"
                                );
                            }
                            if (currElement == parsedPath.size() - 1) {
                                throw new AggregationExecutionException(
                                    "invalid bucket path ends at [" + parsedPath.get(currElement).key() + "]"
                                );
                            }
                            currentAgg = bucket.getAggregations().get(parsedPath.get(++currElement).name());
                        } else {
                            throw new AggregationExecutionException(
                                "bucket_path ["
                                    + bucketsPaths()[0]
                                    + "] indicates bucket_key ["
                                    + parsedPath.get(currElement).key()
                                    + "] at position ["
                                    + currElement
                                    + "] but encountered on agg ["
                                    + currentAgg.getName()
                                    + "] which is not a multi_bucket aggregation"
                            );
                        }
                    } else {
                        break;
                    }
                }
                if (currentAgg instanceof InternalMultiBucketAggregation == false) {
                    String msg = currentAgg == null
                        ? "did not find multi-bucket aggregation for extraction."
                        : "did not find multi-bucket aggregation for extraction. Found [" + currentAgg.getName() + "]";
                    throw new AggregationExecutionException(msg);
                }
                List<String> sublistedPath = AggregationPath.pathElementsAsStringList(parsedPath.subList(currElement, parsedPath.size()));
                // First element is the current agg, so we want the rest of the path
                sublistedPath = sublistedPath.subList(1, sublistedPath.size());
                InternalMultiBucketAggregation<?, ?> multiBucketsAgg = (InternalMultiBucketAggregation<?, ?>) currentAgg;
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
    protected void preCollection() {}

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
