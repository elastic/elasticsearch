/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.xpack.ml.aggs.MlAggsHelper.extractBucket;
import static org.elasticsearch.xpack.ml.aggs.MlAggsHelper.extractDoubleBucketedValues;

public class ChangePointAggregator extends SiblingPipelineAggregator {

    public ChangePointAggregator(String name, String bucketsPath, Map<String, Object> metadata) {
        super(name, new String[] { bucketsPath }, metadata);
    }

    @Override
    public InternalAggregation doReduce(InternalAggregations aggregations, AggregationReduceContext context) {
        Optional<MlAggsHelper.DoubleBucketValues> maybeBucketValues = extractDoubleBucketedValues(
            bucketsPaths()[0],
            aggregations,
            BucketHelpers.GapPolicy.SKIP,
            true
        );
        if (maybeBucketValues.isEmpty()) {
            return new InternalChangePointAggregation(
                name(),
                metadata(),
                List.of(),
                List.of(new ChangeType.Indeterminable("unable to find valid bucket values in bucket path [" + bucketsPaths()[0] + "]"))
            );
        }
        MlAggsHelper.DoubleBucketValues bucketValues = maybeBucketValues.get();

        EventDetector eventDetector = new EventDetector();
        List<ChangeType> events = eventDetector.detect(bucketValues);
        List<ChangePointBucket> changePointBuckets = new ArrayList<>();
        for (ChangeType c : events) {
            if (c.isChange()) {
                changePointBuckets.add(
                    extractBucket(bucketsPaths()[0], aggregations, c.changePoint()).map(
                        b -> new ChangePointBucket(b.getKey(), b.getDocCount(), b.getAggregations())
                    ).orElse(null)
                );
            }
        }

        return new InternalChangePointAggregation(name(), metadata(), changePointBuckets, events);
    }
}
