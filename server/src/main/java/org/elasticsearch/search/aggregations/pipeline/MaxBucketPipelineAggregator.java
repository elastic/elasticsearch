/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MaxBucketPipelineAggregator extends BucketMetricsPipelineAggregator {
    private List<String> maxBucketKeys;
    private double maxValue;

    MaxBucketPipelineAggregator(
        String name,
        String[] bucketsPaths,
        GapPolicy gapPolicy,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        super(name, bucketsPaths, gapPolicy, formatter, metadata);
    }

    /**
     * Read from a stream.
     */
    public MaxBucketPipelineAggregator(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return MaxBucketPipelineAggregationBuilder.NAME;
    }

    @Override
    protected void preCollection() {
        maxBucketKeys = new ArrayList<>();
        maxValue = Double.NEGATIVE_INFINITY;
    }

    @Override
    protected void collectBucketValue(String bucketKey, Double bucketValue) {
        if (bucketValue > maxValue) {
            maxBucketKeys.clear();
            maxBucketKeys.add(bucketKey);
            maxValue = bucketValue;
        } else if (bucketValue.equals(maxValue)) {
            maxBucketKeys.add(bucketKey);
        }
    }

    @Override
    protected InternalAggregation buildAggregation(Map<String, Object> metadata) {
        String[] keys = maxBucketKeys.toArray(new String[maxBucketKeys.size()]);
        return new InternalBucketMetricValue(name(), keys, maxValue, format, metadata());
    }

}
