/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MinBucketPipelineAggregator extends BucketMetricsPipelineAggregator {
    private List<String> minBucketKeys;
    private double minValue;

    MinBucketPipelineAggregator(String name, String[] bucketsPaths, GapPolicy gapPolicy, DocValueFormat formatter,
            Map<String, Object> metadata) {
        super(name, bucketsPaths, gapPolicy, formatter, metadata);
    }

    @Override
    protected void preCollection() {
        minBucketKeys = new ArrayList<>();
        minValue = Double.POSITIVE_INFINITY;
    }

    @Override
    protected void collectBucketValue(String bucketKey, Double bucketValue) {
        if (bucketValue < minValue) {
            minBucketKeys.clear();
            minBucketKeys.add(bucketKey);
            minValue = bucketValue;
        } else if (bucketValue.equals(minValue)) {
            minBucketKeys.add(bucketKey);
        }
    }

    @Override
    protected InternalAggregation buildAggregation(Map<String, Object> metadata) {
        String[] keys = minBucketKeys.toArray(new String[0]);
        return new InternalBucketMetricValue(name(), keys, minValue, format, metadata());
    }

}
