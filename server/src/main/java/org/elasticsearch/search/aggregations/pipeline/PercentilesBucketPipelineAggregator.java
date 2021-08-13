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
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PercentilesBucketPipelineAggregator extends BucketMetricsPipelineAggregator {

    private final double[] percents;
    private boolean keyed = true;
    private List<Double> data;

    PercentilesBucketPipelineAggregator(String name, double[] percents, boolean keyed, String[] bucketsPaths,
                                        GapPolicy gapPolicy, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, bucketsPaths, gapPolicy, formatter, metadata);
        this.percents = percents;
        this.keyed = keyed;
    }

    @Override
    protected void preCollection() {
       data = new ArrayList<>(1024);
    }

    @Override
    protected void collectBucketValue(String bucketKey, Double bucketValue) {
        data.add(bucketValue);
    }

    @Override
    protected InternalAggregation buildAggregation(Map<String, Object> metadata) {
        // Perform the sorting and percentile collection now that all the data
        // has been collected.
        Collections.sort(data);

        double[] percentiles = new double[percents.length];
        if (data.size() == 0) {
            for (int i = 0; i < percents.length; i++) {
                percentiles[i] = Double.NaN;
            }
        } else {
            for (int i = 0; i < percents.length; i++) {
                int index = (int) Math.round((percents[i] / 100.0) * (data.size() - 1));
                percentiles[i] = data.get(index);
            }
        }

        // todo need postCollection() to clean up temp sorted data?

        return new InternalPercentilesBucket(name(), percents, percentiles, keyed, format, metadata);
    }
}
