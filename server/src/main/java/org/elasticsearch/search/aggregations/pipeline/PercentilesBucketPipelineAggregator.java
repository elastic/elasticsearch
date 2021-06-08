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
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.InterpolationType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PercentilesBucketPipelineAggregator extends BucketMetricsPipelineAggregator {

    private final double[] percents;
    private boolean keyed = true;
    private List<Double> data;
    private InterpolationType interpolation;

    PercentilesBucketPipelineAggregator(String name, double[] percents, boolean keyed, InterpolationType interpolation,
                                        String[] bucketsPaths, GapPolicy gapPolicy, DocValueFormat formatter,
                                        Map<String, Object> metadata) {
        super(name, bucketsPaths, gapPolicy, formatter, metadata);
        this.percents = percents;
        this.keyed = keyed;
        this.interpolation = interpolation;
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
            if (Objects.isNull(interpolation) || InterpolationType.NONE.equals(interpolation)) {
                calculatePercentiles(percentiles);
            }
            else if (InterpolationType.LINEAR.equals(interpolation)) {
                calculatePercentilesUsingLinearInterpolation(percentiles);
            }
        }

        // todo need postCollection() to clean up temp sorted data?

        return new InternalPercentilesBucket(name(), percents, percentiles, keyed, interpolation, format, metadata);
    }

    private void calculatePercentiles(double[] percentiles) {
        for (int i = 0; i < percents.length; i++) {
            int index = (int) Math.round((percents[i] / 100.0) * (data.size() - 1));
            percentiles[i] = data.get(index);
        }
    }

    private void calculatePercentilesUsingLinearInterpolation(double[] percentiles) {
        for (int i = 0; i < percents.length; i++) {
            final int dataSize = data.size();
            double index = (dataSize - 1) * (percents[i] / 100.0);
            int lower = (int) index;
            int upper = lower + 1;

            if (lower >= dataSize - 1) {
                percentiles[i] = data.get(dataSize - 1);
            } else {
                double fraction = index - lower;
                percentiles[i] = data.get(lower) + (data.get(upper) - data.get(lower)) * fraction;
            }
        }
    }
}
