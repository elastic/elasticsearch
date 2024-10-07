/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.apache.commons.math3.exception.NotStrictlyPositiveException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;
import org.elasticsearch.xpack.ml.aggs.changepoint.ChangeType.Indeterminable;

import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.xpack.ml.aggs.MlAggsHelper.extractBucket;
import static org.elasticsearch.xpack.ml.aggs.MlAggsHelper.extractDoubleBucketedValues;

public class ChangePointAggregator extends SiblingPipelineAggregator {

    private static final Logger logger = LogManager.getLogger(ChangePointAggregator.class);

    static final double P_VALUE_THRESHOLD = 0.01;
    static final int MINIMUM_BUCKETS = 10;

    private static double changePValueThreshold(int nValues) {
        // This was obtained by simulating the test power for a fixed size effect as a
        // function of the bucket value count.
        return P_VALUE_THRESHOLD * Math.exp(-0.04 * (double) (nValues - 2 * (MINIMUM_BUCKETS + 1)));
    }

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
                null,
                new ChangeType.Indeterminable("unable to find valid bucket values in bucket path [" + bucketsPaths()[0] + "]")
            );
        }
        MlAggsHelper.DoubleBucketValues bucketValues = maybeBucketValues.get();
        if (bucketValues.getValues().length < (2 * MINIMUM_BUCKETS) + 2) {
            return new InternalChangePointAggregation(
                name(),
                metadata(),
                null,
                new ChangeType.Indeterminable(
                    "not enough buckets to calculate change_point. Requires at least ["
                        + ((2 * MINIMUM_BUCKETS) + 2)
                        + "]; found ["
                        + bucketValues.getValues().length
                        + "]"
                )
            );
        }

        ChangeType spikeOrDip = testForSpikeOrDip(bucketValues, P_VALUE_THRESHOLD);

        // Test for change step, trend and distribution changes.
        ChangeType change = testForChange(bucketValues, changePValueThreshold(bucketValues.getValues().length));
        logger.trace("change p-value: [{}]", change.pValue());

        if (spikeOrDip.pValue() < change.pValue()) {
            change = spikeOrDip;
        }

        ChangePointBucket changePointBucket = null;
        if (change.changePoint() >= 0) {
            changePointBucket = extractBucket(bucketsPaths()[0], aggregations, change.changePoint()).map(
                b -> new ChangePointBucket(b.getKey(), b.getDocCount(), b.getAggregations())
            ).orElse(null);
        }

        return new InternalChangePointAggregation(name(), metadata(), changePointBucket, change);
    }

    static ChangeType testForSpikeOrDip(MlAggsHelper.DoubleBucketValues bucketValues, double pValueThreshold) {
        try {
            SpikeAndDipDetector detect = new SpikeAndDipDetector(bucketValues.getValues());
            ChangeType result = detect.detect(pValueThreshold, bucketValues);
            logger.trace("spike or dip p-value: [{}]", result.pValue());
            return result;
        } catch (NotStrictlyPositiveException nspe) {
            logger.debug("failure testing for dips and spikes", nspe);
        }
        return new Indeterminable("failure testing for dips and spikes");
    }

    static ChangeType testForChange(MlAggsHelper.DoubleBucketValues bucketValues, double pValueThreshold) {
        double[] timeWindow = bucketValues.getValues();
        return new ChangeDetector(timeWindow).detect(pValueThreshold, bucketValues);
    }
}
