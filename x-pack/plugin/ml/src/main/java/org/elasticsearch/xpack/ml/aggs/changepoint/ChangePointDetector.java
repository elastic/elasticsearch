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
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;

/**
 * Detects whether a series of values has a change point, by running both
 * ChangeDetector and SpikeAndDipDetector on it. This is the main entrypoint
 * of change point detection.
 */
public class ChangePointDetector {

    private static final Logger logger = LogManager.getLogger(ChangePointDetector.class);

    static final double P_VALUE_THRESHOLD = 0.01;
    static final int MINIMUM_BUCKETS = 10;

    /**
     * Returns the ChangeType of a series of values.
     */
    public static ChangeType getChangeType(MlAggsHelper.DoubleBucketValues bucketValues) {
        if (bucketValues.getValues().length < (2 * MINIMUM_BUCKETS) + 2) {
            return new ChangeType.Indeterminable(
                "not enough buckets to calculate change_point. Requires at least ["
                    + ((2 * MINIMUM_BUCKETS) + 2)
                    + "]; found ["
                    + bucketValues.getValues().length
                    + "]"
            );
        }

        ChangeType spikeOrDip;
        try {
            SpikeAndDipDetector detect = new SpikeAndDipDetector(bucketValues);
            spikeOrDip = detect.detect(P_VALUE_THRESHOLD);
            logger.trace("spike or dip p-value: [{}]", spikeOrDip.pValue());
        } catch (NotStrictlyPositiveException nspe) {
            logger.debug("failure testing for dips and spikes", nspe);
            spikeOrDip = new ChangeType.Indeterminable("failure testing for dips and spikes");
        }

        ChangeType change = new ChangeDetector(bucketValues).detect(P_VALUE_THRESHOLD);
        logger.trace("change p-value: [{}]", change.pValue());

        if (spikeOrDip.pValue() < change.pValue()) {
            change = spikeOrDip;
        }
        return change;
    }
}
