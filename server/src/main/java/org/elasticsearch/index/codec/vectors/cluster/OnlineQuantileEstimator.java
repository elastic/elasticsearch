/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import java.util.Random;

/**
 * Computes an approximation of the q-quantile of a data stream using constant memory.
 * It uses <a href="https://arxiv.org/abs/1407.1121">this approach</a> that was originally designed for integers.
 * This implementation works with floating-point numbers by quantizing the stream values. This requires knowing the minimum and maximum
 * values in the stream and choosing a desired precision.
 * Note that the precision only pertains to the quantization and the quantile estimator carries additional error.
 */
public class OnlineQuantileEstimator {
    private final float min;
    private final float max;
    private final float q;
    private final float precision;
    private final Random random;
    private int estimate;
    private int step;
    private int sign;

    /**
     * Constructor
     * @param q the quantile to compute must be in (0, 1)
     * @param min the minimum value in the stream (the 0-quantile)
     * @param max the maximum value in the stream (the 1-quantile)
     * @param precision the desired numerical precision in (0, 1)
     */
    OnlineQuantileEstimator(float q, float min, float max, float precision, long seed) {
        if (q <= 0.0f || q > 1.0f) {
            throw new IllegalArgumentException("q must be between 0.0f and 1.0f");
        }
        if (precision <= 0.0f || precision > 1.0f) {
            throw new IllegalArgumentException("precision must be between 0.0f and 1.0f");
        }
        this.q = q;
        this.min = min;
        this.max = max;
        this.precision = precision;
        this.random = new Random(seed);

        reset();
    }

    /**
     * Update the estimate with a new value from the stream.
     * @param value the new value from the stream
     */
    public void updateEstimate(float value) {
        // If value is not in the interval, snap it.
        if (value < min) {
            value = min;
        }
        if (value > max) {
            value = max;
        }
        int intValue = Math.round((value - min) / ((max - min) * precision));
        // After quantizing the input, the remaining code follows Algorithm 3 of https://arxiv.org/abs/1407.1121.

        if (estimate == -1) {
            estimate = intValue;
            return;
        }

        float rand = this.random.nextFloat();

        if (intValue > estimate && rand > 1 - this.q) {
            step += (sign > 0) ? 1 : -1;
            estimate += (step > 0) ? step : 1;
            if (estimate > intValue) {
                step += intValue - estimate;
                estimate = intValue;
            }
            if (sign < 0 && step > 1) {
                step = 1;
            }
            sign = 1;
        } else if (intValue < estimate && rand > this.q) {
            step += (sign < 0) ? 1 : -1;
            estimate -= (step > 0) ? step : 1;
            if (estimate < intValue) {
                step += estimate - intValue;
                estimate = intValue;
            }
            if (sign > 0 && step > 1) {
                step = 1;
            }
            sign = -1;
        }
    }

    /**
     * Update the estimate with a batch of values from the stream.
     * @param values the new values from the stream
     */
    public void updateEstimate(float[] values) {
        for (float value : values) {
            updateEstimate(value);
        }
    }


        /** Return the q-quantile estimate. */
    public float getEstimate() {
        return estimate * ((max - min) * precision) + min;
    }

    /** Reset the estimator to start a new stream. */
    void reset() {
        this.estimate = -1;
        this.step = 0;
        this.sign = 1;
    }
}
