/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.adaptiveallocations;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;

/**
 * Estimator for the mean value and stderr of a series of measurements.
 * <br>
 * This implements a 1d Kalman filter with manoeuvre detection. Rather than a derived
 * dynamics model we simply fix how much we want to smooth in the steady state.
 * See also: <a href="https://en.wikipedia.org/wiki/Kalman_filter">Wikipedia</a>.
 */
class KalmanFilter1d {

    private static final Logger logger = LogManager.getLogger(KalmanFilter1d.class);

    private final String name;
    private final double smoothingFactor;
    private final boolean autodetectDynamicsChange;

    private double value;
    private double variance;
    private boolean dynamicsChangedLastTime;

    KalmanFilter1d(String name, double smoothingFactor, boolean autodetectDynamicsChange) {
        this.name = name;
        this.smoothingFactor = smoothingFactor;
        this.autodetectDynamicsChange = autodetectDynamicsChange;
        this.value = Double.MAX_VALUE;
        this.variance = Double.MAX_VALUE;
        this.dynamicsChangedLastTime = false;
    }

    /**
     * Adds a measurement (value, variance) to the estimator.
     * dynamicChangedExternal indicates whether the underlying possibly changed before this measurement.
     */
    void add(double value, double variance, boolean dynamicChangedExternal) {
        boolean dynamicChanged;
        if (hasValue() == false) {
            dynamicChanged = true;
            this.value = value;
            this.variance = variance;
        } else {
            double processVariance = variance / smoothingFactor;
            dynamicChanged = dynamicChangedExternal || detectDynamicsChange(value, variance);
            if (dynamicChanged || dynamicsChangedLastTime) {
                // If we know we likely had a change in the quantity we're estimating or the prediction
                // is 10 stddev off, we inject extra noise in the dynamics for this step.
                processVariance = Math.pow(value, 2);
            }

            double gain = (this.variance + processVariance) / (this.variance + processVariance + variance);
            this.value += gain * (value - this.value);
            this.variance = (1 - gain) * (this.variance + processVariance);
        }
        dynamicsChangedLastTime = dynamicChanged;
        logger.debug(
            () -> Strings.format(
                "[%s] measurement %.3f ± %.3f: estimate %.3f ± %.3f (dynamic changed: %s).",
                name,
                value,
                Math.sqrt(variance),
                this.value,
                Math.sqrt(this.variance),
                dynamicChanged
            )
        );
    }

    /**
     * Returns whether the estimator has received data and contains a value.
     */
    boolean hasValue() {
        return this.value < Double.MAX_VALUE && this.variance < Double.MAX_VALUE;
    }

    /**
     * Returns the estimate of the mean value.
     */
    double estimate() {
        return value;
    }

    /**
     * Returns the stderr of the estimate.
     */
    double error() {
        return Math.sqrt(this.variance);
    }

    /**
     * Returns the lowerbound of the 1 stddev confidence interval of the estimate.
     */
    double lower() {
        return value - error();
    }

    /**
     * Returns the upperbound of the 1 stddev confidence interval of the estimate.
     */
    double upper() {
        return value + error();
    }

    /**
     * Returns whether (value, variance) is very unlikely, indicating that
     * the underlying dynamics have changed.
     */
    private boolean detectDynamicsChange(double value, double variance) {
        return hasValue() && autodetectDynamicsChange && Math.pow(Math.abs(value - this.value), 2) / (variance + this.variance) > 100.0;
    }
}
