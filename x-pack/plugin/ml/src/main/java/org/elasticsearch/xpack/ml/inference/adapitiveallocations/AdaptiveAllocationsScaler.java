/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.adapitiveallocations;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;

public class AdaptiveAllocationsScaler {

    private static final double SCALE_UP_THRESHOLD = 0.9;
    private static final double SCALE_DOWN_THRESHOLD = 0.85;

    private static final Logger logger = LogManager.getLogger(AdaptiveAllocationsScaler.class);

    private final String deploymentId;
    private final KalmanFilter requestRateEstimator;
    private final KalmanFilter inferenceTimeEstimator;

    private int numberOfAllocations;
    private Integer minNumberOfAllocations;
    private Integer maxNumberOfAllocations;
    private boolean dynamicsChanged;

    AdaptiveAllocationsScaler(String deploymentId, int numberOfAllocations) {
        this.deploymentId = deploymentId;
        requestRateEstimator = new KalmanFilter(deploymentId + ":rate", 100, true);
        inferenceTimeEstimator = new KalmanFilter(deploymentId + ":time", 100, false);
        this.numberOfAllocations = numberOfAllocations;
        this.minNumberOfAllocations = null;
        this.maxNumberOfAllocations = null;
        this.dynamicsChanged = false;
    }

    void setMinMaxNumberOfAllocations(Integer minNumberOfAllocations, Integer maxNumberOfAllocations) {
        this.minNumberOfAllocations = minNumberOfAllocations;
        this.maxNumberOfAllocations = maxNumberOfAllocations;
    }

    void process(AdaptiveAllocationsScalerService.Stats stats, double timeIntervalSeconds, int numberOfAllocations) {
        double requestRate = (double) stats.requestCount() / timeIntervalSeconds;
        double requestRateEstimate = requestRateEstimator.hasValue() ? requestRateEstimator.estimate() : requestRate;
        double requestRateVariance = Math.max(1.0, requestRateEstimate * timeIntervalSeconds) / Math.pow(timeIntervalSeconds, 2);
        requestRateEstimator.add(requestRate, requestRateVariance, false);

        if (stats.requestCount() > 0) {
            double inferenceTime = stats.inferenceTime();
            double inferenceTimeEstimate = inferenceTimeEstimator.hasValue() ? inferenceTimeEstimator.estimate() : inferenceTime;
            double inferenceTimeVariance = Math.pow(inferenceTimeEstimate, 2) / stats.requestCount();
            inferenceTimeEstimator.add(inferenceTime, inferenceTimeVariance, dynamicsChanged);
        }

        this.numberOfAllocations = numberOfAllocations;
        dynamicsChanged = false;
    }

    Integer scale() {
        if (requestRateEstimator.hasValue() == false) {
            return null;
        }

        int oldNumberOfAllocations = numberOfAllocations;

        double requestRateLower = Math.max(0.0, requestRateEstimator.lower());
        double inferenceTimeLower = Math.max(0.0, inferenceTimeEstimator.hasValue() ? inferenceTimeEstimator.lower() : 1.0);
        double loadLower = requestRateLower * inferenceTimeLower;
        while (loadLower / numberOfAllocations > SCALE_UP_THRESHOLD) {
            numberOfAllocations++;
        }

        double requestRateUpper = requestRateEstimator.upper();
        double inferenceTimeUpper = inferenceTimeEstimator.hasValue() ? inferenceTimeEstimator.upper() : 1.0;
        double loadUpper = requestRateUpper * inferenceTimeUpper;
        while (numberOfAllocations > 1 && loadUpper / (numberOfAllocations - 1) < SCALE_DOWN_THRESHOLD) {
            numberOfAllocations--;
        }

        if (minNumberOfAllocations != null) {
            numberOfAllocations = Math.max(numberOfAllocations, minNumberOfAllocations);
        }
        if (maxNumberOfAllocations != null) {
            numberOfAllocations = Math.min(numberOfAllocations, maxNumberOfAllocations);
        }

        if (numberOfAllocations != oldNumberOfAllocations) {
            logger.debug(
                () -> Strings.format(
                    "[%s] adaptive allocations scaler: load in [%.3f, %.3f], scaling to %d allocations.",
                    deploymentId,
                    loadLower,
                    loadUpper,
                    numberOfAllocations
                )
            );
        } else {
            logger.debug(
                () -> Strings.format(
                    "[%s] adaptive allocations scaler: load in [%.3f, %.3f], keeping %d allocations.",
                    deploymentId,
                    loadLower,
                    loadUpper,
                    numberOfAllocations
                )
            );
        }

        if (numberOfAllocations != oldNumberOfAllocations) {
            this.dynamicsChanged = true;
            return numberOfAllocations;
        } else {
            return null;
        }
    }
}
