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

import java.util.function.Supplier;

/**
 * Processes measured requests counts and inference times and decides whether
 * the number of allocations should be scaled up or down.
 */
public class AdaptiveAllocationsScaler {

    // visible for testing
    static final double SCALE_UP_THRESHOLD = 0.9;
    private static final double SCALE_DOWN_THRESHOLD = 0.85;

    /**
     * If the max_number_of_allocations is not set, use this value for now to prevent scaling up
     * to high numbers due to possible bugs or unexpected behaviour in the scaler.
     * TODO(jan): remove this safeguard when the scaler behaves as expected in production.
     */
    private static final int MAX_NUMBER_OF_ALLOCATIONS_SAFEGUARD = 32;

    private static final Logger logger = LogManager.getLogger(AdaptiveAllocationsScaler.class);

    private final String deploymentId;
    private final KalmanFilter1d requestRateEstimator;
    private final KalmanFilter1d inferenceTimeEstimator;
    private final Supplier<Long> scaleToZeroAfterNoRequestsSeconds;
    private double timeWithoutRequestsSeconds;

    private int numberOfAllocations;
    private int neededNumberOfAllocations;
    private Integer minNumberOfAllocations;
    private Integer maxNumberOfAllocations;
    private boolean dynamicsChanged;

    private Double lastMeasuredRequestRate;
    private Double lastMeasuredInferenceTime;
    private Long lastMeasuredQueueSize;

    AdaptiveAllocationsScaler(String deploymentId, int numberOfAllocations, Supplier<Long> scaleToZeroAfterNoRequestsSeconds) {
        this.deploymentId = deploymentId;
        this.scaleToZeroAfterNoRequestsSeconds = scaleToZeroAfterNoRequestsSeconds;

        // A smoothing factor of 100 roughly means the last 100 measurements have an effect
        // on the estimated values. The sampling time is 10 seconds, so approximately the
        // last 15 minutes are taken into account.
        // For the request rate, use auto-detection for dynamics changes, because the request
        // rate maybe change due to changed user behaviour.
        // For the inference time, don't use this auto-detection. The dynamics may change when
        // the number of allocations changes, which is passed explicitly to the estimator.
        requestRateEstimator = new KalmanFilter1d(deploymentId + ":rate", 100, true);
        inferenceTimeEstimator = new KalmanFilter1d(deploymentId + ":time", 100, false);
        timeWithoutRequestsSeconds = 0.0;
        this.numberOfAllocations = numberOfAllocations;
        neededNumberOfAllocations = numberOfAllocations;
        minNumberOfAllocations = null;
        maxNumberOfAllocations = null;
        dynamicsChanged = false;

        lastMeasuredRequestRate = null;
        lastMeasuredInferenceTime = null;
        lastMeasuredQueueSize = null;
    }

    void setMinMaxNumberOfAllocations(Integer minNumberOfAllocations, Integer maxNumberOfAllocations) {
        this.minNumberOfAllocations = minNumberOfAllocations;
        this.maxNumberOfAllocations = maxNumberOfAllocations;
    }

    void process(AdaptiveAllocationsScalerService.Stats stats, double timeIntervalSeconds, int numberOfAllocations) {
        lastMeasuredQueueSize = stats.pendingCount();
        if (stats.requestCount() > 0) {
            timeWithoutRequestsSeconds = 0.0;
        } else {
            timeWithoutRequestsSeconds += timeIntervalSeconds;
        }

        // The request rate (per second) is the request count divided by the time.
        // Assuming a Poisson process for the requests, the variance in the request
        // count equals the mean request count, and the variance in the request rate
        // equals that variance divided by the time interval squared.
        // The minimum request count is set to 1, because lower request counts can't
        // be reliably measured.
        // The estimated request rate should be used for the variance calculations,
        // because the measured request rate gives biased estimates.
        double requestRate = (double) stats.requestCount() / timeIntervalSeconds;
        double requestRateEstimate = requestRateEstimator.hasValue() ? requestRateEstimator.estimate() : requestRate;
        double requestRateVariance = Math.max(1.0, requestRateEstimate * timeIntervalSeconds) / Math.pow(timeIntervalSeconds, 2);
        requestRateEstimator.add(requestRate, requestRateVariance, false);
        lastMeasuredRequestRate = requestRate;

        if (stats.requestCount() > 0 && Double.isNaN(stats.inferenceTime()) == false) {
            // The inference time distribution is unknown. For simplicity, we assume
            // a std.error equal to the mean, so that the variance equals the mean
            // value squared. The variance of the mean is inversely proportional to
            // the number of inference measurements it contains.
            // Again, the estimated inference time should be used for the variance
            // calculations to prevent biased estimates.
            double inferenceTime = stats.inferenceTime();
            double inferenceTimeEstimate = inferenceTimeEstimator.hasValue() ? inferenceTimeEstimator.estimate() : inferenceTime;
            double inferenceTimeVariance = Math.pow(inferenceTimeEstimate, 2) / stats.requestCount();
            inferenceTimeEstimator.add(inferenceTime, inferenceTimeVariance, dynamicsChanged);
            lastMeasuredInferenceTime = inferenceTime;
        } else {
            lastMeasuredInferenceTime = null;
        }

        this.numberOfAllocations = numberOfAllocations;
        dynamicsChanged = false;
    }

    void resetTimeWithoutRequests() {
        timeWithoutRequestsSeconds = 0;
    }

    double getLoadLower() {
        double requestRateLower = Math.max(0.0, requestRateEstimator.lower());
        double inferenceTimeLower = Math.max(0.0, inferenceTimeEstimator.hasValue() ? inferenceTimeEstimator.lower() : 1.0);
        return requestRateLower * inferenceTimeLower;
    }

    double getLoadUpper() {
        double requestRateUpper = requestRateEstimator.upper();
        double inferenceTimeUpper = inferenceTimeEstimator.hasValue() ? inferenceTimeEstimator.upper() : 1.0;
        return requestRateUpper * inferenceTimeUpper;
    }

    Double getRequestRateEstimate() {
        return requestRateEstimator.hasValue() ? requestRateEstimator.estimate() : null;
    }

    Double getInferenceTimeEstimate() {
        return inferenceTimeEstimator.hasValue() ? inferenceTimeEstimator.estimate() : null;
    }

    Integer scale() {

        if (requestRateEstimator.hasValue() == false) {
            return null;
        }

        int oldNumberOfAllocations = numberOfAllocations;

        double loadLower = getLoadLower();
        while (loadLower / numberOfAllocations > SCALE_UP_THRESHOLD) {
            numberOfAllocations++;
        }

        double loadUpper = getLoadUpper();
        while (numberOfAllocations > 1 && loadUpper / (numberOfAllocations - 1) < SCALE_DOWN_THRESHOLD) {
            numberOfAllocations--;
        }

        neededNumberOfAllocations = numberOfAllocations;

        if (maxNumberOfAllocations == null) {
            numberOfAllocations = Math.min(numberOfAllocations, MAX_NUMBER_OF_ALLOCATIONS_SAFEGUARD);
        }
        if (minNumberOfAllocations != null) {
            numberOfAllocations = Math.max(numberOfAllocations, minNumberOfAllocations);
        }
        if (maxNumberOfAllocations != null) {
            numberOfAllocations = Math.min(numberOfAllocations, maxNumberOfAllocations);
        }

        if ((minNumberOfAllocations == null || minNumberOfAllocations == 0)
            && timeWithoutRequestsSeconds > scaleToZeroAfterNoRequestsSeconds.get()) {

            if (oldNumberOfAllocations != 0) {
                // avoid logging this message if there is no change
                logger.debug("[{}] adaptive allocations scaler: scaling down to zero, because of no requests.", deploymentId);
            }
            numberOfAllocations = 0;
            neededNumberOfAllocations = 0;
        }

        if (numberOfAllocations != oldNumberOfAllocations) {
            logger.debug(
                () -> Strings.format(
                    "[%s] adaptive allocations scaler: load in [%.3f, %.3f], scaling from %d to %d allocations.",
                    deploymentId,
                    loadLower,
                    loadUpper,
                    oldNumberOfAllocations,
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

    public String getDeploymentId() {
        return deploymentId;
    }

    public long getNumberOfAllocations() {
        return numberOfAllocations;
    }

    public long getNeededNumberOfAllocations() {
        return neededNumberOfAllocations;
    }

    public Double getLastMeasuredRequestRate() {
        return lastMeasuredRequestRate;
    }

    public Double getLastMeasuredInferenceTime() {
        return lastMeasuredInferenceTime;
    }

    public Long getLastMeasuredQueueSize() {
        return lastMeasuredQueueSize;
    }

    public Integer getMinNumberOfAllocations() {
        return minNumberOfAllocations;
    }

    public Integer getMaxNumberOfAllocations() {
        return maxNumberOfAllocations;
    }
}
