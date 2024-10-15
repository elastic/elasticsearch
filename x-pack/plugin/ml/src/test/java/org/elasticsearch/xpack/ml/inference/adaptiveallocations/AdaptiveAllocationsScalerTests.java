/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.adaptiveallocations;

import org.elasticsearch.test.ESTestCase;

import java.util.Random;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;

public class AdaptiveAllocationsScalerTests extends ESTestCase {

    public void testAutoscaling_scaleUpAndDown() {
        AdaptiveAllocationsScaler adaptiveAllocationsScaler = new AdaptiveAllocationsScaler("test-deployment", 1);

        // With 1 allocation the system can handle 500 requests * 0.020 sec/request.
        // To handle remaining requests the system should scale to 2 allocations.
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(500, 100, 100, 0.020), 10, 1);
        assertThat(adaptiveAllocationsScaler.scale(), equalTo(2));

        // With 2 allocation the system can handle 800 requests * 0.025 sec/request.
        // To handle remaining requests the system should scale to 3 allocations.
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(800, 100, 50, 0.025), 10, 2);
        assertThat(adaptiveAllocationsScaler.scale(), equalTo(3));

        // With 3 allocations the system can handle the load.
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(1000, 0, 0, 0.025), 10, 3);
        assertThat(adaptiveAllocationsScaler.scale(), nullValue());

        // No load anymore, so the system should gradually scale down to 1 allocation.
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(0, 0, 0, Double.NaN), 10, 3);
        assertThat(adaptiveAllocationsScaler.scale(), nullValue());
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(0, 0, 0, Double.NaN), 10, 3);
        assertThat(adaptiveAllocationsScaler.scale(), equalTo(2));
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(0, 0, 0, Double.NaN), 10, 2);
        assertThat(adaptiveAllocationsScaler.scale(), nullValue());
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(0, 0, 0, Double.NaN), 10, 2);
        assertThat(adaptiveAllocationsScaler.scale(), equalTo(1));
    }

    public void testAutoscaling_noOscillating() {
        AdaptiveAllocationsScaler adaptiveAllocationsScaler = new AdaptiveAllocationsScaler("test-deployment", 1);

        // With 1 allocation the system can handle 880 requests * 0.010 sec/request.
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(880, 0, 0, 0.010), 10, 1);
        assertThat(adaptiveAllocationsScaler.scale(), nullValue());
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(880, 0, 0, 0.010), 10, 1);
        assertThat(adaptiveAllocationsScaler.scale(), nullValue());

        // Increase the load to 980 requests * 0.010 sec/request, and the system
        // should scale to 2 allocations to have some spare capacity.
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(920, 0, 0, 0.010), 10, 1);
        assertThat(adaptiveAllocationsScaler.scale(), nullValue());
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(950, 0, 0, 0.010), 10, 1);
        assertThat(adaptiveAllocationsScaler.scale(), nullValue());
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(980, 0, 0, 0.010), 10, 1);
        assertThat(adaptiveAllocationsScaler.scale(), equalTo(2));
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(980, 0, 0, 0.010), 10, 2);
        assertThat(adaptiveAllocationsScaler.scale(), nullValue());

        // Reducing the load to just 880 requests * 0.010 sec/request should not
        // trigger scaling down again, to prevent oscillating.
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(880, 0, 0, 0.010), 10, 2);
        assertThat(adaptiveAllocationsScaler.scale(), nullValue());
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(880, 0, 0, 0.010), 10, 2);
        assertThat(adaptiveAllocationsScaler.scale(), nullValue());
    }

    public void testAutoscaling_respectMinMaxAllocations() {
        AdaptiveAllocationsScaler adaptiveAllocationsScaler = new AdaptiveAllocationsScaler("test-deployment", 1);
        adaptiveAllocationsScaler.setMinMaxNumberOfAllocations(2, 5);

        // Even though there are no requests, scale to the minimum of 2 allocations.
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(0, 0, 0, 0.010), 10, 1);
        assertThat(adaptiveAllocationsScaler.scale(), equalTo(2));
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(0, 0, 0, 0.010), 10, 2);
        assertThat(adaptiveAllocationsScaler.scale(), nullValue());

        // Even though there are many requests, the scale to the maximum of 5 allocations.
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(100, 10000, 1000, 0.010), 10, 2);
        assertThat(adaptiveAllocationsScaler.scale(), equalTo(5));
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(500, 10000, 1000, 0.010), 10, 5);
        assertThat(adaptiveAllocationsScaler.scale(), nullValue());

        // After a while of no requests, scale to the minimum of 2 allocations.
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(0, 0, 0, 0.010), 10, 5);
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(0, 0, 0, 0.010), 10, 5);
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(0, 0, 0, 0.010), 10, 5);
        assertThat(adaptiveAllocationsScaler.scale(), equalTo(2));
    }

    public void testEstimation_highVariance() {
        AdaptiveAllocationsScaler adaptiveAllocationsScaler = new AdaptiveAllocationsScaler("test-deployment", 1);

        Random random = new Random(42);

        double averageLoadMean = 0.0;
        double averageLoadError = 0.0;

        double time = 0.0;
        for (int nextMeasurementTime = 1; nextMeasurementTime <= 100; nextMeasurementTime++) {
            // Sample one second of data (until the next measurement time).
            // This contains approximately 100 requests with high-variance inference times.
            AdaptiveAllocationsScalerService.Stats stats = new AdaptiveAllocationsScalerService.Stats(0, 0, 0, 0);
            while (time < nextMeasurementTime) {
                // Draw inference times from a log-normal distribution, which has high variance.
                // This distribution approximately has: mean=3.40, variance=98.4.
                double inferenceTime = Math.exp(random.nextGaussian(0.1, 1.5));
                stats = stats.add(new AdaptiveAllocationsScalerService.Stats(1, 0, 0, inferenceTime));

                // The requests are Poisson distributed, which means the time inbetween
                // requests follows an exponential distribution.
                // This distribution has on average 100 requests per second.
                double dt = 0.01 * random.nextExponential();
                time += dt;
            }

            adaptiveAllocationsScaler.process(stats, 1, 1);
            double lower = adaptiveAllocationsScaler.getLoadLower();
            double upper = adaptiveAllocationsScaler.getLoadUpper();
            averageLoadMean += (upper + lower) / 2.0;
            averageLoadError += (upper - lower) / 2.0;
        }

        averageLoadMean /= 100;
        averageLoadError /= 100;

        double expectedLoad = 100 * 3.40;
        assertThat(averageLoadMean - averageLoadError, lessThan(expectedLoad));
        assertThat(averageLoadMean + averageLoadError, greaterThan(expectedLoad));
        assertThat(averageLoadError / averageLoadMean, lessThan(1 - AdaptiveAllocationsScaler.SCALE_UP_THRESHOLD));
    }

    public void testAutoscaling_maxAllocationsSafeguard() {
        AdaptiveAllocationsScaler adaptiveAllocationsScaler = new AdaptiveAllocationsScaler("test-deployment", 1);
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(1_000_000, 10_000_000, 1, 0.05), 10, 1);
        assertThat(adaptiveAllocationsScaler.scale(), equalTo(32));
        adaptiveAllocationsScaler.setMinMaxNumberOfAllocations(2, 77);
        assertThat(adaptiveAllocationsScaler.scale(), equalTo(77));
    }

    public void testAutoscaling_scaleDownToZeroAllocations() {
        assumeTrue("Should only run if adaptive allocations feature flag is enabled", ScaleToZeroFeatureFlag.isEnabled());

        AdaptiveAllocationsScaler adaptiveAllocationsScaler = new AdaptiveAllocationsScaler("test-deployment", 1);
        // 1 hour with 1 request per 1 seconds, so don't scale.
        for (int i = 0; i < 3600; i++) {
            adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(1, 0, 0, 0.05), 1, 1);
            assertThat(adaptiveAllocationsScaler.scale(), nullValue());
        }
        // 15 minutes with no requests, so don't scale.
        for (int i = 0; i < 900; i++) {
            adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(0, 0, 0, 0.05), 1, 1);
            assertThat(adaptiveAllocationsScaler.scale(), nullValue());
        }
        // 1 second with a request, so don't scale.
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(1, 0, 0, 0.05), 1, 1);
        assertThat(adaptiveAllocationsScaler.scale(), nullValue());
        // 15 minutes with no requests, so don't scale.
        for (int i = 0; i < 900; i++) {
            adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(0, 0, 0, 0.05), 1, 1);
            assertThat(adaptiveAllocationsScaler.scale(), nullValue());
        }
        // another second with no requests, so scale to zero allocations.
        adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(0, 0, 0, 0.05), 1, 1);
        assertThat(adaptiveAllocationsScaler.scale(), equalTo(0));
        // 15 minutes with no requests, so don't scale.
        for (int i = 0; i < 900; i++) {
            adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(0, 0, 0, 0.05), 1, 0);
            assertThat(adaptiveAllocationsScaler.scale(), nullValue());
        }
    }

    public void testAutoscaling_dontScaleDownToZeroAllocationsWhenMinAllocationsIsSet() {
        assumeTrue("Should only run if adaptive allocations feature flag is enabled", ScaleToZeroFeatureFlag.isEnabled());

        AdaptiveAllocationsScaler adaptiveAllocationsScaler = new AdaptiveAllocationsScaler("test-deployment", 1);
        adaptiveAllocationsScaler.setMinMaxNumberOfAllocations(1, null);

        // 1 hour with no requests,
        for (int i = 0; i < 3600; i++) {
            adaptiveAllocationsScaler.process(new AdaptiveAllocationsScalerService.Stats(1, 0, 0, 0.05), 1, 1);
            assertThat(adaptiveAllocationsScaler.scale(), nullValue());
        }
    }
}
