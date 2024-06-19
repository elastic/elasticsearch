/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.adapitiveallocations;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
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
}
