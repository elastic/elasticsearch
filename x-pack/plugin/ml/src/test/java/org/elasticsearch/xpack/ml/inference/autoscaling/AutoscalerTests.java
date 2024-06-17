/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.autoscaling;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class AutoscalerTests extends ESTestCase {

    public void testAutoscaling_scaleUpAndDown() {
        Autoscaler autoscaler = new Autoscaler("test-deployment", 1);

        // With 1 allocation the system can handle 500 requests * 0.020 sec/request.
        // To handle remaining requests the system should scale to 2 allocations.
        autoscaler.process(new AutoscalerService.Stats(500, 100, 100, 0.020), 10, 1);
        assertThat(autoscaler.autoscale(), equalTo(2));

        // With 2 allocation the system can handle 800 requests * 0.025 sec/request.
        // To handle remaining requests the system should scale to 3 allocations.
        autoscaler.process(new AutoscalerService.Stats(800, 100, 50, 0.025), 10, 2);
        assertThat(autoscaler.autoscale(), equalTo(3));

        // With 3 allocations the system can handle the load.
        autoscaler.process(new AutoscalerService.Stats(1000, 0, 0, 0.025), 10, 3);
        assertThat(autoscaler.autoscale(), nullValue());

        // No load anymore, so the system should gradually scale down to 1 allocation.
        autoscaler.process(new AutoscalerService.Stats(0, 0, 0, Double.NaN), 10, 3);
        assertThat(autoscaler.autoscale(), nullValue());
        autoscaler.process(new AutoscalerService.Stats(0, 0, 0, Double.NaN), 10, 3);
        assertThat(autoscaler.autoscale(), equalTo(2));
        autoscaler.process(new AutoscalerService.Stats(0, 0, 0, Double.NaN), 10, 2);
        assertThat(autoscaler.autoscale(), nullValue());
        autoscaler.process(new AutoscalerService.Stats(0, 0, 0, Double.NaN), 10, 2);
        assertThat(autoscaler.autoscale(), equalTo(1));
    }

    public void testAutoscaling_noOscillating() {
        Autoscaler autoscaler = new Autoscaler("test-deployment", 1);

        // With 1 allocation the system can handle 880 requests * 0.010 sec/request.
        autoscaler.process(new AutoscalerService.Stats(880, 0, 0, 0.010), 10, 1);
        assertThat(autoscaler.autoscale(), nullValue());
        autoscaler.process(new AutoscalerService.Stats(880, 0, 0, 0.010), 10, 1);
        assertThat(autoscaler.autoscale(), nullValue());

        // Increase the load to 980 requests * 0.010 sec/request, and the system
        // should scale to 2 allocations to have some spare capacity.
        autoscaler.process(new AutoscalerService.Stats(920, 0, 0, 0.010), 10, 1);
        assertThat(autoscaler.autoscale(), nullValue());
        autoscaler.process(new AutoscalerService.Stats(950, 0, 0, 0.010), 10, 1);
        assertThat(autoscaler.autoscale(), nullValue());
        autoscaler.process(new AutoscalerService.Stats(980, 0, 0, 0.010), 10, 1);
        assertThat(autoscaler.autoscale(), equalTo(2));
        autoscaler.process(new AutoscalerService.Stats(980, 0, 0, 0.010), 10, 2);
        assertThat(autoscaler.autoscale(), nullValue());

        // Reducing the load to just 880 requests * 0.010 sec/request should not
        // trigger scaling down again, to prevent oscillating.
        autoscaler.process(new AutoscalerService.Stats(880, 0, 0, 0.010), 10, 2);
        assertThat(autoscaler.autoscale(), nullValue());
        autoscaler.process(new AutoscalerService.Stats(880, 0, 0, 0.010), 10, 2);
        assertThat(autoscaler.autoscale(), nullValue());
    }

    public void testAutoscaling_respectMinMaxAllocations() {
        Autoscaler autoscaler = new Autoscaler("test-deployment", 1);
        autoscaler.setMinMaxNumberOfAllocations(2, 5);

        // Even though there are no requests, scale to the minimum of 2 allocations.
        autoscaler.process(new AutoscalerService.Stats(0, 0, 0, 0.010), 10, 1);
        assertThat(autoscaler.autoscale(), equalTo(2));
        autoscaler.process(new AutoscalerService.Stats(0, 0, 0, 0.010), 10, 2);
        assertThat(autoscaler.autoscale(), nullValue());

        // Even though there are many requests, the scale to the maximum of 5 allocations.
        autoscaler.process(new AutoscalerService.Stats(100, 10000, 1000, 0.010), 10, 2);
        assertThat(autoscaler.autoscale(), equalTo(5));
        autoscaler.process(new AutoscalerService.Stats(500, 10000, 1000, 0.010), 10, 5);
        assertThat(autoscaler.autoscale(), nullValue());

        // After a while of no requests, scale to the minimum of 2 allocations.
        autoscaler.process(new AutoscalerService.Stats(0, 0, 0, 0.010), 10, 5);
        autoscaler.process(new AutoscalerService.Stats(0, 0, 0, 0.010), 10, 5);
        autoscaler.process(new AutoscalerService.Stats(0, 0, 0, 0.010), 10, 5);
        assertThat(autoscaler.autoscale(), equalTo(2));
    }
}
