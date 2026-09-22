/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;

public class ObservedMemoryEstimatorTests extends ESTestCase {

    public void testFirstObservationDividesByAllocations() {
        ObservedMemoryEstimator estimator = new ObservedMemoryEstimator();
        assertThat(estimator.get("d"), is(nullValue()));

        long effective = estimator.update("d", 4_000L, 4);
        assertThat(effective, equalTo(1_000L));
        assertThat(estimator.get("d"), equalTo(1_000L));
    }

    public void testAllocationsFlooredAtOne() {
        ObservedMemoryEstimator estimator = new ObservedMemoryEstimator();
        assertThat(estimator.update("d", 2_000L, 0), equalTo(2_000L));
        assertThat(estimator.update("e", 2_000L, -3), equalTo(2_000L));
    }

    public void testRatchetsUpInstantly() {
        ObservedMemoryEstimator estimator = new ObservedMemoryEstimator();
        estimator.update("d", 1_000L, 1);
        // A larger observation is adopted immediately so the memory guards stay conservative.
        assertThat(estimator.update("d", 5_000L, 1), equalTo(5_000L));
    }

    public void testDecaysSlowlyOnSmallerObservations() {
        ObservedMemoryEstimator estimator = new ObservedMemoryEstimator();
        estimator.update("d", 10_000L, 1);

        long afterOneStep = estimator.update("d", 0L, 1);
        // With a decay factor of 0.1 a single step towards zero only removes ~10% of the estimate.
        assertThat(afterOneStep, equalTo(9_000L));
        assertThat(afterOneStep, greaterThan(0L));

        // Repeated smaller observations continue to pull the estimate down gradually rather than instantly.
        long afterTwoSteps = estimator.update("d", 0L, 1);
        assertThat(afterTwoSteps, lessThan(afterOneStep));
        assertThat(afterTwoSteps, greaterThan(0L));
    }

    public void testEstimatesAreIndependentPerDeployment() {
        ObservedMemoryEstimator estimator = new ObservedMemoryEstimator();
        estimator.update("d1", 1_000L, 1);
        estimator.update("d2", 8_000L, 1);
        assertThat(estimator.get("d1"), equalTo(1_000L));
        assertThat(estimator.get("d2"), equalTo(8_000L));
    }

    public void testRemoveForgetsState() {
        ObservedMemoryEstimator estimator = new ObservedMemoryEstimator();
        estimator.update("d", 1_000L, 1);
        estimator.remove("d");
        assertThat(estimator.get("d"), is(nullValue()));
    }
}
