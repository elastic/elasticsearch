/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.adaptiveallocations;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class KalmanFilter1dTests extends ESTestCase {

    public void testEstimation_equalValues() {
        KalmanFilter1d filter = new KalmanFilter1d("test-filter", 100, false);
        assertThat(filter.hasValue(), equalTo(false));

        filter.add(42.0, 9.0, false);
        assertThat(filter.hasValue(), equalTo(true));
        assertThat(filter.estimate(), equalTo(42.0));
        assertThat(filter.error(), equalTo(3.0));
        assertThat(filter.lower(), equalTo(39.0));
        assertThat(filter.upper(), equalTo(45.0));

        // With more data the estimation error should go down.
        double previousError = filter.error();
        for (int i = 0; i < 20; i++) {
            filter.add(42.0, 9.0, false);
            assertThat(filter.estimate(), equalTo(42.0));
            assertThat(filter.error(), lessThan(previousError));
            previousError = filter.error();
        }
    }

    public void testEstimation_increasingValues() {
        KalmanFilter1d filter = new KalmanFilter1d("test-filter", 100, false);
        filter.add(10.0, 1.0, false);
        assertThat(filter.estimate(), equalTo(10.0));

        // As the measured values increase, the estimated value should increase too,
        // but it should lag behind.
        double previousEstimate = filter.estimate();
        for (double value = 11.0; value < 20.0; value += 1.0) {
            filter.add(value, 1.0, false);
            assertThat(filter.estimate(), greaterThan(previousEstimate));
            assertThat(filter.estimate(), lessThan(value));
            previousEstimate = filter.estimate();
        }

        // More final values should bring the estimate close to it.
        for (int i = 0; i < 20; i++) {
            filter.add(20.0, 1.0, false);
        }
        assertThat(filter.estimate(), greaterThan(19.0));
        assertThat(filter.estimate(), lessThan(20.0));
    }

    public void testEstimation_bigJumpNoAutoDetectDynamicsChanges() {
        KalmanFilter1d filter = new KalmanFilter1d("test-filter", 100, false);
        filter.add(0.0, 100.0, false);
        filter.add(0.0, 1.0, false);
        assertThat(filter.estimate(), equalTo(0.0));

        // Without dynamics change autodetection the estimated value should be
        // inbetween the old and the new value.
        filter.add(100.0, 1.0, false);
        assertThat(filter.estimate(), greaterThan(49.0));
        assertThat(filter.estimate(), lessThan(51.0));
    }

    public void testEstimation_bigJumpWithAutoDetectDynamicsChanges() {
        KalmanFilter1d filter = new KalmanFilter1d("test-filter", 100, true);
        filter.add(0.0, 100.0, false);
        filter.add(0.0, 1.0, false);
        assertThat(filter.estimate(), equalTo(0.0));

        // With dynamics change autodetection the estimated value should jump
        // instantly to almost the new value.
        filter.add(100.0, 1.0, false);
        assertThat(filter.estimate(), greaterThan(99.0));
        assertThat(filter.estimate(), lessThan(100.0));
    }

    public void testEstimation_bigJumpWithExternalDetectDynamicsChange() {
        KalmanFilter1d filter = new KalmanFilter1d("test-filter", 100, false);
        filter.add(0.0, 100.0, false);
        filter.add(0.0, 1.0, false);
        assertThat(filter.estimate(), equalTo(0.0));

        // For external dynamics changes the estimated value should jump
        // instantly to almost the new value.
        filter.add(100.0, 1.0, true);
        assertThat(filter.estimate(), greaterThan(99.0));
        assertThat(filter.estimate(), lessThan(100.0));
    }

    public void testEstimation_differentSmoothing() {
        KalmanFilter1d quickFilter = new KalmanFilter1d("test-filter", 1e-3, false);
        for (int i = 0; i < 100; i++) {
            quickFilter.add(42.0, 1.0, false);
        }
        assertThat(quickFilter.estimate(), equalTo(42.0));
        // With low smoothing, the value should be close to the new value.
        quickFilter.add(77.0, 1.0, false);
        assertThat(quickFilter.estimate(), greaterThan(75.0));
        assertThat(quickFilter.estimate(), lessThan(77.0));

        KalmanFilter1d slowFilter = new KalmanFilter1d("test-filter", 1e3, false);
        for (int i = 0; i < 100; i++) {
            slowFilter.add(42.0, 1.0, false);
        }
        assertThat(slowFilter.estimate(), equalTo(42.0));
        // With high smoothing, the value should be close to the old value.
        slowFilter.add(77.0, 1.0, false);
        assertThat(slowFilter.estimate(), greaterThan(42.0));
        assertThat(slowFilter.estimate(), lessThan(44.0));
    }
}
