/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.cache;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class FrequencySketchTests extends ESTestCase {

    public void testEstimateNeverUnderCountsBetweenHalvings() {
        final FrequencySketch sketch = new FrequencySketch(1024, 1_000_000);
        for (int key = 0; key < 100; key++) {
            for (int i = 0; i <= key % 7; i++) {
                sketch.incrementAndEstimate(key);
            }
        }
        for (int key = 0; key < 100; key++) {
            assertThat(sketch.estimate(key), greaterThanOrEqualTo(key % 7 + 1));
        }
    }

    public void testHalvesAfterTheConfiguredNumberOfAccesses() {
        final long sample = randomLongBetween(50, 500);
        final FrequencySketch sketch = new FrequencySketch(64, sample);
        int halvings = 0;
        int lastEstimate = 0;
        for (long i = 1; i <= 3 * sample; i++) {
            lastEstimate = sketch.incrementAndEstimate(7);
            if (sketch.wasHalved()) {
                halvings++;
                assertThat("halving happens exactly every sampleSize/2 increments after the first", i >= sample, equalTo(true));
            }
        }
        assertThat(halvings >= 2, equalTo(true));
        assertThat((long) lastEstimate, lessThanOrEqualTo(sample));
    }

    public void testDefaultSampleSizeIsTenTimesTheWidth() {
        final FrequencySketch sketch = new FrequencySketch(16);
        for (int i = 1; i < 160; i++) {
            sketch.incrementAndEstimate(1);
            assertThat(sketch.wasHalved(), equalTo(false));
        }
        sketch.incrementAndEstimate(1);
        assertThat(sketch.wasHalved(), equalTo(true));
    }
}
