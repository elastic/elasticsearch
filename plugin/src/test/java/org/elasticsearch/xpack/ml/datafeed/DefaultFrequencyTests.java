/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.test.ESTestCase;

import java.time.Duration;

public class DefaultFrequencyTests extends ESTestCase {

    public void testCalc_GivenNegative() {
        ESTestCase.expectThrows(IllegalArgumentException.class, () -> DefaultFrequency.ofBucketSpan(-1));
    }


    public void testCalc() {
        assertEquals(Duration.ofMinutes(1), DefaultFrequency.ofBucketSpan(1));
        assertEquals(Duration.ofMinutes(1), DefaultFrequency.ofBucketSpan(30));
        assertEquals(Duration.ofMinutes(1), DefaultFrequency.ofBucketSpan(60));
        assertEquals(Duration.ofMinutes(1), DefaultFrequency.ofBucketSpan(90));
        assertEquals(Duration.ofMinutes(1), DefaultFrequency.ofBucketSpan(120));
        assertEquals(Duration.ofMinutes(1), DefaultFrequency.ofBucketSpan(121));

        assertEquals(Duration.ofSeconds(61), DefaultFrequency.ofBucketSpan(122));
        assertEquals(Duration.ofSeconds(75), DefaultFrequency.ofBucketSpan(150));
        assertEquals(Duration.ofSeconds(150), DefaultFrequency.ofBucketSpan(300));
        assertEquals(Duration.ofMinutes(10), DefaultFrequency.ofBucketSpan(1200));

        assertEquals(Duration.ofMinutes(10), DefaultFrequency.ofBucketSpan(1201));
        assertEquals(Duration.ofMinutes(10), DefaultFrequency.ofBucketSpan(1800));
        assertEquals(Duration.ofMinutes(10), DefaultFrequency.ofBucketSpan(3600));
        assertEquals(Duration.ofMinutes(10), DefaultFrequency.ofBucketSpan(7200));
        assertEquals(Duration.ofMinutes(10), DefaultFrequency.ofBucketSpan(12 * 3600));

        assertEquals(Duration.ofHours(1), DefaultFrequency.ofBucketSpan(12 * 3600 + 1));
        assertEquals(Duration.ofHours(1), DefaultFrequency.ofBucketSpan(13 * 3600));
        assertEquals(Duration.ofHours(1), DefaultFrequency.ofBucketSpan(24 * 3600));
        assertEquals(Duration.ofHours(1), DefaultFrequency.ofBucketSpan(48 * 3600));
    }
}
