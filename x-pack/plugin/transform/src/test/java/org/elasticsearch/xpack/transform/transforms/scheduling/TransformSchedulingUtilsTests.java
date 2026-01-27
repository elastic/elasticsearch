/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.scheduling;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.transform.Transform;

import java.time.Instant;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

public class TransformSchedulingUtilsTests extends ESTestCase {

    public void testCalculateNextScheduledTimeExponentialBackoff() {
        long lastTriggeredTimeMillis = Instant.now().toEpochMilli();
        long[] expectedDelayMillis = {
            Transform.DEFAULT_TRANSFORM_FREQUENCY.millis(),    // normal schedule
            5000,    // 5s
            5000,    // 5s
            8000,    // 8s
            16000,   // 16s
            32000,   // 32s
            64000,   // ~1min
            128000,  // ~2min
            256000,  // ~4min
            512000,  // ~8.5min
            1024000, // ~17min
            2048000, // ~34min
            3600000, // 1h
            3600000, // 1h
            3600000, // 1h
            3600000  // 1h
        };
        for (int failureCount = 0; failureCount < 1000; ++failureCount) {
            assertThat(
                "failureCount = " + failureCount,
                TransformSchedulingUtils.calculateNextScheduledTime(lastTriggeredTimeMillis, null, failureCount),
                is(equalTo(lastTriggeredTimeMillis + expectedDelayMillis[Math.min(failureCount, expectedDelayMillis.length - 1)]))
            );
        }
    }

    public void testCalculateNextScheduledTime() {
        long now = Instant.now().toEpochMilli();
        assertThat(
            TransformSchedulingUtils.calculateNextScheduledTime(null, TimeValue.timeValueSeconds(10), 0),
            is(greaterThanOrEqualTo(now + 10_000))
        );
        assertThat(
            TransformSchedulingUtils.calculateNextScheduledTime(now, null, 0),
            is(equalTo(now + Transform.DEFAULT_TRANSFORM_FREQUENCY.millis()))
        );
        assertThat(
            TransformSchedulingUtils.calculateNextScheduledTime(null, null, 0),
            is(greaterThanOrEqualTo(now + Transform.DEFAULT_TRANSFORM_FREQUENCY.millis()))
        );
    }
}
