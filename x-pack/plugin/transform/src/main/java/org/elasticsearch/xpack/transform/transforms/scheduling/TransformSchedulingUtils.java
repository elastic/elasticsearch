/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.scheduling;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.transform.Transform;

import java.time.Duration;

public final class TransformSchedulingUtils {

    /**
     * Minimum delay that can be applied after a failure.
     */
    private static final long MIN_DELAY_MILLIS = Duration.ofSeconds(5).toMillis();
    /**
     * Maximum delay that can be applied after a failure.
     */
    private static final long MAX_DELAY_MILLIS = Duration.ofHours(1).toMillis();

    /**
     * Calculates the appropriate next scheduled time taking number of failures into account.
     * This method implements exponential backoff approach.
     *
     * @param lastTriggeredTimeMillis the last time (in millis) the task was triggered
     * @param frequency               the frequency of the transform
     * @param failureCount            the number of failures that happened since the task was triggered
     * @return next scheduled time for a task
     */
    public static long calculateNextScheduledTime(Long lastTriggeredTimeMillis, TimeValue frequency, int failureCount) {
        final long baseTime = lastTriggeredTimeMillis != null ? lastTriggeredTimeMillis : System.currentTimeMillis();

        if (failureCount == 0) {
            return baseTime + (frequency != null ? frequency : Transform.DEFAULT_TRANSFORM_FREQUENCY).millis();
        }

        // Math.min(failureCount, 32) is applied in order to avoid overflow.
        long delayMillis = Math.min(Math.max((1L << Math.min(failureCount, 32)) * 1000, MIN_DELAY_MILLIS), MAX_DELAY_MILLIS);
        return baseTime + delayMillis;
    }
}
