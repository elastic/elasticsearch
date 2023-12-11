/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.scheduling;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.transform.Transform;

import java.util.Objects;

import static org.elasticsearch.xpack.transform.transforms.scheduling.TransformSchedulingUtils.calculateNextScheduledTime;

/**
 * {@link TransformScheduledTask} is a structure describing the scheduled task in the queue.
 * <p>
 * This class is immutable.
 */
final class TransformScheduledTask {

    private final String transformId;
    private final TimeValue frequency;
    private final Long lastTriggeredTimeMillis;
    private final int failureCount;
    private final long nextScheduledTimeMillis;
    private final TransformScheduler.Listener listener;

    TransformScheduledTask(
        String transformId,
        TimeValue frequency,
        Long lastTriggeredTimeMillis,
        int failureCount,
        long nextScheduledTimeMillis,
        TransformScheduler.Listener listener
    ) {
        this.transformId = Objects.requireNonNull(transformId);
        this.frequency = frequency != null ? frequency : Transform.DEFAULT_TRANSFORM_FREQUENCY;
        this.lastTriggeredTimeMillis = lastTriggeredTimeMillis;
        this.failureCount = failureCount;
        this.nextScheduledTimeMillis = nextScheduledTimeMillis;
        this.listener = Objects.requireNonNull(listener);
    }

    TransformScheduledTask(
        String transformId,
        TimeValue frequency,
        Long lastTriggeredTimeMillis,
        int failureCount,
        TransformScheduler.Listener listener
    ) {
        this(
            transformId,
            frequency,
            lastTriggeredTimeMillis,
            failureCount,
            calculateNextScheduledTime(lastTriggeredTimeMillis, frequency, failureCount),
            listener
        );
    }

    String getTransformId() {
        return transformId;
    }

    TimeValue getFrequency() {
        return frequency;
    }

    Long getLastTriggeredTimeMillis() {
        return lastTriggeredTimeMillis;
    }

    int getFailureCount() {
        return failureCount;
    }

    long getNextScheduledTimeMillis() {
        return nextScheduledTimeMillis;
    }

    TransformScheduler.Listener getListener() {
        return listener;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        TransformScheduledTask that = (TransformScheduledTask) other;
        return Objects.equals(this.transformId, that.transformId)
            && Objects.equals(this.frequency, that.frequency)
            && Objects.equals(this.lastTriggeredTimeMillis, that.lastTriggeredTimeMillis)
            && this.failureCount == that.failureCount
            && this.nextScheduledTimeMillis == that.nextScheduledTimeMillis
            && this.listener == that.listener;  // Yes, we purposedly compare the references here
    }

    @Override
    public int hashCode() {
        // To ensure the "equals" and "hashCode" methods have the same view on equality, we use listener's system identity here.
        return Objects.hash(
            transformId,
            frequency,
            lastTriggeredTimeMillis,
            failureCount,
            nextScheduledTimeMillis,
            System.identityHashCode(listener)
        );
    }

    @Override
    public String toString() {
        return new StringBuilder("TransformScheduledTask[").append("transformId=")
            .append(transformId)
            .append(",frequency=")
            .append(frequency)
            .append(",lastTriggeredTimeMillis=")
            .append(lastTriggeredTimeMillis)
            .append(",failureCount=")
            .append(failureCount)
            .append(",nextScheduledTimeMillis=")
            .append(nextScheduledTimeMillis)
            .append(",listener=")
            .append(listener)
            .append("]")
            .toString();
    }
}
