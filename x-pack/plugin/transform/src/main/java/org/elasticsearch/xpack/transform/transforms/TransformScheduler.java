/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;
import org.elasticsearch.xpack.transform.Transform;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * {@link TransformScheduler} class is responsible for scheduling transform tasks according to their configured frequency as well as
 * retrying policy.
 */
public class TransformScheduler {

    /**
     * {@link Event} class encapsulates data that might be useful to the listener when the listener is being triggered.
     *
     * This class is immutable.
     */
    public static class Event {
        private final String transformId;
        private final long scheduledTime;
        private final long triggeredTime;

        public Event(String transformId, long scheduledTime, long triggeredTime) {
            this.transformId = transformId;
            this.scheduledTime = scheduledTime;
            this.triggeredTime = triggeredTime;
        }

        public String getTransformId() {
            return transformId;
        }

        public long getScheduledTime() {
            return scheduledTime;
        }

        public long getTriggeredTime() {
            return triggeredTime;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (other == null || getClass() != other.getClass()) return false;
            Event that = (Event) other;
            return Objects.equals(this.transformId, that.transformId)
                && this.scheduledTime == that.scheduledTime
                && this.triggeredTime == that.triggeredTime;
        }

        @Override
        public int hashCode() {
            return Objects.hash(transformId, scheduledTime, triggeredTime);
        }

        @Override
        public String toString() {
            return new StringBuilder("Event[").append("transformId=")
                .append(transformId)
                .append(",scheduledTime=")
                .append(Instant.ofEpochMilli(scheduledTime))
                .append(",triggeredTime=")
                .append(Instant.ofEpochMilli(triggeredTime))
                .append("]")
                .toString();
        }
    }

    /**
     * {@link Listener} interface allows receiving events about the scheduled task being triggered.
     */
    public interface Listener {
        /**
         * Called whenever scheduled task is being triggered.
         *
         * @param event event structure containing data that might be useful to the listener
         */
        void triggered(Event event);
    }

    /**
     * {@link ScheduledTransformTask} is a structure describing the scheduled task in the queue.
     *
     * This class is immutable.
     */
    static class ScheduledTransformTask {

        /** Minimum delay that can be applied after a failure. */
        private static final long MIN_DELAY_MILLIS = Duration.ofSeconds(1).toMillis();
        /** Maximum delay that can be applied after a failure. */
        private static final long MAX_DELAY_MILLIS = Duration.ofHours(1).toMillis();

        private final String transformId;
        private final TimeValue frequency;
        private final Long lastTriggeredTimeMillis;
        private final int failureCount;
        private final long nextScheduledTimeMillis;
        private final Listener listener;

        ScheduledTransformTask(
            String transformId,
            TimeValue frequency,
            Long lastTriggeredTimeMillis,
            int failureCount,
            long nextScheduledTimeMillis,
            Listener listener
        ) {
            this.transformId = Objects.requireNonNull(transformId);
            this.frequency = frequency != null ? frequency : Transform.DEFAULT_TRANSFORM_FREQUENCY;
            this.lastTriggeredTimeMillis = lastTriggeredTimeMillis;
            this.failureCount = failureCount;
            this.nextScheduledTimeMillis = nextScheduledTimeMillis;
            this.listener = Objects.requireNonNull(listener);
        }

        ScheduledTransformTask(String transformId, TimeValue frequency, Long lastTriggeredTimeMillis, int failureCount, Listener listener) {
            this(
                transformId,
                frequency,
                lastTriggeredTimeMillis,
                failureCount,
                failureCount == 0
                    ? lastTriggeredTimeMillis + frequency.millis()
                    : calculateNextScheduledTimeAfterFailure(lastTriggeredTimeMillis, failureCount),
                listener
            );
        }

        // Visible for testing
        /**
         * Calculates the appropriate next scheduled time after a number of failures.
         * This method implements exponential backoff approach.
         *
         * @param lastTriggeredTimeMillis the last time (in millis) the task was triggered
         * @param failureCount the number of failures that happened since the task was triggered
         * @return next scheduled time for a task
         */
        static long calculateNextScheduledTimeAfterFailure(long lastTriggeredTimeMillis, int failureCount) {
            long delayMillis = Math.min(Math.max((long) Math.pow(2, failureCount) * 1000, MIN_DELAY_MILLIS), MAX_DELAY_MILLIS);
            return lastTriggeredTimeMillis + delayMillis;
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

        Listener getListener() {
            return listener;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (other == null || getClass() != other.getClass()) return false;
            ScheduledTransformTask that = (ScheduledTransformTask) other;
            return Objects.equals(this.transformId, that.transformId)
                && Objects.equals(this.frequency, that.frequency)
                && Objects.equals(this.lastTriggeredTimeMillis, that.lastTriggeredTimeMillis)
                && this.failureCount == that.failureCount
                && this.nextScheduledTimeMillis == that.nextScheduledTimeMillis
                && this.listener == that.listener;  // Yes, we purposedly compare the references here
        }

        @Override
        public int hashCode() {
            return Objects.hash(transformId, frequency, lastTriggeredTimeMillis, failureCount, nextScheduledTimeMillis, listener);
        }

        @Override
        public String toString() {
            return new StringBuilder("ScheduledTransformTask[").append("transformId=")
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

    private static final Logger logger = LogManager.getLogger(TransformScheduler.class);

    private final Clock clock;
    private final ThreadPool threadPool;
    private final TimeValue schedulerFrequency;
    private final ConcurrentPriorityQueue<ScheduledTransformTask> scheduledTasks;
    /**
     * Prevents two concurrent invocations of the "processScheduledTasks" method.
     *
     * Set to {@code true} before processing
     * Set to {@code false} after processing (doesn't matter whether successful or not).
     */
    private final AtomicBoolean isProcessingActive;

    public TransformScheduler(Clock clock, ThreadPool threadPool, Settings settings) {
        this.clock = Objects.requireNonNull(clock);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.schedulerFrequency = Transform.SCHEDULER_FREQUENCY.get(settings);
        this.scheduledTasks = new ConcurrentPriorityQueue<>(
            ScheduledTransformTask::getTransformId,
            ScheduledTransformTask::getNextScheduledTimeMillis
        );
        this.isProcessingActive = new AtomicBoolean();
    }

    /**
     * Starts the processing.
     * Method {@code processScheduledTasks} is invoked according to its configured {@code schedulerFrequency}.
     */
    public void start() {
        threadPool.scheduleWithFixedDelay(this::processScheduledTasks, schedulerFrequency, ThreadPool.Names.GENERIC);
    }

    // Visible for testing
    void processScheduledTasks() {
        // Prevent two concurrent invocations of the "processScheduledTasks" method
        if (isProcessingActive.compareAndSet(false, true) == false) {
            return;
        }
        logger.trace("Processing scheduled tasks started");
        Instant processingStarted = clock.instant();
        try {
            processScheduledTasksInternal();
        } finally {
            // Make sure we clear the "isProcessingActive" bit regardless of whether there was a success or failure.
            // Otherwise, the processing would be stuck forever.
            isProcessingActive.set(false);
        }
        Instant processingFinished = clock.instant();
        logger.trace("Processing scheduled tasks finished, took {}ms", Duration.between(processingStarted, processingFinished).toMillis());
    }

    private void processScheduledTasksInternal() {
        if (scheduledTasks.isEmpty()) {
            // There are no scheduled tasks, hence, nothing to do
            return;
        }
        long currentTimeMillis = clock.millis();
        ScheduledTransformTask scheduledTask = scheduledTasks.first();
        // Check if the task is eligible for processing
        if (currentTimeMillis < scheduledTask.getNextScheduledTimeMillis()) {
            // It is too early to process this task.
            // Consequently, it is too early to process other tasks because the tasks are sorted by their next scheduled time.
            // Try again later.
            return;
        }
        // Create event that will be sent to the listener
        Event event = new Event(scheduledTask.getTransformId(), scheduledTask.getNextScheduledTimeMillis(), currentTimeMillis);
        // Trigger the listener
        scheduledTask.getListener().triggered(event);
        // Update the task's last_triggered_time to current time (next_scheduled_time gets automatically re-calculated)
        scheduledTasks.update(scheduledTask.getTransformId(), task -> {
            if (task.equals(scheduledTask) == false) {
                logger.debug(
                    new ParameterizedMessage(
                        "[{}] task object got modified while processing. Expected: {}, was: {}",
                        scheduledTask.getTransformId(),
                        scheduledTask,
                        task
                    )
                );
            }
            return new ScheduledTransformTask(
                task.getTransformId(),
                task.getFrequency(),
                currentTimeMillis,
                task.getFailureCount(),
                task.getListener()
            );
        });
    }

    /**
     * Stops the processing.
     */
    public void stop() {
        threadPool.shutdown();
    }

    /**
     * Registers the transform by adding it to the queue.
     *
     * @param transformTaskParams structure containing transform's id and frequency
     * @param listener listener to be triggered
     */
    public void registerTransform(TransformTaskParams transformTaskParams, Listener listener) {
        String transformId = transformTaskParams.getId();
        logger.trace(new ParameterizedMessage("[{}] about to register the transform", transformId));
        long currentTimeMillis = clock.millis();
        ScheduledTransformTask scheduledTransformTask = new ScheduledTransformTask(
            transformId,
            transformTaskParams.getFrequency(),
            null,  // this task has not been triggered yet
            0,  // this task has not failed yet
            currentTimeMillis,  // we schedule this task at current clock time so that it is processed ASAP
            listener
        );
        scheduledTasks.add(scheduledTransformTask);
        processScheduledTasks();
    }

    /**
     * Updates the transform task's failure count.
     * Updating the failure count affects the task's next_scheduled_time and may result in the task being processed earlier that it would
     * normally (i.e.: according to its frequency) be.
     *
     * @param transformId id of the transform to update
     * @param failureCount new value of transform task's failure count
     */
    public void handleTransformFailureCountChanged(String transformId, int failureCount) {
        logger.trace(new ParameterizedMessage("[{}] about to handle transform failure count to {}", transformId, failureCount));
        // Update the task's failure count (next_scheduled_time gets automatically re-calculated)
        scheduledTasks.update(
            transformId,
            task -> new ScheduledTransformTask(
                task.getTransformId(),
                task.getFrequency(),
                task.getLastTriggeredTimeMillis(),
                failureCount,
                task.getListener()
            )
        );
    }

    /**
     * De-registers the given transform by removing it from the queue.
     *
     * @param transformId id of the transform to de-register
     */
    public void deregisterTransform(String transformId) {
        Objects.requireNonNull(transformId);
        logger.trace(new ParameterizedMessage("[{}] about to de-register the transform", transformId));
        scheduledTasks.remove(transformId);
    }

    // Visible for testing
    /**
     * @return queue current contents
     */
    List<ScheduledTransformTask> getScheduledTransformTasks() {
        return StreamSupport.stream(scheduledTasks.spliterator(), false).collect(toUnmodifiableList());
    }
}
