/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.scheduling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;
import org.elasticsearch.xpack.transform.Transform;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.core.Strings.format;

/**
 * {@link TransformScheduler} class is responsible for scheduling transform tasks according to their configured frequency as well as
 * retrying policy.
 */
public final class TransformScheduler {

    /**
     * {@link Event} record encapsulates data that might be useful to the listener when the listener is being triggered.
     */
    public record Event(String transformId, long scheduledTime, long triggeredTime) {
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

    private static final Logger logger = LogManager.getLogger(TransformScheduler.class);

    private final Clock clock;
    private final ThreadPool threadPool;
    private final TimeValue schedulerFrequency;
    private final TransformScheduledTaskQueue scheduledTasks;
    /**
     * Prevents two concurrent invocations of the "processScheduledTasks" method.
     *
     * Set to {@code true} before processing
     * Set to {@code false} after processing (doesn't matter whether successful or not).
     */
    private final AtomicBoolean isProcessingActive;
    /**
     * Stored the scheduled execution for future cancellation.
     */
    private Scheduler.Cancellable scheduledFuture;

    public TransformScheduler(Clock clock, ThreadPool threadPool, Settings settings) {
        this.clock = Objects.requireNonNull(clock);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.schedulerFrequency = Transform.SCHEDULER_FREQUENCY.get(settings);
        this.scheduledTasks = new TransformScheduledTaskQueue();
        this.isProcessingActive = new AtomicBoolean();
    }

    /**
     * Starts the processing.
     * Method {@code processScheduledTasks} is invoked according to its configured {@code schedulerFrequency}.
     */
    public void start() {
        if (scheduledFuture == null) {
            scheduledFuture = threadPool.scheduleWithFixedDelay(this::processScheduledTasks, schedulerFrequency, ThreadPool.Names.GENERIC);
        }
    }

    // Visible for testing
    void processScheduledTasks() {
        // Prevent two concurrent invocations of the "processScheduledTasks" method
        if (isProcessingActive.compareAndSet(false, true) == false) {
            return;
        }
        logger.trace("Processing scheduled tasks started");
        final boolean isTraceEnabled = logger.isTraceEnabled();
        Instant processingStarted = isTraceEnabled ? clock.instant() : null;
        final boolean taskWasProcessed;
        try {
            taskWasProcessed = processScheduledTasksInternal();
        } finally {
            // Make sure we clear the "isProcessingActive" bit regardless of whether there was a success or failure.
            // Otherwise, the processing would be stuck forever.
            isProcessingActive.set(false);
        }
        if (isTraceEnabled) {
            Instant processingFinished = clock.instant();
            logger.trace(
                format("Processing scheduled tasks finished, took %dms", Duration.between(processingStarted, processingFinished).toMillis())
            );
        }
        if (taskWasProcessed == false) {
            return;
        }
        // If we happened to process the task, there may be other tasks also eligible for processing.
        // We try to process them ASAP as we don't want to wait the `delay` for every task.
        // Tail call optimization is enforced by making the following call the last call in this method.
        processScheduledTasks();
    }

    private boolean processScheduledTasksInternal() {
        TransformScheduledTask scheduledTask = scheduledTasks.first();

        if (scheduledTask == null) {
            // There are no scheduled tasks, hence, nothing to do
            return false;
        }
        long currentTimeMillis = clock.millis();

        // Check if the task is eligible for processing
        if (currentTimeMillis < scheduledTask.getNextScheduledTimeMillis()) {
            // It is too early to process this task.
            // Consequently, it is too early to process other tasks because the tasks are sorted by their next scheduled time.
            // Try again later.
            return false;
        }
        // Create event that will be sent to the listener
        Event event = new Event(scheduledTask.getTransformId(), scheduledTask.getNextScheduledTimeMillis(), currentTimeMillis);
        // Trigger the listener
        scheduledTask.getListener().triggered(event);
        // Update the task's last_triggered_time to current time (next_scheduled_time gets automatically re-calculated)
        scheduledTasks.update(scheduledTask.getTransformId(), task -> {
            if (task.equals(scheduledTask) == false) {
                logger.debug(
                    () -> format(
                        "[%s] task object got modified while processing. Expected: %s, was: %s",
                        scheduledTask.getTransformId(),
                        scheduledTask,
                        task
                    )
                );
            }
            return new TransformScheduledTask(
                task.getTransformId(),
                task.getFrequency(),
                currentTimeMillis,
                task.getFailureCount(),
                task.getListener()
            );
        });
        return true;
    }

    /**
     * Stops the processing.
     */
    public void stop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
            scheduledFuture = null;
        }
    }

    /**
     * Registers and triggers the transform.
     * The transform is registered by adding it to the queue and then the transform is triggered immediately so that it does not have to
     * wait until the next scheduled run.
     *
     * @param transformTaskParams structure containing transform's id and frequency
     * @param listener listener to be triggered
     */
    public void registerTransform(TransformTaskParams transformTaskParams, Listener listener) {
        String transformId = transformTaskParams.getId();
        logger.trace(() -> format("[%s] register the transform", transformId));
        long currentTimeMillis = clock.millis();
        TransformScheduledTask transformScheduledTask = new TransformScheduledTask(
            transformId,
            transformTaskParams.getFrequency(),
            null,  // this task has not been triggered yet
            0,  // this task has not failed yet
            currentTimeMillis,  // we schedule this task at current clock time so that it is processed ASAP
            listener
        );
        scheduledTasks.add(transformScheduledTask);
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
        logger.trace(() -> format("[%s] handle transform failure count change to %d", transformId, failureCount));
        // Update the task's failure count (next_scheduled_time gets automatically re-calculated)
        scheduledTasks.update(
            transformId,
            task -> new TransformScheduledTask(
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
        logger.trace(() -> format("[%s] de-register the transform", transformId));
        scheduledTasks.remove(transformId);
    }

    // Visible for testing
    /**
     * @return queue current contents
     */
    List<TransformScheduledTask> getTransformScheduledTasks() {
        return scheduledTasks.listScheduledTasks();
    }
}
