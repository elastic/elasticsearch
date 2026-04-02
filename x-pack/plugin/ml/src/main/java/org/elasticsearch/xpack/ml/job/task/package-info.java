/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Manages the lifecycle of Anomaly Detection (AD) job persistent tasks on an assigned ML node.
 *
 * <h2>Responsibility</h2>
 * <p>
 * {@link org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutor} is the entry point
 * invoked by the persistent task framework whenever a job is assigned to this node. It orchestrates
 * the full opening pipeline: results-index mapping update → snapshot-version verification →
 * forecast cleanup → datafeed check → snapshot revert (if needed) → native-process creation and
 * state restore.
 *
 * <h2>Retry Resilience (system-initiated reassignments)</h2>
 * <p>
 * The opening pipeline distinguishes two request sources:
 * <p>
 * 1. <b>User-initiated opens</b> (no prior {@code JobTaskState}, i.e. allocation IDs match):
 *    the pipeline runs directly. Any error immediately calls
 *    {@link org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutor#failTask failTask()},
 *    giving the user fast, clear feedback.
 * <p>
 * 2. <b>System-initiated reassignments</b> (allocation ID in persisted {@code JobTaskState} differs
 *    from the current task allocation ID, indicating the task was previously assigned elsewhere):
 *    the pipeline is wrapped in {@code OpenJobRetryableAction}, which retries with exponential
 *    backoff (5 s initial delay, 5 min cap, configurable total timeout via
 *    {@code xpack.ml.job_open_retry_timeout}, default 60 min). Only errors classified as
 *    recoverable by
 *    {@link org.elasticsearch.xpack.ml.utils.MlRecoverableErrorClassifier MlRecoverableErrorClassifier}
 *    trigger a retry; irrecoverable errors (bad request, not found, task cancelled, etc.) still
 *    fail fast. The retry loop also stops immediately if the task is closing or vacating.
 * <p>
 * This design ensures jobs survive transient infrastructure disruptions (master unavailability,
 * shard relocations, thread-pool saturation, transport errors) during rolling upgrades and node
 * failures, without degrading the user experience for interactive job opens.
 *
 * <h2>State Transition Robustness</h2>
 * <p>
 * Committing job state to cluster state (both {@code OPENED} and {@code FAILED}) is wrapped in
 * {@link org.elasticsearch.xpack.ml.job.task.UpdateStateRetryableAction UpdateStateRetryableAction},
 * which retries the {@code updatePersistentTaskState} call until the master acknowledges it or a
 * timeout is reached. This prevents ghost tasks stuck in {@code OPENING} when the master is
 * temporarily unavailable. {@code setJobState(OPENED)} uses a 365-day timeout (a running native
 * process must be matched with a committed state). {@code failTask()} uses the same configurable
 * timeout as the open-pipeline retry window.
 *
 * <h2>Process Boundary</h2>
 * <p>
 * All pre-process operations (mapping updates, searches, index creation, {@code getAutodetectParams})
 * are idempotent and safe to repeat on retry. Errors in these steps propagate through the listener
 * callback to the orchestrator, which decides whether to retry. Once the native process is created
 * (the process boundary), errors instead trigger process cleanup and {@code setJobState(FAILED)};
 * process creation is not retriable.
 */
package org.elasticsearch.xpack.ml.job.task;
