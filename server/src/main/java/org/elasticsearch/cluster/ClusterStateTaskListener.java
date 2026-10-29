/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster;

/// A generic interface for [ClusterState] update tasks which can be submitted to the [org.elasticsearch.cluster.service.MasterService] but
/// which might fail before execution.
///
/// Refer to [ClusterStateTaskExecutor] for more information about task execution, and particularly note that the success of a task
/// requires the task to be executed and is therefore handled by [ClusterStateTaskExecutor.TaskContext#success] rather than by the task
/// itself.
public interface ClusterStateTaskListener {
    /// A callback for when the execution of a [ClusterState] update task submitted to the [org.elasticsearch.cluster.service.MasterService]
    /// fails.
    ///
    /// Tasks may themselves fail during execution, either by throwing an exception rather than returning an updated [ClusterState] or
    /// by calling [ClusterStateTaskExecutor.TaskContext#onFailure]. If so, the exception is passed to this callback, on the master-service
    /// thread, before attempting to execute the next batch of tasks.
    ///
    /// Tasks may also fail with a [NotMasterException] before execution, if the node on which they're running is not the elected master.
    /// If so, the exception is passed to this callback, on the master-service thread, before attempting to execute the next batch of tasks.
    /// In practice this tends to drain the whole queue when a node stops being the elected master, but in some cases the node can be
    /// re-elected and start executing tasks again before it completely drains its queue.
    ///
    /// Tasks may also fail with a [NotMasterException] before execution, if the node on which they're running is either starting up or
    /// shutting down and [org.elasticsearch.cluster.service.MasterService#lifecycleState()] is not
    /// [org.elasticsearch.common.component.Lifecycle.State#STARTED]. In that case the exception may be passed to this callback on the
    /// submitting thread, or on the master-service thread.
    ///
    /// Tasks may also fail with a [org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException] before execution, if the
    /// submission of the task specified a timeout and that timeout has elapsed before the task reached the head of the queue. In that case
    /// this callback is completed on a [org.elasticsearch.threadpool.ThreadPool.Names#GENERIC] thread.
    ///
    /// Tasks may fail after execution with a [org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException] if they completed
    /// successfully but the publication of the resulting [ClusterState] failed. In that case this callback is completed on the
    /// master-service thread before attempting to execute the next batch of tasks. When a [ClusterState] publication fails, the publishing
    /// node stops being the elected master and triggers a new master election. The newly elected master may or may not be the same node as
    /// the previous one. The [ClusterState] update may or may not take effect after this election, depending on whether the newly-elected
    /// master node accepted the published state that failed to be committed.
    ///
    /// Use [org.elasticsearch.cluster.service.MasterService#isPublishFailureException] to detect the "expected" master failure cases if
    /// needed.
    ///
    /// Implementations of this callback must not throw exceptions: an exception thrown here is logged by the master service at `ERROR`
    /// level and otherwise ignored, except in tests where it raises an [AssertionError]. If log-and-ignore is the right behaviour then
    /// implementations must do so themselves, typically using a more specific logger and at a less dramatic log level.
    void onFailure(Exception e);
}
