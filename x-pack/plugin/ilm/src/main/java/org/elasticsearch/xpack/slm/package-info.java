/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

/**
 * This is the Snapshot Lifecycle Management (SLM) main package. SLM is part of the wider ILM feature, reusing quite a bit of the
 * functionality for itself in some places, which is why the two features are contained in the same plugin.
 *
 * This package contains the {@link org.elasticsearch.xpack.slm.SnapshotLifecycleService} and
 * {@link org.elasticsearch.xpack.slm.SnapshotLifecycleTask}, as well as the Rest and Transport actions for the
 * feature set.
 * This package contains the primary execution logic and most of the user facing
 * surface area for the plugin, but not everything. The model objects for the cluster state as well as several supporting classes are
 * contained in the {@link org.elasticsearch.xpack.core.slm} package.
 *
 * <p>{@link org.elasticsearch.xpack.slm.SnapshotLifecycleService} maintains an internal
 * {@link org.elasticsearch.xpack.core.scheduler.SchedulerEngine SchedulerEngine} that handles scheduling snapshots. The service
 * executes on the currently elected master node. It listens to the cluster state, detecting new policies to schedule, and unscheduling
 * policies when they are deleted or if ILM is stopped. The bulk of this scheduling management is handled within
 * {@link org.elasticsearch.xpack.slm.SnapshotLifecycleService#maybeScheduleSnapshot(SnapshotLifecyclePolicyMetadata)}
 * which is executed on all snapshot policies each update.
 *
 * <p>The {@link org.elasticsearch.xpack.slm.SnapshotLifecycleTask} object is what receives an event when a scheduled policy
 * is triggered for execution. It constructs a snapshot request and runs it as the user who originally set up the policy. The bulk of this
 * logic is contained in the
 * {@link org.elasticsearch.xpack.slm.SnapshotLifecycleTask#maybeTakeSnapshot(String, Client, ClusterService,
 * SnapshotHistoryStore)} method. After a snapshot request has been submitted, it persists the result (success or failure) in a history
 * store (an index), caching the latest success and failure information in the cluster state. It is important to note that this task
 * fires the snapshot request off and forgets it; It does not wait until the entire snapshot completes. Any success or failure that this
 * task sees will be from the initial submission of the snapshot request only.
 */
package org.elasticsearch.xpack.slm;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.slm.history.SnapshotHistoryStore;
