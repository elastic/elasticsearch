/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

/**
 * This package contains all the SLM Rest and Transport actions.
 *
 * <p>The {@link org.elasticsearch.xpack.snapshotlifecycle.action.TransportPutSnapshotLifecycleAction} creates or updates a snapshot
 * lifecycle policy in the cluster state. The {@link org.elasticsearch.xpack.snapshotlifecycle.action.TransportGetSnapshotLifecycleAction}
 * simply retrieves a policy by id. The {@link org.elasticsearch.xpack.snapshotlifecycle.action.TransportDeleteSnapshotLifecycleAction}
 * removes a policy from the cluster state. These actions only interact with the cluster state. Most of the logic that take place in
 * response to these actions happens on the master node in the {@link org.elasticsearch.xpack.snapshotlifecycle.SnapshotLifecycleService}.
 *
 * <p>The {@link org.elasticsearch.xpack.snapshotlifecycle.action.TransportExecuteSnapshotLifecycleAction} operates as if the snapshot
 * policy given was immediately triggered by the scheduler. It does not interfere with any currently scheduled operations, it just runs
 * the snapshot operation ad hoc.
 */
package org.elasticsearch.xpack.snapshotlifecycle.action;
