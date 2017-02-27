/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

/**
 * The Persistent Actions are actions responsible for executing restartable actions that can survive disappearance of a
 * coordinating and executor nodes.
 * <p>
 * In order to be resilient to node restarts, the persistent actions are using the cluster state instead of a transport service to send
 * requests and responses. The execution is done in six phases:
 * <p>
 * 1. The coordinating node sends an ordinary transport request to the master node to start a new persistent action. This action is handled
 * by the {@link org.elasticsearch.xpack.persistent.PersistentActionService}, which is using
 * {@link org.elasticsearch.xpack.persistent.PersistentTaskClusterService} to update cluster state with the record about running persistent
 * task.
 * <p>
 * 2. The master node updates the {@link org.elasticsearch.xpack.persistent.PersistentTasks} in the cluster state to indicate that
 * there is a new persistent action
 * running in the system.
 * <p>
 * 3. The {@link org.elasticsearch.xpack.persistent.PersistentActionCoordinator} running on every node in the cluster monitors changes in
 * the cluster state and starts execution of all new actions assigned to the node it is running on.
 * <p>
 * 4. If the action fails to start on the node, the {@link org.elasticsearch.xpack.persistent.PersistentActionCoordinator} uses the
 * {@link org.elasticsearch.xpack.persistent.PersistentTasks} to notify the
 * {@link org.elasticsearch.xpack.persistent.PersistentActionService}, which reassigns the action to another node in the cluster.
 * <p>
 * 5. If action finishes successfully on the node and calls listener.onResponse(), the corresponding persistent action is removed from the
 * cluster state unless .
 * <p>
 * 6. The {@link org.elasticsearch.xpack.persistent.RemovePersistentTaskAction} action can be also used to remove the persistent action.
 */
package org.elasticsearch.xpack.persistent;