/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * The Persistent Tasks Executors are responsible for executing restartable tasks that can survive disappearance of a
 * coordinating and executor nodes.
 * <p>
 * In order to be resilient to node restarts, the persistent tasks are using the cluster state instead of a transport service to send
 * requests and responses. The execution is done in six phases:
 * <p>
 * 1. The coordinating node sends an ordinary transport request to the master node to start a new persistent task. This task is handled
 * by the {@link org.elasticsearch.persistent.PersistentTasksService}, which is using
 * {@link org.elasticsearch.persistent.PersistentTasksClusterService} to update cluster state with the record about running persistent
 * task.
 * <p>
 * 2. The master node updates the {@link org.elasticsearch.persistent.PersistentTasksCustomMetadata} in the cluster state to indicate
 * that there is a new persistent task running in the system.
 * <p>
 * 3. The {@link org.elasticsearch.persistent.PersistentTasksNodeService} running on every node in the cluster monitors changes in
 * the cluster state and starts execution of all new tasks assigned to the node it is running on.
 * <p>
 * 4. If the task fails to start on the node, the {@link org.elasticsearch.persistent.PersistentTasksNodeService} uses the
 * {@link org.elasticsearch.persistent.PersistentTasksCustomMetadata} to notify the
 * {@link org.elasticsearch.persistent.PersistentTasksService}, which reassigns the action to another node in the cluster.
 * <p>
 * 5. If a task finishes successfully on the node and calls listener.onResponse(), the corresponding persistent action is removed from the
 * cluster state unless removeOnCompletion flag for this task is set to false.
 * <p>
 * 6. The {@link org.elasticsearch.persistent.RemovePersistentTaskAction} action can be also used to remove the persistent task.
 */
package org.elasticsearch.persistent;
