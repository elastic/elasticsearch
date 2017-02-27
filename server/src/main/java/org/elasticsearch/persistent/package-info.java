/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * The Persistent Actions are actions responsible for executing restartable actions that can survive disappearance of a
 * coordinating and executor nodes.
 * <p>
 * In order to be resilient to node restarts, the persistent actions are using the cluster state instead of a transport service to send
 * requests and responses. The execution is done in six phases:
 * <p>
 * 1. The coordinating node sends an ordinary transport request to the master node to start a new persistent action. This action is handled
 * by the {@link org.elasticsearch.persistent.PersistentActionService}, which is using
 * {@link org.elasticsearch.persistent.PersistentTaskClusterService} to update cluster state with the record about running persistent
 * task.
 * <p>
 * 2. The master node updates the {@link org.elasticsearch.persistent.PersistentTasks} in the cluster state to indicate that
 * there is a new persistent action
 * running in the system.
 * <p>
 * 3. The {@link org.elasticsearch.persistent.PersistentActionCoordinator} running on every node in the cluster monitors changes in
 * the cluster state and starts execution of all new actions assigned to the node it is running on.
 * <p>
 * 4. If the action fails to start on the node, the {@link org.elasticsearch.persistent.PersistentActionCoordinator} uses the
 * {@link org.elasticsearch.persistent.PersistentTasks} to notify the
 * {@link org.elasticsearch.persistent.PersistentActionService}, which reassigns the action to another node in the cluster.
 * <p>
 * 5. If action finishes successfully on the node and calls listener.onResponse(), the corresponding persistent action is removed from the
 * cluster state unless .
 * <p>
 * 6. The {@link org.elasticsearch.persistent.RemovePersistentTaskAction} action can be also used to remove the persistent action.
 */
package org.elasticsearch.persistent;
