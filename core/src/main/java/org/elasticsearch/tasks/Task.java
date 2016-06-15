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


package org.elasticsearch.tasks;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContent;

import java.io.IOException;

/**
 * Current task information
 */
public class Task {

    private final long id;

    private final String type;

    private final String action;

    private final String description;

    private final TaskId parentTask;

    private final long startTime;

    private final long startTimeNanos;

    public Task(long id, String type, String action, String description, TaskId parentTask) {
        this(id, type, action, description, parentTask, System.currentTimeMillis(), System.nanoTime());
    }

    public Task(long id, String type, String action, String description, TaskId parentTask, long startTime, long startTimeNanos) {
        this.id = id;
        this.type = type;
        this.action = action;
        this.description = description;
        this.parentTask = parentTask;
        this.startTime = startTime;
        this.startTimeNanos = startTimeNanos;
    }

    /**
     * Build a version of the task status you can throw over the wire and back
     * to the user.
     *
     * @param node
     *            the node this task is running on
     * @param detailed
     *            should the information include detailed, potentially slow to
     *            generate data?
     */
    public TaskInfo taskInfo(DiscoveryNode node, boolean detailed) {
        String description = null;
        Task.Status status = null;
        if (detailed) {
            description = getDescription();
            status = getStatus();
        }
        return new TaskInfo(new TaskId(node.getId(), getId()), getType(), getAction(), description, status, startTime,
                System.nanoTime() - startTimeNanos, this instanceof CancellableTask, parentTask);
    }

    /**
     * Returns task id
     */
    public long getId() {
        return id;
    }

    /**
     * Returns task channel type (netty, transport, direct)
     */
    public String getType() {
        return type;
    }

    /**
     * Returns task action
     */
    public String getAction() {
        return action;
    }

    /**
     * Generates task description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Returns the task start time
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Returns id of the parent task or NO_PARENT_ID if the task doesn't have any parent tasks
     */
    public TaskId getParentTaskId() {
        return parentTask;
    }

    /**
     * Build a status for this task or null if this task doesn't have status.
     * Since most tasks don't have status this defaults to returning null. While
     * this can never perform IO it might be a costly operation, requiring
     * collating lists of results, etc. So only use it if you need the value.
     */
    public Status getStatus() {
        return null;
    }

    public interface Status extends ToXContent, NamedWriteable {}

    public PersistedTaskInfo result(DiscoveryNode node, Throwable error) throws IOException {
        return new PersistedTaskInfo(taskInfo(node, true), error);
    }

    public PersistedTaskInfo result(DiscoveryNode node, ActionResponse response) throws IOException {
        if (response instanceof ToXContent) {
            return new PersistedTaskInfo(taskInfo(node, true), (ToXContent) response);
        } else {
            throw new IllegalStateException("response has to implement ToXContent for persistence");
        }
    }
}
