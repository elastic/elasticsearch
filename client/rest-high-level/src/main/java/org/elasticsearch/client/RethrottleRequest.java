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

package org.elasticsearch.client;

import org.elasticsearch.tasks.TaskId;

import java.util.Objects;

/**
 * A request changing throttling of a task.
 */
public class RethrottleRequest implements Validatable {

    static final String REQUEST_PER_SECOND_PARAMETER = "requests_per_second";

    private final TaskId taskId;
    private final float requestsPerSecond;

    /**
     * Create a new {@link RethrottleRequest} which disables any throttling for the given taskId.
     * @param taskId the task for which throttling will be disabled
     */
    public RethrottleRequest(TaskId taskId) {
        this.taskId = taskId;
        this.requestsPerSecond = Float.POSITIVE_INFINITY;
    }

    /**
     * Create a new {@link RethrottleRequest} which changes the throttling for the given taskId.
     * @param taskId the task that throttling changes will be applied to
     * @param requestsPerSecond the number of requests per second that the task should perform. This needs to be a positive value.
     */
    public RethrottleRequest(TaskId taskId, float requestsPerSecond) {
        Objects.requireNonNull(taskId, "taskId cannot be null");
        if (requestsPerSecond <= 0) {
            throw new IllegalArgumentException("requestsPerSecond needs to be positive value but was [" + requestsPerSecond+"]");
        }
        this.taskId = taskId;
        this.requestsPerSecond = requestsPerSecond;
    }

    /**
     * @return the task Id
     */
    public TaskId getTaskId() {
        return taskId;
    }

    /**
     * @return the requests per seconds value
     */
    public float getRequestsPerSecond() {
        return requestsPerSecond;
    }

    @Override
    public String toString() {
        return "RethrottleRequest: taskID = " + taskId +"; reqestsPerSecond = " + requestsPerSecond;
    }
}