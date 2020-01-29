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
package org.elasticsearch.client.tasks;

import java.util.Objects;

/**
 * client side version of a {@link org.elasticsearch.tasks.TaskId}
 */
public class TaskId {

    protected final String nodeId;
    protected final long id;

    public TaskId(String nodeId, long id) {
        this.nodeId = nodeId;
        this.id = id;
    }

    /**
     * accepts a raw format task id
     * @param taskId expected to be nodeid:taskId
     */
    public TaskId(String taskId) {
        if (taskId == null) {
            throw new IllegalArgumentException("null task id");
        }
        String[] s = taskId.split(":");
        if (s.length != 2) {
            throw new IllegalArgumentException("malformed task id " + taskId);
        }
        this.nodeId = s[0];
        try {
            this.id = Long.parseLong(s[1]);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("malformed task id " + taskId, ex);
        }
    }

    public String getNodeId() {
        return nodeId;
    }

    public long getId() {
        return id;
    }

    public boolean isSet() {
        return id != -1L;
    }

    @Override
    public String toString() {
        if (isSet()) {
            return nodeId + ":" + id;
        } else {
            return "unset";
        }
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TaskId)) return false;
        TaskId taskId = (TaskId) o;
        return getId() == taskId.getId() &&
            Objects.equals(getNodeId(), taskId.getNodeId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNodeId(), getId());
    }
}
