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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Task id that consists of node id and id of the task on the node
 */
public final class TaskId implements Writeable<TaskId> {

    public final static TaskId EMPTY_TASK_ID = new TaskId("", -1L);

    private final String nodeId;
    private final long id;

    public TaskId(String nodeId, long id) {
        this.nodeId = nodeId;
        this.id = id;
    }

    public TaskId(String taskId) {
        if (Strings.hasLength(taskId) && "unset".equals(taskId) == false) {
            String[] s = Strings.split(taskId, ":");
            if (s == null || s.length != 2) {
                throw new IllegalArgumentException("malformed task id " + taskId);
            }
            this.nodeId = s[0];
            try {
                this.id = Long.parseLong(s[1]);
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("malformed task id " + taskId, ex);
            }
        } else {
            nodeId = "";
            id = -1L;
        }
    }

    public TaskId(StreamInput in) throws IOException {
        nodeId = in.readString();
        id = in.readLong();
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
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        out.writeLong(id);
    }

    @Override
    public TaskId readFrom(StreamInput in) throws IOException {
        return new TaskId(in);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaskId taskId = (TaskId) o;

        if (id != taskId.id) return false;
        return nodeId.equals(taskId.nodeId);

    }

    @Override
    public int hashCode() {
        int result = nodeId.hashCode();
        result = 31 * result + (int) (id ^ (id >>> 32));
        return result;
    }
}
