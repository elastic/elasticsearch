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

package org.elasticsearch.action.support.replication;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * Task that tracks replication actions.
 */
public class ReplicationTask extends Task {
    private volatile String phase = "starting";

    public ReplicationTask(long id, String type, String action, String description, TaskId parentTaskId) {
        super(id, type, action, description, parentTaskId);
    }

    /**
     * Set the current phase of the task.
     */
    public void setPhase(String phase) {
        this.phase = phase;
    }

    /**
     * Get the current phase of the task.
     */
    public String getPhase() {
        return phase;
    }

    @Override
    public Status getStatus() {
        return new Status(phase);
    }

    public static class Status implements Task.Status {
        public static final String NAME = "replication";

        private final String phase;

        public Status(String phase) {
            this.phase = requireNonNull(phase, "Phase cannot be null");
        }

        public Status(StreamInput in) throws IOException {
            phase = in.readString();
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("phase", phase);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(phase);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        // Implements equals and hashcode for testing
        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != ReplicationTask.Status.class) {
                return false;
            }
            ReplicationTask.Status other = (Status) obj;
            return phase.equals(other.phase);
        }

        @Override
        public int hashCode() {
            return phase.hashCode();
        }
    }
}
