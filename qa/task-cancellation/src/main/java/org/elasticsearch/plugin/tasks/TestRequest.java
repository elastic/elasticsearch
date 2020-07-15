/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugin.tasks;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

final class TestRequest extends ActionRequest {
    final String id;
    final Set<Target> targets;

    TestRequest(String id, Set<Target> targets) {
        this.id = id;
        this.targets = targets;
    }

    TestRequest(StreamInput in) throws IOException {
        super(in);
        this.id = in.readString();
        this.targets = in.readSet(Target::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeCollection(targets);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public Task createTask(long taskId, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        final String description = "id=" + id;
        return new BlockingCancellableTask(taskId, type, action, description, parentTaskId, headers) {

        };
    }

    @Override
    public String toString() {
        return "id:" + id + ", targets:" + targets;
    }

    static final class Target implements Writeable {
        final String clusterAlias;
        final String nodeId;

        Target(StreamInput in) throws IOException {
            clusterAlias = in.readString();
            nodeId = in.readString();
        }

        Target(String clusterAlias, String nodeId) {
            this.clusterAlias = clusterAlias;
            this.nodeId = nodeId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(clusterAlias);
            out.writeString(nodeId);
        }

        @Override
        public String toString() {
            return clusterAlias + ":" + nodeId;
        }
    }
}
