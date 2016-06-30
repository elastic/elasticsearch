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

package org.elasticsearch.action.admin.cluster.node.tasks.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.PersistedTaskInfo;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * Returns the list of tasks currently running on the nodes
 */
public class GetTaskResponse extends ActionResponse implements ToXContent {
    private PersistedTaskInfo task;

    public GetTaskResponse() {
    }

    public GetTaskResponse(PersistedTaskInfo task) {
        this.task = requireNonNull(task, "task is required");
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        task = in.readOptionalWriteable(PersistedTaskInfo::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(task);
    }

    /**
     * Get the actual result of the fetch.
     */
    public PersistedTaskInfo getTask() {
        return task;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return task.innerToXContent(builder, params);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
