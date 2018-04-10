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

package org.elasticsearch.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static org.elasticsearch.ExceptionsHelper.detailedMessage;

/**
 * Information about task operation failures
 *
 * The class is final due to serialization limitations
 */
public final class TaskOperationFailure implements Writeable, ToXContentFragment {

    private final String nodeId;

    private final long taskId;

    private final Exception reason;

    private final RestStatus status;

    public TaskOperationFailure(String nodeId, long taskId, Exception e) {
        this.nodeId = nodeId;
        this.taskId = taskId;
        this.reason = e;
        status = ExceptionsHelper.status(e);
    }

    /**
     * Read from a stream.
     */
    public TaskOperationFailure(StreamInput in) throws IOException {
        nodeId = in.readString();
        taskId = in.readLong();
        reason = in.readException();
        status = RestStatus.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        out.writeLong(taskId);
        out.writeException(reason);
        RestStatus.writeTo(out, status);
    }

    public String getNodeId() {
        return this.nodeId;
    }

    public long getTaskId() {
        return this.taskId;
    }

    public String getReason() {
        return detailedMessage(reason);
    }

    public RestStatus getStatus() {
        return status;
    }

    public Exception getCause() {
        return reason;
    }

    @Override
    public String toString() {
        return "[" + nodeId + "][" + taskId + "] failed, reason [" + getReason() + "]";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("task_id", getTaskId());
        builder.field("node_id", getNodeId());
        builder.field("status", status.name());
        if (reason != null) {
            builder.field("reason");
            builder.startObject();
            ElasticsearchException.generateThrowableXContent(builder, params, reason);
            builder.endObject();
        }
        return builder;

    }

}
