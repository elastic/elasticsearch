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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Information about task operation failures
 *
 * The class is final due to serialization limitations
 */
public final class TaskOperationFailure implements Writeable, ToXContentFragment {
    private static final String TASK_ID = "task_id";
    private static final String NODE_ID = "node_id";
    private static final String STATUS = "status";
    private static final String REASON = "reason";
    private final String nodeId;

    private final long taskId;

    private final Exception reason;

    private final RestStatus status;

    private static final ConstructingObjectParser<TaskOperationFailure, Void> PARSER =
            new ConstructingObjectParser<>("task_info", true, constructorObjects -> {
                int i = 0;
                String nodeId = (String) constructorObjects[i++];
                long taskId = (long) constructorObjects[i++];
                ElasticsearchException reason = (ElasticsearchException) constructorObjects[i];
                return new TaskOperationFailure(nodeId, taskId, reason);
            });

    static {
        PARSER.declareString(constructorArg(), new ParseField(NODE_ID));
        PARSER.declareLong(constructorArg(), new ParseField(TASK_ID));
        PARSER.declareObject(constructorArg(), (parser, c) -> ElasticsearchException.fromXContent(parser), new ParseField(REASON));
    }

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

    public RestStatus getStatus() {
        return status;
    }

    public Exception getCause() {
        return reason;
    }

    @Override
    public String toString() {
        return "[" + nodeId + "][" + taskId + "] failed, reason [" + reason + "]";
    }

    public static TaskOperationFailure fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(TASK_ID, getTaskId());
        builder.field(NODE_ID, getNodeId());
        builder.field(STATUS, status.name());
        if (reason != null) {
            builder.field(REASON);
            builder.startObject();
            ElasticsearchException.generateThrowableXContent(builder, params, reason);
            builder.endObject();
        }
        return builder;

    }
}
