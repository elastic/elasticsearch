/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

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

    private static final ConstructingObjectParser<TaskOperationFailure, Void> PARSER = new ConstructingObjectParser<>(
        "task_info",
        true,
        constructorObjects -> {
            int i = 0;
            String nodeId = (String) constructorObjects[i++];
            long taskId = (long) constructorObjects[i++];
            ElasticsearchException reason = (ElasticsearchException) constructorObjects[i];
            return new TaskOperationFailure(nodeId, taskId, reason);
        }
    );

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
