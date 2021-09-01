/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.tasks;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * client side counterpart of server side
 * {@link org.elasticsearch.action.TaskOperationFailure}
 */
public class TaskOperationFailure {

    private final String nodeId;
    private final long taskId;
    private final ElasticsearchException reason;
    private final String status;

    public TaskOperationFailure(String nodeId, long taskId,String status, ElasticsearchException reason) {
        this.nodeId = nodeId;
        this.taskId = taskId;
        this.status = status;
        this.reason = reason;
    }

    public String getNodeId() {
        return nodeId;
    }

    public long getTaskId() {
        return taskId;
    }

    public ElasticsearchException getReason() {
        return reason;
    }

    public String getStatus() {
        return status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof TaskOperationFailure) == false) return false;
        TaskOperationFailure that = (TaskOperationFailure) o;
        return getTaskId() == that.getTaskId() &&
            Objects.equals(getNodeId(), that.getNodeId()) &&
            Objects.equals(getReason(), that.getReason()) &&
            Objects.equals(getStatus(), that.getStatus());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNodeId(), getTaskId(), getReason(), getStatus());
    }
    @Override
    public String toString() {
        return "TaskOperationFailure{" +
            "nodeId='" + nodeId + '\'' +
            ", taskId=" + taskId +
            ", reason=" + reason +
            ", status='" + status + '\'' +
            '}';
    }
    public static TaskOperationFailure fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private static final ConstructingObjectParser<TaskOperationFailure, Void> PARSER =
        new ConstructingObjectParser<>("task_info", true, constructorObjects -> {
            int i = 0;
            String nodeId = (String) constructorObjects[i++];
            long taskId = (long) constructorObjects[i++];
            String status = (String) constructorObjects[i++];
            ElasticsearchException reason = (ElasticsearchException) constructorObjects[i];
            return new TaskOperationFailure(nodeId, taskId, status, reason);
        });

    static {
        PARSER.declareString(constructorArg(), new ParseField("node_id"));
        PARSER.declareLong(constructorArg(), new ParseField("task_id"));
        PARSER.declareString(constructorArg(), new ParseField("status"));
        PARSER.declareObject(constructorArg(), (parser, c) -> ElasticsearchException.fromXContent(parser), new ParseField("reason"));
    }
}
