/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.tasks;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.ExceptionsHelper.rethrowAndSuppress;

/**
 * Base class for responses of task-related operations
 */
public class BaseTasksResponse extends ActionResponse {
    protected static final String TASK_FAILURES = "task_failures";
    protected static final String NODE_FAILURES = "node_failures";

    private List<TaskOperationFailure> taskFailures;
    private List<ElasticsearchException> nodeFailures;

    public BaseTasksResponse(List<TaskOperationFailure> taskFailures, List<? extends ElasticsearchException> nodeFailures) {
        this.taskFailures = taskFailures == null ? Collections.emptyList() : List.copyOf(taskFailures);
        this.nodeFailures = nodeFailures == null ? Collections.emptyList() : List.copyOf(nodeFailures);
    }

    public BaseTasksResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        List<TaskOperationFailure> taskFailures = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            taskFailures.add(new TaskOperationFailure(in));
        }
        size = in.readVInt();
        this.taskFailures = Collections.unmodifiableList(taskFailures);
        List<FailedNodeException> nodeFailures = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            nodeFailures.add(new FailedNodeException(in));
        }
        this.nodeFailures = Collections.unmodifiableList(nodeFailures);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(taskFailures.size());
        for (TaskOperationFailure exp : taskFailures) {
            exp.writeTo(out);
        }
        out.writeVInt(nodeFailures.size());
        for (ElasticsearchException exp : nodeFailures) {
            exp.writeTo(out);
        }
    }

    /**
     * The list of task failures exception.
     */
    public List<TaskOperationFailure> getTaskFailures() {
        return taskFailures;
    }

    /**
     * The list of node failures exception.
     */
    public List<ElasticsearchException> getNodeFailures() {
        return nodeFailures;
    }

    /**
     * Rethrow task failures if there are any.
     */
    public void rethrowFailures(String operationName) {
        rethrowAndSuppress(
            Stream.concat(
                getNodeFailures().stream(),
                getTaskFailures().stream()
                    .map(
                        f -> new ElasticsearchException(
                            "{} of [{}] failed",
                            f.getCause(),
                            operationName,
                            new TaskId(f.getNodeId(), f.getTaskId())
                        )
                    )
            ).collect(toList())
        );
    }

    protected void toXContentCommon(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (getTaskFailures() != null && getTaskFailures().size() > 0) {
            builder.startArray(TASK_FAILURES);
            for (TaskOperationFailure ex : getTaskFailures()) {
                builder.startObject();
                builder.value(ex);
                builder.endObject();
            }
            builder.endArray();
        }

        if (getNodeFailures() != null && getNodeFailures().size() > 0) {
            builder.startArray(NODE_FAILURES);
            for (ElasticsearchException ex : getNodeFailures()) {
                builder.startObject();
                ex.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BaseTasksResponse response = (BaseTasksResponse) o;
        return taskFailures.equals(response.taskFailures) && nodeFailures.equals(response.nodeFailures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskFailures, nodeFailures);
    }
}
