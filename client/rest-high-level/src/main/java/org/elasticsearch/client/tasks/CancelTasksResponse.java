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

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * cancel tasks response that contains
 * - task failures
 * - node failures
 * - tasks
 */
public class CancelTasksResponse extends ListTasksResponse {

    CancelTasksResponse(List<NodeData> nodesInfoData,
                        List<TaskOperationFailure> taskFailures,
                        List<ElasticsearchException> nodeFailures) {
        super(nodesInfoData, taskFailures, nodeFailures);
    }

    public static CancelTasksResponse fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private static ConstructingObjectParser<CancelTasksResponse, Void> PARSER;

    static {
        ConstructingObjectParser<CancelTasksResponse, Void> parser = new ConstructingObjectParser<>("cancel_tasks_response", true,
            constructingObjects -> {
                int i = 0;
                @SuppressWarnings("unchecked")
                List<TaskOperationFailure> tasksFailures = (List<TaskOperationFailure>) constructingObjects[i++];
                @SuppressWarnings("unchecked")
                List<ElasticsearchException> nodeFailures = (List<ElasticsearchException>) constructingObjects[i++];
                @SuppressWarnings("unchecked")
                List<NodeData> nodesInfoData = (List<NodeData>) constructingObjects[i];
                return new CancelTasksResponse(nodesInfoData, tasksFailures, nodeFailures);
            });

        parser.declareObjectArray(optionalConstructorArg(), (p, c) ->
            TaskOperationFailure.fromXContent(p), new ParseField("task_failures"));
        parser.declareObjectArray(optionalConstructorArg(), (p, c) ->
            ElasticsearchException.fromXContent(p), new ParseField("node_failures"));
        parser.declareNamedObjects(optionalConstructorArg(), NodeData.PARSER, new ParseField("nodes"));
        PARSER = parser;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return "CancelTasksResponse{" +
            "taskFailures=" + taskFailures +
            ", nodeFailures=" + nodeFailures +
            ", nodesInfoData=" + nodesInfoData +
            ", tasks=" + tasks +
            ", taskGroups=" + taskGroups +
            '}';
    }
}
