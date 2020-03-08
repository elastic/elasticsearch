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
package org.elasticsearch.client.tasks;

import org.elasticsearch.common.ParseField;
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
