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

package org.elasticsearch.client.transform;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.client.transform.transforms.TransformStats;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class GetTransformStatsResponse {

    public static final ParseField TRANSFORMS = new ParseField("transforms");
    public static final ParseField COUNT = new ParseField("count");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<GetTransformStatsResponse, Void> PARSER = new ConstructingObjectParser<>(
            "get_transform_stats_response", true,
            args -> new GetTransformStatsResponse((List<TransformStats>) args[0],
                    (List<TaskOperationFailure>) args[1], (List<ElasticsearchException>) args[2]));

    static {
        PARSER.declareObjectArray(constructorArg(), TransformStats.PARSER::apply, TRANSFORMS);
        // Discard the count field which is the size of the transforms array
        PARSER.declareInt((a, b) -> {}, COUNT);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> TaskOperationFailure.fromXContent(p),
                AcknowledgedTasksResponse.TASK_FAILURES);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> ElasticsearchException.fromXContent(p),
                AcknowledgedTasksResponse.NODE_FAILURES);
    }

    public static GetTransformStatsResponse fromXContent(final XContentParser parser) {
        return GetTransformStatsResponse.PARSER.apply(parser, null);
    }

    private final List<TransformStats> transformsStats;
    private final List<TaskOperationFailure> taskFailures;
    private final List<ElasticsearchException> nodeFailures;

    public GetTransformStatsResponse(List<TransformStats> transformsStats,
                                              @Nullable List<TaskOperationFailure> taskFailures,
                                              @Nullable List<? extends ElasticsearchException> nodeFailures) {
        this.transformsStats = transformsStats;
        this.taskFailures = taskFailures == null ? Collections.emptyList() : Collections.unmodifiableList(taskFailures);
        this.nodeFailures = nodeFailures == null ? Collections.emptyList() : Collections.unmodifiableList(nodeFailures);
    }

    public List<TransformStats> getTransformsStats() {
        return transformsStats;
    }

    public List<ElasticsearchException> getNodeFailures() {
        return nodeFailures;
    }

    public List<TaskOperationFailure> getTaskFailures() {
        return taskFailures;
    }

    @Override
    public int hashCode() {
        return Objects.hash(transformsStats, nodeFailures, taskFailures);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final GetTransformStatsResponse that = (GetTransformStatsResponse) other;
        return Objects.equals(this.transformsStats, that.transformsStats)
                && Objects.equals(this.nodeFailures, that.nodeFailures)
                && Objects.equals(this.taskFailures, that.taskFailures);
    }
}
