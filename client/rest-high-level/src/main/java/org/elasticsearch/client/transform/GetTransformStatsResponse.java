/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.client.transform.transforms.TransformStats;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class GetTransformStatsResponse {

    public static final ParseField TRANSFORMS = new ParseField("transforms");
    public static final ParseField COUNT = new ParseField("count");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<GetTransformStatsResponse, Void> PARSER = new ConstructingObjectParser<>(
        "get_transform_stats_response",
        true,
        args -> new GetTransformStatsResponse(
            (List<TransformStats>) args[0],
            (long) args[1],
            (List<TaskOperationFailure>) args[2],
            (List<ElasticsearchException>) args[3]
        )
    );

    static {
        PARSER.declareObjectArray(constructorArg(), TransformStats.PARSER::apply, TRANSFORMS);
        PARSER.declareLong(constructorArg(), COUNT);
        PARSER.declareObjectArray(
            optionalConstructorArg(),
            (p, c) -> TaskOperationFailure.fromXContent(p),
            AcknowledgedTasksResponse.TASK_FAILURES
        );
        PARSER.declareObjectArray(
            optionalConstructorArg(),
            (p, c) -> ElasticsearchException.fromXContent(p),
            AcknowledgedTasksResponse.NODE_FAILURES
        );
    }

    public static GetTransformStatsResponse fromXContent(final XContentParser parser) {
        return GetTransformStatsResponse.PARSER.apply(parser, null);
    }

    private final List<TransformStats> transformsStats;
    private final long count;
    private final List<TaskOperationFailure> taskFailures;
    private final List<ElasticsearchException> nodeFailures;

    public GetTransformStatsResponse(
        List<TransformStats> transformsStats,
        long count,
        @Nullable List<TaskOperationFailure> taskFailures,
        @Nullable List<? extends ElasticsearchException> nodeFailures
    ) {
        this.transformsStats = transformsStats;
        this.count = count;
        this.taskFailures = taskFailures == null ? Collections.emptyList() : Collections.unmodifiableList(taskFailures);
        this.nodeFailures = nodeFailures == null ? Collections.emptyList() : Collections.unmodifiableList(nodeFailures);
    }

    public List<TransformStats> getTransformsStats() {
        return transformsStats;
    }

    public long getCount() {
        return count;
    }

    public List<ElasticsearchException> getNodeFailures() {
        return nodeFailures;
    }

    public List<TaskOperationFailure> getTaskFailures() {
        return taskFailures;
    }

    @Override
    public int hashCode() {
        return Objects.hash(transformsStats, count, nodeFailures, taskFailures);
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
            && Objects.equals(this.count, that.count)
            && Objects.equals(this.nodeFailures, that.nodeFailures)
            && Objects.equals(this.taskFailures, that.taskFailures);
    }
}
