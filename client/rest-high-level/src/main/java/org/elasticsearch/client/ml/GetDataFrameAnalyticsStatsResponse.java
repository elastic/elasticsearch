/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsStats;
import org.elasticsearch.client.transform.AcknowledgedTasksResponse;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class GetDataFrameAnalyticsStatsResponse {

    public static GetDataFrameAnalyticsStatsResponse fromXContent(XContentParser parser) {
        return GetDataFrameAnalyticsStatsResponse.PARSER.apply(parser, null);
    }

    private static final ParseField DATA_FRAME_ANALYTICS = new ParseField("data_frame_analytics");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GetDataFrameAnalyticsStatsResponse, Void> PARSER =
        new ConstructingObjectParser<>(
            "get_data_frame_analytics_stats_response", true,
            args -> new GetDataFrameAnalyticsStatsResponse(
                (List<DataFrameAnalyticsStats>) args[0],
                (List<TaskOperationFailure>) args[1],
                (List<ElasticsearchException>) args[2]));

    static {
        PARSER.declareObjectArray(constructorArg(), (p, c) -> DataFrameAnalyticsStats.fromXContent(p), DATA_FRAME_ANALYTICS);
        PARSER.declareObjectArray(
            optionalConstructorArg(), (p, c) -> TaskOperationFailure.fromXContent(p), AcknowledgedTasksResponse.TASK_FAILURES);
        PARSER.declareObjectArray(
            optionalConstructorArg(), (p, c) -> ElasticsearchException.fromXContent(p), AcknowledgedTasksResponse.NODE_FAILURES);
    }

    private final List<DataFrameAnalyticsStats> analyticsStats;
    private final List<TaskOperationFailure> taskFailures;
    private final List<ElasticsearchException> nodeFailures;

    public GetDataFrameAnalyticsStatsResponse(List<DataFrameAnalyticsStats> analyticsStats,
                                              @Nullable List<TaskOperationFailure> taskFailures,
                                              @Nullable List<? extends ElasticsearchException> nodeFailures) {
        this.analyticsStats = analyticsStats;
        this.taskFailures = taskFailures == null ? Collections.emptyList() : Collections.unmodifiableList(taskFailures);
        this.nodeFailures = nodeFailures == null ? Collections.emptyList() : Collections.unmodifiableList(nodeFailures);
    }

    public List<DataFrameAnalyticsStats> getAnalyticsStats() {
        return analyticsStats;
    }

    public List<ElasticsearchException> getNodeFailures() {
        return nodeFailures;
    }

    public List<TaskOperationFailure> getTaskFailures() {
        return taskFailures;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GetDataFrameAnalyticsStatsResponse other = (GetDataFrameAnalyticsStatsResponse) o;
        return Objects.equals(analyticsStats, other.analyticsStats)
            && Objects.equals(nodeFailures, other.nodeFailures)
            && Objects.equals(taskFailures, other.taskFailures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(analyticsStats, nodeFailures, taskFailures);
    }
}
