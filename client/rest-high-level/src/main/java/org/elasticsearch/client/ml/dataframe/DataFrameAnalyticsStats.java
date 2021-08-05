/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.client.ml.NodeAttributes;
import org.elasticsearch.client.ml.dataframe.stats.AnalysisStats;
import org.elasticsearch.client.ml.dataframe.stats.common.DataCounts;
import org.elasticsearch.client.ml.dataframe.stats.common.MemoryUsage;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.inject.internal.ToStringBuilder;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DataFrameAnalyticsStats {

    public static DataFrameAnalyticsStats fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    static final ParseField ID = new ParseField("id");
    static final ParseField STATE = new ParseField("state");
    static final ParseField FAILURE_REASON = new ParseField("failure_reason");
    static final ParseField PROGRESS = new ParseField("progress");
    static final ParseField DATA_COUNTS = new ParseField("data_counts");
    static final ParseField MEMORY_USAGE = new ParseField("memory_usage");
    static final ParseField ANALYSIS_STATS = new ParseField("analysis_stats");
    static final ParseField NODE = new ParseField("node");
    static final ParseField ASSIGNMENT_EXPLANATION = new ParseField("assignment_explanation");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DataFrameAnalyticsStats, Void> PARSER =
        new ConstructingObjectParser<>("data_frame_analytics_stats", true,
            args -> new DataFrameAnalyticsStats(
                (String) args[0],
                (DataFrameAnalyticsState) args[1],
                (String) args[2],
                (List<PhaseProgress>) args[3],
                (DataCounts) args[4],
                (MemoryUsage) args[5],
                (AnalysisStats) args[6],
                (NodeAttributes) args[7],
                (String) args[8]));

    static {
        PARSER.declareString(constructorArg(), ID);
        PARSER.declareString(constructorArg(), DataFrameAnalyticsState::fromString, STATE);
        PARSER.declareString(optionalConstructorArg(), FAILURE_REASON);
        PARSER.declareObjectArray(optionalConstructorArg(), PhaseProgress.PARSER, PROGRESS);
        PARSER.declareObject(optionalConstructorArg(), DataCounts.PARSER, DATA_COUNTS);
        PARSER.declareObject(optionalConstructorArg(), MemoryUsage.PARSER, MEMORY_USAGE);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> parseAnalysisStats(p), ANALYSIS_STATS);
        PARSER.declareObject(optionalConstructorArg(), NodeAttributes.PARSER, NODE);
        PARSER.declareString(optionalConstructorArg(), ASSIGNMENT_EXPLANATION);
    }

    private static AnalysisStats parseAnalysisStats(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
        AnalysisStats analysisStats = parser.namedObject(AnalysisStats.class, parser.currentName(), true);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return analysisStats;
    }

    private final String id;
    private final DataFrameAnalyticsState state;
    private final String failureReason;
    private final List<PhaseProgress> progress;
    private final DataCounts dataCounts;
    private final MemoryUsage memoryUsage;
    private final AnalysisStats analysisStats;
    private final NodeAttributes node;
    private final String assignmentExplanation;

    public DataFrameAnalyticsStats(String id, DataFrameAnalyticsState state, @Nullable String failureReason,
                                   @Nullable List<PhaseProgress> progress, @Nullable DataCounts dataCounts,
                                   @Nullable MemoryUsage memoryUsage, @Nullable AnalysisStats analysisStats, @Nullable NodeAttributes node,
                                   @Nullable String assignmentExplanation) {
        this.id = id;
        this.state = state;
        this.failureReason = failureReason;
        this.progress = progress;
        this.dataCounts = dataCounts;
        this.memoryUsage = memoryUsage;
        this.analysisStats = analysisStats;
        this.node = node;
        this.assignmentExplanation = assignmentExplanation;
    }

    public String getId() {
        return id;
    }

    public DataFrameAnalyticsState getState() {
        return state;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public List<PhaseProgress> getProgress() {
        return progress;
    }

    @Nullable
    public DataCounts getDataCounts() {
        return dataCounts;
    }

    @Nullable
    public MemoryUsage getMemoryUsage() {
        return memoryUsage;
    }

    @Nullable
    public AnalysisStats getAnalysisStats() {
        return analysisStats;
    }

    public NodeAttributes getNode() {
        return node;
    }

    public String getAssignmentExplanation() {
        return assignmentExplanation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataFrameAnalyticsStats other = (DataFrameAnalyticsStats) o;
        return Objects.equals(id, other.id)
            && Objects.equals(state, other.state)
            && Objects.equals(failureReason, other.failureReason)
            && Objects.equals(progress, other.progress)
            && Objects.equals(dataCounts, other.dataCounts)
            && Objects.equals(memoryUsage, other.memoryUsage)
            && Objects.equals(analysisStats, other.analysisStats)
            && Objects.equals(node, other.node)
            && Objects.equals(assignmentExplanation, other.assignmentExplanation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, state, failureReason, progress, dataCounts, memoryUsage, analysisStats, node, assignmentExplanation);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(getClass())
            .add("id", id)
            .add("state", state)
            .add("failureReason", failureReason)
            .add("progress", progress)
            .add("dataCounts", dataCounts)
            .add("memoryUsage", memoryUsage)
            .add("analysisStats", analysisStats)
            .add("node", node)
            .add("assignmentExplanation", assignmentExplanation)
            .toString();
    }
}
