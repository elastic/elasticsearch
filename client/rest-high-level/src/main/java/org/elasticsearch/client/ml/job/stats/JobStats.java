/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.stats;

import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.client.ml.job.config.JobState;
import org.elasticsearch.client.ml.job.process.DataCounts;
import org.elasticsearch.client.ml.job.process.ModelSizeStats;
import org.elasticsearch.client.ml.job.process.TimingStats;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.client.ml.NodeAttributes;

import java.io.IOException;
import java.util.Objects;

/**
 * Class containing the statistics for a Machine Learning job.
 *
 */
public class JobStats implements ToXContentObject {

    private static final ParseField DATA_COUNTS = new ParseField("data_counts");
    private static final ParseField MODEL_SIZE_STATS = new ParseField("model_size_stats");
    private static final ParseField TIMING_STATS = new ParseField("timing_stats");
    private static final ParseField FORECASTS_STATS = new ParseField("forecasts_stats");
    private static final ParseField STATE = new ParseField("state");
    private static final ParseField NODE = new ParseField("node");
    private static final ParseField OPEN_TIME = new ParseField("open_time");
    private static final ParseField ASSIGNMENT_EXPLANATION = new ParseField("assignment_explanation");

    public static final ConstructingObjectParser<JobStats, Void> PARSER =
        new ConstructingObjectParser<>("job_stats",
            true,
            (a) -> {
                int i = 0;
                String jobId = (String) a[i++];
                DataCounts dataCounts = (DataCounts) a[i++];
                JobState jobState = (JobState) a[i++];
                ModelSizeStats.Builder modelSizeStatsBuilder = (ModelSizeStats.Builder) a[i++];
                ModelSizeStats modelSizeStats = modelSizeStatsBuilder == null ? null : modelSizeStatsBuilder.build();
                TimingStats timingStats = (TimingStats) a[i++];
                ForecastStats forecastStats = (ForecastStats) a[i++];
                NodeAttributes node = (NodeAttributes) a[i++];
                String assignmentExplanation = (String) a[i++];
                TimeValue openTime = (TimeValue) a[i];
                return new JobStats(jobId,
                    dataCounts,
                    jobState,
                    modelSizeStats,
                    timingStats,
                    forecastStats,
                    node,
                    assignmentExplanation,
                    openTime);
            });

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), DataCounts.PARSER, DATA_COUNTS);
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            (p) -> JobState.fromString(p.text()),
            STATE,
            ObjectParser.ValueType.VALUE);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ModelSizeStats.PARSER, MODEL_SIZE_STATS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), TimingStats.PARSER, TIMING_STATS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ForecastStats.PARSER, FORECASTS_STATS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), NodeAttributes.PARSER, NODE);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ASSIGNMENT_EXPLANATION);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.textOrNull(), OPEN_TIME.getPreferredName()),
            OPEN_TIME,
            ObjectParser.ValueType.STRING_OR_NULL);
    }


    private final String jobId;
    private final DataCounts dataCounts;
    private final JobState state;
    private final ModelSizeStats modelSizeStats;
    private final TimingStats timingStats;
    private final ForecastStats forecastStats;
    private final NodeAttributes node;
    private final String assignmentExplanation;
    private final TimeValue openTime;

    JobStats(String jobId, DataCounts dataCounts, JobState state, @Nullable ModelSizeStats modelSizeStats,
             @Nullable TimingStats timingStats, @Nullable ForecastStats forecastStats, @Nullable NodeAttributes node,
             @Nullable String assignmentExplanation, @Nullable TimeValue openTime) {
        this.jobId = Objects.requireNonNull(jobId);
        this.dataCounts = Objects.requireNonNull(dataCounts);
        this.state = Objects.requireNonNull(state);
        this.modelSizeStats = modelSizeStats;
        this.timingStats = timingStats;
        this.forecastStats = forecastStats;
        this.node = node;
        this.assignmentExplanation = assignmentExplanation;
        this.openTime = openTime;
    }

    /**
     * The jobId referencing the job for these statistics
     */
    public String getJobId() {
        return jobId;
    }

    /**
     * An object that describes the number of records processed and any related error counts
     * See {@link DataCounts}
     */
    public DataCounts getDataCounts() {
        return dataCounts;
    }

    /**
     * An object that provides information about the size and contents of the model.
     * See {@link ModelSizeStats}
     */
    public ModelSizeStats getModelSizeStats() {
        return modelSizeStats;
    }

    public TimingStats getTimingStats() {
        return timingStats;
    }

    /**
     * An object that provides statistical information about forecasts of this job.
     * See {@link ForecastStats}
     */
    public ForecastStats getForecastStats() {
        return forecastStats;
    }

    /**
     * The status of the job
     * See {@link JobState}
     */
    public JobState getState() {
        return state;
    }

    /**
     * For open jobs only, contains information about the node where the job runs
     * See {@link NodeAttributes}
     */
    public NodeAttributes getNode() {
        return node;
    }

    /**
     * For open jobs only, contains messages relating to the selection of a node to run the job.
     */
    public String getAssignmentExplanation() {
        return assignmentExplanation;
    }

    /**
     * For open jobs only, the elapsed time for which the job has been open
     */
    public TimeValue getOpenTime() {
        return openTime;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(DATA_COUNTS.getPreferredName(), dataCounts);
        builder.field(STATE.getPreferredName(), state.toString());
        if (modelSizeStats != null) {
            builder.field(MODEL_SIZE_STATS.getPreferredName(), modelSizeStats);
        }
        if (timingStats != null) {
            builder.field(TIMING_STATS.getPreferredName(), timingStats);
        }
        if (forecastStats != null) {
            builder.field(FORECASTS_STATS.getPreferredName(), forecastStats);
        }
        if (node != null) {
            builder.field(NODE.getPreferredName(), node);
        }
        if (assignmentExplanation != null) {
            builder.field(ASSIGNMENT_EXPLANATION.getPreferredName(), assignmentExplanation);
        }
        if (openTime != null) {
            builder.field(OPEN_TIME.getPreferredName(), openTime.getStringRep());
        }
        return builder.endObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, dataCounts, modelSizeStats, timingStats, forecastStats, state, node, assignmentExplanation, openTime);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        JobStats other = (JobStats) obj;
        return Objects.equals(jobId, other.jobId) &&
            Objects.equals(this.dataCounts, other.dataCounts) &&
            Objects.equals(this.modelSizeStats, other.modelSizeStats) &&
            Objects.equals(this.timingStats, other.timingStats) &&
            Objects.equals(this.forecastStats, other.forecastStats) &&
            Objects.equals(this.state, other.state) &&
            Objects.equals(this.node, other.node) &&
            Objects.equals(this.assignmentExplanation, other.assignmentExplanation) &&
            Objects.equals(this.openTime, other.openTime);
    }
}
