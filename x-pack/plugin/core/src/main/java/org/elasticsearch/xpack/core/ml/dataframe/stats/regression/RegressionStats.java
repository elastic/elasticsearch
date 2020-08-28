/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.regression;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.dataframe.stats.AnalysisStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.Fields;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

public class RegressionStats implements AnalysisStats {

    public static final String TYPE_VALUE = "regression_stats";

    public static final ParseField ITERATION = new ParseField("iteration");
    public static final ParseField HYPERPARAMETERS = new ParseField("hyperparameters");
    public static final ParseField TIMING_STATS = new ParseField("timing_stats");
    public static final ParseField VALIDATION_LOSS = new ParseField("validation_loss");

    public static final ConstructingObjectParser<RegressionStats, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<RegressionStats, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<RegressionStats, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<RegressionStats, Void> parser = new ConstructingObjectParser<>(TYPE_VALUE, ignoreUnknownFields,
            a -> new RegressionStats(
                (String) a[0],
                (Instant) a[1],
                (int) a[2],
                (Hyperparameters) a[3],
                (TimingStats) a[4],
                (ValidationLoss) a[5]
            )
        );

        parser.declareString((bucket, s) -> {}, Fields.TYPE);
        parser.declareString(ConstructingObjectParser.constructorArg(), Fields.JOB_ID);
        parser.declareField(ConstructingObjectParser.constructorArg(),
            p -> TimeUtils.parseTimeFieldToInstant(p, Fields.TIMESTAMP.getPreferredName()),
            Fields.TIMESTAMP,
            ObjectParser.ValueType.VALUE);
        parser.declareInt(ConstructingObjectParser.constructorArg(), ITERATION);
        parser.declareObject(ConstructingObjectParser.constructorArg(),
            (p, c) -> Hyperparameters.fromXContent(p, ignoreUnknownFields), HYPERPARAMETERS);
        parser.declareObject(ConstructingObjectParser.constructorArg(),
            (p, c) -> TimingStats.fromXContent(p, ignoreUnknownFields), TIMING_STATS);
        parser.declareObject(ConstructingObjectParser.constructorArg(),
            (p, c) -> ValidationLoss.fromXContent(p, ignoreUnknownFields), VALIDATION_LOSS);
        return parser;
    }

    private final String jobId;
    private final Instant timestamp;
    private final int iteration;
    private final Hyperparameters hyperparameters;
    private final TimingStats timingStats;
    private final ValidationLoss validationLoss;

    public RegressionStats(String jobId, Instant timestamp, int iteration, Hyperparameters hyperparameters, TimingStats timingStats,
                           ValidationLoss validationLoss) {
        this.jobId = Objects.requireNonNull(jobId);
        // We intend to store this timestamp in millis granularity. Thus we're rounding here to ensure
        // internal representation matches toXContent
        this.timestamp = Instant.ofEpochMilli(ExceptionsHelper.requireNonNull(timestamp, Fields.TIMESTAMP).toEpochMilli());
        this.iteration = iteration;
        this.hyperparameters = Objects.requireNonNull(hyperparameters);
        this.timingStats = Objects.requireNonNull(timingStats);
        this.validationLoss = Objects.requireNonNull(validationLoss);
    }

    public RegressionStats(StreamInput in) throws IOException {
        this.jobId = in.readString();
        this.timestamp = in.readInstant();
        this.iteration = in.readVInt();
        this.hyperparameters = new Hyperparameters(in);
        this.timingStats = new TimingStats(in);
        this.validationLoss = new ValidationLoss(in);
    }

    @Override
    public String getWriteableName() {
        return TYPE_VALUE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeInstant(timestamp);
        out.writeVInt(iteration);
        hyperparameters.writeTo(out);
        timingStats.writeTo(out);
        validationLoss.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
            builder.field(Fields.TYPE.getPreferredName(), TYPE_VALUE);
            builder.field(Fields.JOB_ID.getPreferredName(), jobId);
        }
        builder.timeField(Fields.TIMESTAMP.getPreferredName(), Fields.TIMESTAMP.getPreferredName() + "_string", timestamp.toEpochMilli());
        builder.field(ITERATION.getPreferredName(), iteration);
        builder.field(HYPERPARAMETERS.getPreferredName(), hyperparameters);
        builder.field(TIMING_STATS.getPreferredName(), timingStats);
        builder.field(VALIDATION_LOSS.getPreferredName(), validationLoss, params);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegressionStats that = (RegressionStats) o;
        return Objects.equals(jobId, that.jobId)
            && Objects.equals(timestamp, that.timestamp)
            && iteration == that.iteration
            && Objects.equals(hyperparameters, that.hyperparameters)
            && Objects.equals(timingStats, that.timingStats)
            && Objects.equals(validationLoss, that.validationLoss);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, timestamp, iteration, hyperparameters, timingStats, validationLoss);
    }

    public String documentId(String jobId) {
        return documentIdPrefix(jobId) + timestamp.toEpochMilli();
    }

    public static String documentIdPrefix(String jobId) {
        return TYPE_VALUE + "_" + jobId + "_";
    }
}
