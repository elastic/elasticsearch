/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.outlierdetection;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.dataframe.stats.AnalysisStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.Fields;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

public class OutlierDetectionStats implements AnalysisStats {

    public static final String TYPE_VALUE = "outlier_detection_stats";

    public static final ParseField PARAMETERS = new ParseField("parameters");
    public static final ParseField TIMING_STATS = new ParseField("timing_stats");

    public static final ConstructingObjectParser<OutlierDetectionStats, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<OutlierDetectionStats, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<OutlierDetectionStats, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<OutlierDetectionStats, Void> parser = new ConstructingObjectParser<>(
            TYPE_VALUE,
            ignoreUnknownFields,
            a -> new OutlierDetectionStats((String) a[0], (Instant) a[1], (Parameters) a[2], (TimingStats) a[3])
        );

        parser.declareString((bucket, s) -> {}, Fields.TYPE);
        parser.declareString(ConstructingObjectParser.constructorArg(), Fields.JOB_ID);
        parser.declareField(
            ConstructingObjectParser.constructorArg(),
            p -> TimeUtils.parseTimeFieldToInstant(p, Fields.TIMESTAMP.getPreferredName()),
            Fields.TIMESTAMP,
            ObjectParser.ValueType.VALUE
        );
        parser.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> Parameters.fromXContent(p, ignoreUnknownFields),
            PARAMETERS
        );
        parser.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> TimingStats.fromXContent(p, ignoreUnknownFields),
            TIMING_STATS
        );
        return parser;
    }

    private final String jobId;
    private final Instant timestamp;
    private final Parameters parameters;
    private final TimingStats timingStats;

    public OutlierDetectionStats(String jobId, Instant timestamp, Parameters parameters, TimingStats timingStats) {
        this.jobId = Objects.requireNonNull(jobId);
        // We intend to store this timestamp in millis granularity. Thus we're rounding here to ensure
        // internal representation matches toXContent
        this.timestamp = Instant.ofEpochMilli(ExceptionsHelper.requireNonNull(timestamp, Fields.TIMESTAMP).toEpochMilli());
        this.parameters = Objects.requireNonNull(parameters);
        this.timingStats = Objects.requireNonNull(timingStats);
    }

    public OutlierDetectionStats(StreamInput in) throws IOException {
        this.jobId = in.readString();
        this.timestamp = in.readInstant();
        this.parameters = new Parameters(in);
        this.timingStats = new TimingStats(in);
    }

    @Override
    public String getWriteableName() {
        return TYPE_VALUE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeInstant(timestamp);
        parameters.writeTo(out);
        timingStats.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
            builder.field(Fields.TYPE.getPreferredName(), TYPE_VALUE);
            builder.field(Fields.JOB_ID.getPreferredName(), jobId);
        }
        builder.timeField(Fields.TIMESTAMP.getPreferredName(), Fields.TIMESTAMP.getPreferredName() + "_string", timestamp.toEpochMilli());
        builder.field(PARAMETERS.getPreferredName(), parameters);
        builder.field(TIMING_STATS.getPreferredName(), timingStats);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OutlierDetectionStats that = (OutlierDetectionStats) o;
        return Objects.equals(jobId, that.jobId)
            && Objects.equals(timestamp, that.timestamp)
            && Objects.equals(parameters, that.parameters)
            && Objects.equals(timingStats, that.timingStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, timestamp, parameters, timingStats);
    }

    public String documentId(String _jobId) {
        return documentIdPrefix(_jobId) + timestamp.toEpochMilli();
    }

    public static String documentIdPrefix(String jobId) {
        return TYPE_VALUE + "_" + jobId + "_";
    }
}
