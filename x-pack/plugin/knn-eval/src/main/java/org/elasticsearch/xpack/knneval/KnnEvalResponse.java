/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Reports each candidate setting's recall and cost against a common baseline. */
final class KnnEvalResponse extends ActionResponse implements ToXContentObject {

    /** Counts full-precision comparisons across all matching vectors. */
    static final String FULL_PRECISION_SCAN = "full_precision_scan";

    /** Counts quantized visits plus full-precision rescoring work. */
    static final String QUANTIZED_VISIT_PLUS_RESCORE = "quantized_visit_plus_rescore";

    static final ParseField BASELINE_FIELD = new ParseField("baseline");
    static final ParseField BASELINE_TOOK_MS_FIELD = new ParseField("baseline_took_ms");
    static final ParseField BASELINE_VECTOR_OPS_FIELD = new ParseField("baseline_vector_ops");
    static final ParseField BASELINE_VECTOR_OPS_KIND_FIELD = new ParseField("baseline_vector_ops_kind");
    static final ParseField RESULTS_FIELD = new ParseField("results");
    static final ParseField FAILURES_FIELD = new ParseField("failures");

    private final ReportedSettings baseline;
    private final long baselineTookMs;
    private final long baselineVectorOps;
    private final String baselineVectorOpsKind;
    private final List<KnnSettingsResult> results;
    private final Map<String, Exception> failures;

    KnnEvalResponse(
        ReportedSettings baseline,
        long baselineTookMs,
        long baselineVectorOps,
        String baselineVectorOpsKind,
        List<KnnSettingsResult> results,
        Map<String, Exception> failures
    ) {
        this.baseline = Objects.requireNonNull(baseline);
        this.baselineTookMs = baselineTookMs;
        this.baselineVectorOps = baselineVectorOps;
        this.baselineVectorOpsKind = Objects.requireNonNull(baselineVectorOpsKind);
        this.results = List.copyOf(results);
        this.failures = Map.copyOf(failures);
    }

    KnnEvalResponse(StreamInput in) throws IOException {
        this.baseline = new ReportedSettings(in);
        this.baselineTookMs = in.readVLong();
        this.baselineVectorOps = in.readVLong();
        this.baselineVectorOpsKind = in.readString();
        this.results = in.readCollectionAsList(KnnSettingsResult::new);
        this.failures = in.readMap(StreamInput::readException);
    }

    public ReportedSettings getBaseline() {
        return baseline;
    }

    public long getBaselineTookMs() {
        return baselineTookMs;
    }

    public long getBaselineVectorOps() {
        return baselineVectorOps;
    }

    public String getBaselineVectorOpsKind() {
        return baselineVectorOpsKind;
    }

    public List<KnnSettingsResult> getResults() {
        return results;
    }

    public Map<String, Exception> getFailures() {
        return failures;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        baseline.writeTo(out);
        out.writeVLong(baselineTookMs);
        out.writeVLong(baselineVectorOps);
        out.writeString(baselineVectorOpsKind);
        out.writeCollection(results);
        out.writeMap(failures, StreamOutput::writeException);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(BASELINE_FIELD.getPreferredName());
        baseline.toXContent(builder, params);
        builder.field(BASELINE_TOOK_MS_FIELD.getPreferredName(), baselineTookMs);
        builder.field(BASELINE_VECTOR_OPS_FIELD.getPreferredName(), baselineVectorOps);
        builder.field(BASELINE_VECTOR_OPS_KIND_FIELD.getPreferredName(), baselineVectorOpsKind);
        builder.startArray(RESULTS_FIELD.getPreferredName());
        for (KnnSettingsResult result : results) {
            result.toXContent(builder, params);
        }
        builder.endArray();
        builder.startObject(FAILURES_FIELD.getPreferredName());
        for (Map.Entry<String, Exception> failure : failures.entrySet()) {
            builder.startObject(failure.getKey());
            ElasticsearchException.generateFailureXContent(builder, params, failure.getValue(), true);
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KnnEvalResponse that = (KnnEvalResponse) o;
        return baselineTookMs == that.baselineTookMs
            && baselineVectorOps == that.baselineVectorOps
            && baseline.equals(that.baseline)
            && baselineVectorOpsKind.equals(that.baselineVectorOpsKind)
            && results.equals(that.results)
            && failureMessages().equals(that.failureMessages());
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseline, baselineTookMs, baselineVectorOps, baselineVectorOpsKind, results, failureMessages());
    }

    // Exceptions lack value equality, so failures compare by message.
    private Map<String, String> failureMessages() {
        return failures.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> String.valueOf(e.getValue().getMessage())));
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /** How one settings entry compared with the baseline. */
    public record KnnSettingsResult(
        ReportedSettings knnSettings,
        @Nullable Double recall,
        long includedQueries,
        long excludedQueries,
        long tookMs,
        long vectorOps
    ) implements Writeable, ToXContentObject {

        static final ParseField KNN_SETTINGS_FIELD = new ParseField("knn_settings");
        static final ParseField RECALL_FIELD = new ParseField("recall");
        static final ParseField INCLUDED_QUERIES_FIELD = new ParseField("included_queries");
        static final ParseField EXCLUDED_QUERIES_FIELD = new ParseField("excluded_queries");
        static final ParseField TOOK_MS_FIELD = new ParseField("took_ms");
        static final ParseField VECTOR_OPS_FIELD = new ParseField("vector_ops");

        public KnnSettingsResult {
            knnSettings = Objects.requireNonNull(knnSettings);
        }

        KnnSettingsResult(StreamInput in) throws IOException {
            this(new ReportedSettings(in), in.readOptionalDouble(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            knnSettings.writeTo(out);
            out.writeOptionalDouble(recall);
            out.writeVLong(includedQueries);
            out.writeVLong(excludedQueries);
            out.writeVLong(tookMs);
            out.writeVLong(vectorOps);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(KNN_SETTINGS_FIELD.getPreferredName());
            knnSettings.toXContent(builder, params);
            builder.field(RECALL_FIELD.getPreferredName(), recall);
            builder.field(INCLUDED_QUERIES_FIELD.getPreferredName(), includedQueries);
            builder.field(EXCLUDED_QUERIES_FIELD.getPreferredName(), excludedQueries);
            builder.field(TOOK_MS_FIELD.getPreferredName(), tookMs);
            builder.field(VECTOR_OPS_FIELD.getPreferredName(), vectorOps);
            builder.endObject();
            return builder;
        }
    }

    /** The requested settings plus whether full-precision rescoring hit its 10,000-vector limit. */
    public record ReportedSettings(KnnEvalSettings knnSettings, boolean rescoreWindowCapped) implements Writeable, ToXContentObject {

        static final ParseField RESCORE_WINDOW_CAPPED_FIELD = new ParseField("rescore_window_capped");

        public static ReportedSettings of(KnnEvalSettings knnSettings) {
            return new ReportedSettings(knnSettings, false);
        }

        public ReportedSettings {
            knnSettings = Objects.requireNonNull(knnSettings);
        }

        ReportedSettings(StreamInput in) throws IOException {
            this(new KnnEvalSettings(in), in.readBoolean());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            knnSettings.writeTo(out);
            out.writeBoolean(rescoreWindowCapped);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            knnSettings.innerToXContent(builder, params);
            if (rescoreWindowCapped) {
                builder.field(RESCORE_WINDOW_CAPPED_FIELD.getPreferredName(), true);
            }
            builder.endObject();
            return builder;
        }
    }

}
