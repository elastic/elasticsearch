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
    static final ParseField MAX_QUERIES_PER_BATCH_FIELD = new ParseField("max_queries_per_batch");
    static final ParseField RESULTS_FIELD = new ParseField("results");
    static final ParseField FAILURES_FIELD = new ParseField("failures");

    private final ReportedKnobs baseline;
    private final long baselineTookMs;
    private final long baselineVectorOps;
    private final String baselineVectorOpsKind;
    private final int maxQueriesPerBatch;
    private final List<KnnSettingsResult> results;
    private final Map<String, Exception> failures;

    /** Creates an immutable snapshot of evaluation results. */
    KnnEvalResponse(
        ReportedKnobs baseline,
        long baselineTookMs,
        long baselineVectorOps,
        String baselineVectorOpsKind,
        int maxQueriesPerBatch,
        List<KnnSettingsResult> results,
        Map<String, Exception> failures
    ) {
        this.baseline = Objects.requireNonNull(baseline);
        this.baselineTookMs = baselineTookMs;
        this.baselineVectorOps = baselineVectorOps;
        this.baselineVectorOpsKind = Objects.requireNonNull(baselineVectorOpsKind);
        this.maxQueriesPerBatch = maxQueriesPerBatch;
        this.results = List.copyOf(results);
        this.failures = Map.copyOf(failures);
    }

    KnnEvalResponse(StreamInput in) throws IOException {
        this.baseline = new ReportedKnobs(in);
        this.baselineTookMs = in.readVLong();
        this.baselineVectorOps = in.readVLong();
        this.baselineVectorOpsKind = in.readString();
        this.maxQueriesPerBatch = in.readVInt();
        this.results = in.readCollectionAsList(KnnSettingsResult::new);
        this.failures = in.readMap(StreamInput::readException);
    }

    public ReportedKnobs getBaseline() {
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

    public int getMaxQueriesPerBatch() {
        return maxQueriesPerBatch;
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
        out.writeVInt(maxQueriesPerBatch);
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
        builder.field(MAX_QUERIES_PER_BATCH_FIELD.getPreferredName(), maxQueriesPerBatch);
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
    public String toString() {
        return Strings.toString(this);
    }

    /** How one knob set compared with the baseline. */
    public record KnnSettingsResult(
        ReportedKnobs knnSettings,
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
            this(new ReportedKnobs(in), in.readOptionalDouble(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
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

    /** Requested knobs plus whether full-precision rescoring hit its 10,000-vector limit. */
    public record ReportedKnobs(KnnEvalKnobs knobs, boolean rescoreWindowCapped) implements Writeable, ToXContentObject {

        static final ParseField RESCORE_WINDOW_CAPPED_FIELD = new ParseField("rescore_window_capped");

        /** Creates knobs whose rescore window is not capped. */
        public static ReportedKnobs of(KnnEvalKnobs knobs) {
            return new ReportedKnobs(knobs, false);
        }

        public ReportedKnobs {
            knobs = Objects.requireNonNull(knobs);
        }

        ReportedKnobs(StreamInput in) throws IOException {
            this(new KnnEvalKnobs(in), in.readBoolean());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            knobs.writeTo(out);
            out.writeBoolean(rescoreWindowCapped);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            knobs.innerToXContent(builder, params);
            if (rescoreWindowCapped) {
                builder.field(RESCORE_WINDOW_CAPPED_FIELD.getPreferredName(), true);
            }
            builder.endObject();
            return builder;
        }
    }

}
