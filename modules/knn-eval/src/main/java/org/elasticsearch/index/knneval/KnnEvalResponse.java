/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.knneval;

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

/**
 * The result of a {@link KnnEvalRequest}: one recall figure per knob set, all measured against the same baseline.
 * <p>
 * Holds no pooled {@link org.elasticsearch.search.SearchHit} references -- only copied ids and scores -- so it needs no
 * {@code close()}.
 */
public class KnnEvalResponse extends ActionResponse implements ToXContentObject {

    /** One float32 comparison per live vector scanned, taken from the total hit count because exact_knn is not profiled. */
    public static final String FULL_PRECISION_SCAN = "full_precision_scan";

    /**
     * Mostly 1-bit comparisons over the visited posting lists plus the rescore window. Comparable with {@link #FULL_PRECISION_SCAN} as
     * work done, not as bytes touched.
     */
    public static final String QUANTIZED_VISIT_PLUS_RESCORE = "quantized_visit_plus_rescore";

    static final ParseField BASELINE_FIELD = new ParseField("baseline");
    static final ParseField BASELINE_TOOK_MS_FIELD = new ParseField("baseline_took_ms");
    static final ParseField BASELINE_VECTOR_OPS_FIELD = new ParseField("baseline_vector_ops");
    static final ParseField BASELINE_VECTOR_OPS_KIND_FIELD = new ParseField("baseline_vector_ops_kind");
    static final ParseField BASELINE_DETAILS_FIELD = new ParseField("baseline_details");
    static final ParseField ENVIRONMENT_FIELD = new ParseField("environment");
    static final ParseField VALUE_TOLERANCE_FIELD = new ParseField("value_tolerance");
    static final ParseField RESULTS_FIELD = new ParseField("results");
    static final ParseField FAILURES_FIELD = new ParseField("failures");

    private final EffectiveKnobs baseline;
    private final KnnEvalStats baselineTookMs;
    private final KnnEvalStats baselineVectorOps;
    /** the two kinds are not the same unit of work; see the constants above */
    private final String baselineVectorOpsKind;
    /** reported once rather than per candidate; empty unless details were asked for */
    private final Map<String, KnnEvalDetails.BaselineDetail> baselineDetails;
    /** What the numbers were measured on, so a stored response is reproducible; {@code null} when nothing could be read. */
    @Nullable
    private final KnnEvalEnvironment environment;
    /** Echoed so a stored response is self-describing; {@code null} when the value-based metrics were not requested. */
    @Nullable
    private final Double valueTolerance;
    private final List<KnnSettingsResult> results;
    /** exceptions for individual evaluation queries, keyed by their id */
    private final Map<String, Exception> failures;

    public KnnEvalResponse(
        EffectiveKnobs baseline,
        KnnEvalStats baselineTookMs,
        KnnEvalStats baselineVectorOps,
        String baselineVectorOpsKind,
        Map<String, KnnEvalDetails.BaselineDetail> baselineDetails,
        @Nullable KnnEvalEnvironment environment,
        @Nullable Double valueTolerance,
        List<KnnSettingsResult> results,
        Map<String, Exception> failures
    ) {
        this.baseline = Objects.requireNonNull(baseline);
        this.baselineTookMs = Objects.requireNonNull(baselineTookMs);
        this.baselineVectorOps = Objects.requireNonNull(baselineVectorOps);
        this.baselineVectorOpsKind = Objects.requireNonNull(baselineVectorOpsKind);
        this.baselineDetails = Map.copyOf(baselineDetails);
        this.environment = environment;
        this.valueTolerance = valueTolerance;
        this.results = List.copyOf(results);
        this.failures = Map.copyOf(failures);
    }

    KnnEvalResponse(StreamInput in) throws IOException {
        this.baseline = new EffectiveKnobs(in);
        this.baselineTookMs = new KnnEvalStats(in);
        this.baselineVectorOps = new KnnEvalStats(in);
        this.baselineVectorOpsKind = in.readString();
        this.baselineDetails = in.readMap(KnnEvalDetails.BaselineDetail::new);
        this.environment = in.readOptionalWriteable(KnnEvalEnvironment::new);
        this.valueTolerance = in.readOptionalDouble();
        this.results = in.readCollectionAsList(KnnSettingsResult::new);
        this.failures = in.readMap(StreamInput::readException);
    }

    public EffectiveKnobs getBaseline() {
        return baseline;
    }

    public KnnEvalStats getBaselineTookMs() {
        return baselineTookMs;
    }

    public KnnEvalStats getBaselineVectorOps() {
        return baselineVectorOps;
    }

    public String getBaselineVectorOpsKind() {
        return baselineVectorOpsKind;
    }

    public Map<String, KnnEvalDetails.BaselineDetail> getBaselineDetails() {
        return baselineDetails;
    }

    @Nullable
    public KnnEvalEnvironment getEnvironment() {
        return environment;
    }

    @Nullable
    public Double getValueTolerance() {
        return valueTolerance;
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
        baselineTookMs.writeTo(out);
        baselineVectorOps.writeTo(out);
        out.writeString(baselineVectorOpsKind);
        out.writeMap(baselineDetails, StreamOutput::writeWriteable);
        out.writeOptionalWriteable(environment);
        out.writeOptionalDouble(valueTolerance);
        out.writeCollection(results);
        out.writeMap(failures, StreamOutput::writeException);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(BASELINE_FIELD.getPreferredName());
        baseline.toXContent(builder, params);
        builder.field(BASELINE_TOOK_MS_FIELD.getPreferredName());
        baselineTookMs.toXContent(builder, params);
        builder.field(BASELINE_VECTOR_OPS_FIELD.getPreferredName());
        baselineVectorOps.toXContent(builder, params);
        builder.field(BASELINE_VECTOR_OPS_KIND_FIELD.getPreferredName(), baselineVectorOpsKind);
        if (baselineDetails.isEmpty() == false) {
            builder.startObject(BASELINE_DETAILS_FIELD.getPreferredName());
            for (Map.Entry<String, KnnEvalDetails.BaselineDetail> baselineDetail : baselineDetails.entrySet()) {
                builder.field(baselineDetail.getKey());
                baselineDetail.getValue().toXContent(builder, params);
            }
            builder.endObject();
        }
        if (environment != null) {
            builder.field(ENVIRONMENT_FIELD.getPreferredName());
            environment.toXContent(builder, params);
        }
        if (valueTolerance != null) {
            builder.field(VALUE_TOLERANCE_FIELD.getPreferredName(), valueTolerance);
        }
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

    /**
     * How one knob set compared with the baseline.
     * <p>
     * {@code recall} is recall of the <em>baseline's</em> top-k, not of relevance judgements: a knob set can score 1.0 while both
     * configurations return poor results. A large gap between it and {@code recallValue} says the corpus has duplicates or dense ties,
     * so the id-based number is punishing an equally good alternative. {@code tookMs} is the search response's {@code took}, not
     * end-to-end latency; {@code vectorOps} is the load-independent cost axis.
     *
     * @param recallHistogramBinWidth present only when the histogram is binned, in which case each entry's {@code recall} means
     *                    {@code [recall, recall + width)} -- except 1.0, which stays exact
     */
    public record KnnSettingsResult(
        EffectiveKnobs knnSettings,
        double recall,
        KnnEvalStats recallStats,
        @Nullable List<RecallBucket> recallHistogram,
        @Nullable Double recallHistogramBinWidth,
        @Nullable Double recallValue,
        @Nullable KnnEvalStats recallValueStats,
        @Nullable String recallValueSkipped,
        @Nullable KnnEvalFidelity.Summary fidelity,
        KnnEvalStats tookMs,
        KnnEvalStats vectorOps,
        Map<String, KnnEvalDetails.QueryDetail> details
    ) implements Writeable, ToXContentObject {

        static final ParseField KNN_SETTINGS_FIELD = new ParseField("knn_settings");
        static final ParseField RECALL_FIELD = new ParseField("recall");
        static final ParseField RECALL_STATS_FIELD = new ParseField("recall_stats");
        static final ParseField RECALL_HISTOGRAM_FIELD = new ParseField("recall_histogram");
        static final ParseField RECALL_HISTOGRAM_BIN_WIDTH_FIELD = new ParseField("recall_histogram_bin_width");
        static final ParseField RECALL_VALUE_FIELD = new ParseField("recall_value");
        static final ParseField RECALL_VALUE_STATS_FIELD = new ParseField("recall_value_stats");
        static final ParseField RECALL_VALUE_SKIPPED_FIELD = new ParseField("recall_value_skipped");
        static final ParseField FIDELITY_FIELD = new ParseField("fidelity");
        static final ParseField TOOK_MS_FIELD = new ParseField("took_ms");
        static final ParseField VECTOR_OPS_FIELD = new ParseField("vector_ops");
        static final ParseField DETAILS_FIELD = new ParseField("details");

        public KnnSettingsResult(
            EffectiveKnobs knnSettings,
            double recall,
            KnnEvalStats recallStats,
            @Nullable List<RecallBucket> recallHistogram,
            @Nullable Double recallHistogramBinWidth,
            @Nullable Double recallValue,
            @Nullable KnnEvalStats recallValueStats,
            @Nullable String recallValueSkipped,
            @Nullable KnnEvalFidelity.Summary fidelity,
            KnnEvalStats tookMs,
            KnnEvalStats vectorOps,
            Map<String, KnnEvalDetails.QueryDetail> details
        ) {
            this.knnSettings = Objects.requireNonNull(knnSettings);
            this.recall = recall;
            this.recallStats = Objects.requireNonNull(recallStats);
            this.recallHistogram = recallHistogram == null ? null : List.copyOf(recallHistogram);
            this.recallHistogramBinWidth = recallHistogramBinWidth;
            this.recallValue = recallValue;
            this.recallValueStats = recallValueStats;
            this.recallValueSkipped = recallValueSkipped;
            this.fidelity = fidelity;
            this.tookMs = Objects.requireNonNull(tookMs);
            this.vectorOps = Objects.requireNonNull(vectorOps);
            this.details = Map.copyOf(details);
        }

        KnnSettingsResult(StreamInput in) throws IOException {
            this(
                new EffectiveKnobs(in),
                in.readDouble(),
                new KnnEvalStats(in),
                in.readOptionalCollectionAsList(RecallBucket::new),
                in.readOptionalDouble(),
                in.readOptionalDouble(),
                in.readOptionalWriteable(KnnEvalStats::new),
                in.readOptionalString(),
                in.readOptionalWriteable(KnnEvalFidelity.Summary::new),
                new KnnEvalStats(in),
                new KnnEvalStats(in),
                in.readMap(KnnEvalDetails.QueryDetail::new)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            knnSettings.writeTo(out);
            out.writeDouble(recall);
            recallStats.writeTo(out);
            out.writeOptionalCollection(recallHistogram);
            out.writeOptionalDouble(recallHistogramBinWidth);
            out.writeOptionalDouble(recallValue);
            out.writeOptionalWriteable(recallValueStats);
            out.writeOptionalString(recallValueSkipped);
            out.writeOptionalWriteable(fidelity);
            tookMs.writeTo(out);
            vectorOps.writeTo(out);
            out.writeMap(details, StreamOutput::writeWriteable);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(KNN_SETTINGS_FIELD.getPreferredName());
            knnSettings.toXContent(builder, params);
            builder.field(RECALL_FIELD.getPreferredName(), recall);
            builder.field(RECALL_STATS_FIELD.getPreferredName());
            recallStats.toXContent(builder, params);
            if (recallHistogram != null) {
                builder.startArray(RECALL_HISTOGRAM_FIELD.getPreferredName());
                for (RecallBucket bucket : recallHistogram) {
                    bucket.toXContent(builder, params);
                }
                builder.endArray();
                if (recallHistogramBinWidth != null) {
                    builder.field(RECALL_HISTOGRAM_BIN_WIDTH_FIELD.getPreferredName(), recallHistogramBinWidth);
                }
            }
            // fidelity is the marker for the whole value-based group
            if (fidelity != null) {
                builder.field(RECALL_VALUE_FIELD.getPreferredName(), recallValue);
                if (recallValueSkipped != null) {
                    builder.field(RECALL_VALUE_SKIPPED_FIELD.getPreferredName(), recallValueSkipped);
                } else {
                    builder.field(RECALL_VALUE_STATS_FIELD.getPreferredName());
                    recallValueStats.toXContent(builder, params);
                }
                builder.field(FIDELITY_FIELD.getPreferredName());
                fidelity.toXContent(builder, params);
            }
            builder.field(TOOK_MS_FIELD.getPreferredName());
            tookMs.toXContent(builder, params);
            builder.field(VECTOR_OPS_FIELD.getPreferredName());
            vectorOps.toXContent(builder, params);
            if (details.isEmpty() == false) {
                builder.startObject(DETAILS_FIELD.getPreferredName());
                for (Map.Entry<String, KnnEvalDetails.QueryDetail> detail : details.entrySet()) {
                    builder.field(detail.getKey());
                    detail.getValue().toXContent(builder, params);
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }
    }

    /**
     * A knob set echoed back with what the search will actually do. Both derived values are {@code null} when the field mapping could
     * not be read, which {@code view_index_metadata} is what enables.
     */
    public record EffectiveKnobs(KnnEvalKnobs knobs, @Nullable Integer effectiveNumCandidates, @Nullable Integer rescoreWindow)
        implements
            Writeable,
            ToXContentObject {

        static final ParseField EFFECTIVE_NUM_CANDIDATES_FIELD = new ParseField("effective_num_candidates");
        static final ParseField RESCORE_WINDOW_FIELD = new ParseField("rescore_window");

        public static EffectiveKnobs of(KnnEvalKnobs knobs) {
            return new EffectiveKnobs(knobs, null, null);
        }

        public EffectiveKnobs(KnnEvalKnobs knobs, @Nullable Integer effectiveNumCandidates, @Nullable Integer rescoreWindow) {
            this.knobs = Objects.requireNonNull(knobs);
            this.effectiveNumCandidates = effectiveNumCandidates;
            this.rescoreWindow = rescoreWindow;
        }

        EffectiveKnobs(StreamInput in) throws IOException {
            this(new KnnEvalKnobs(in), in.readOptionalVInt(), in.readOptionalVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            knobs.writeTo(out);
            out.writeOptionalVInt(effectiveNumCandidates);
            out.writeOptionalVInt(rescoreWindow);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            knobs.innerToXContent(builder, params);
            if (effectiveNumCandidates != null) {
                builder.field(EFFECTIVE_NUM_CANDIDATES_FIELD.getPreferredName(), effectiveNumCandidates);
            }
            if (rescoreWindow != null) {
                builder.field(RESCORE_WINDOW_FIELD.getPreferredName(), rescoreWindow);
            }
            builder.endObject();
            return builder;
        }
    }

}
