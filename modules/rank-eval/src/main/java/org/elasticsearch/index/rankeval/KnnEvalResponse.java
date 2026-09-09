/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * The result of a {@link KnnEvalRequest}: one recall figure per candidate configuration, all measured against the same baseline.
 * <p>
 * Unlike {@link RankEvalResponse} this response holds no references to pooled {@link org.elasticsearch.search.SearchHit} instances --
 * the transport action copies out only the ids and scores it needs while the search response is still alive, so this object needs no
 * {@code close()} and can be handed to a REST listener like any plain response.
 */
public class KnnEvalResponse extends ActionResponse implements ToXContentObject {

    /**
     * An exact baseline's operations: one float32 comparison per live vector scanned, counted from the query's total hits because the
     * work is known by construction rather than profiled.
     */
    public static final String FULL_PRECISION_SCAN = "full_precision_scan";

    /**
     * An approximate baseline's operations, as the codec counts them: mostly 1-bit comparisons over the visited posting lists plus the
     * full-precision rescore window. Comparable with {@link #FULL_PRECISION_SCAN} as "work done", not as bytes touched -- a
     * full-precision comparison is far more expensive than a 1-bit one.
     */
    public static final String QUANTIZED_VISIT_PLUS_RESCORE = "quantized_visit_plus_rescore";

    static final ParseField BASELINE_FIELD = new ParseField("baseline");
    static final ParseField BASELINE_TOOK_MS_FIELD = new ParseField("baseline_took_ms");
    static final ParseField BASELINE_VECTOR_OPS_FIELD = new ParseField("baseline_vector_ops");
    static final ParseField BASELINE_VECTOR_OPS_KIND_FIELD = new ParseField("baseline_vector_ops_kind");
    static final ParseField BASELINE_DETAILS_FIELD = new ParseField("baseline_details");
    static final ParseField VALUE_TOLERANCE_FIELD = new ParseField("value_tolerance");
    static final ParseField RESULTS_FIELD = new ParseField("results");
    static final ParseField FAILURES_FIELD = new ParseField("failures");

    private final EffectiveKnobs baseline;
    private final LongStats baselineTookMs;
    private final LongStats baselineVectorOps;
    /** what the baseline's vector operations were, since the two kinds are not the same unit of work; see the constants above */
    private final String baselineVectorOpsKind;
    /** the reference hit list per query, reported once rather than repeated under every candidate; empty unless details were asked for */
    private final Map<String, BaselineDetail> baselineDetails;
    /**
     * Echoed so that a stored response is self-describing: the value recall means nothing without the slack it was measured at.
     * {@code null} when the value-based metrics were not requested, in which case nothing about them appears at all.
     */
    @Nullable
    private final Double valueTolerance;
    private final List<CandidateResult> results;
    /** exceptions for individual evaluation queries, keyed by their id */
    private final Map<String, Exception> failures;

    public KnnEvalResponse(
        EffectiveKnobs baseline,
        LongStats baselineTookMs,
        LongStats baselineVectorOps,
        String baselineVectorOpsKind,
        Map<String, BaselineDetail> baselineDetails,
        @Nullable Double valueTolerance,
        List<CandidateResult> results,
        Map<String, Exception> failures
    ) {
        this.baseline = Objects.requireNonNull(baseline);
        this.baselineTookMs = Objects.requireNonNull(baselineTookMs);
        this.baselineVectorOps = Objects.requireNonNull(baselineVectorOps);
        this.baselineVectorOpsKind = Objects.requireNonNull(baselineVectorOpsKind);
        this.baselineDetails = Map.copyOf(baselineDetails);
        this.valueTolerance = valueTolerance;
        this.results = List.copyOf(results);
        this.failures = Map.copyOf(failures);
    }

    KnnEvalResponse(StreamInput in) throws IOException {
        this.baseline = new EffectiveKnobs(in);
        this.baselineTookMs = new LongStats(in);
        this.baselineVectorOps = new LongStats(in);
        this.baselineVectorOpsKind = in.readString();
        this.baselineDetails = in.readMap(BaselineDetail::new);
        this.valueTolerance = in.readOptionalDouble();
        this.results = in.readCollectionAsList(CandidateResult::new);
        this.failures = in.readMap(StreamInput::readException);
    }

    public EffectiveKnobs getBaseline() {
        return baseline;
    }

    public LongStats getBaselineTookMs() {
        return baselineTookMs;
    }

    public LongStats getBaselineVectorOps() {
        return baselineVectorOps;
    }

    public String getBaselineVectorOpsKind() {
        return baselineVectorOpsKind;
    }

    public Map<String, BaselineDetail> getBaselineDetails() {
        return baselineDetails;
    }

    @Nullable
    public Double getValueTolerance() {
        return valueTolerance;
    }

    public List<CandidateResult> getResults() {
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
            for (Map.Entry<String, BaselineDetail> baselineDetail : baselineDetails.entrySet()) {
                builder.field(baselineDetail.getKey());
                baselineDetail.getValue().toXContent(builder, params);
            }
            builder.endObject();
        }
        if (valueTolerance != null) {
            builder.field(VALUE_TOLERANCE_FIELD.getPreferredName(), valueTolerance);
        }
        builder.startArray(RESULTS_FIELD.getPreferredName());
        for (CandidateResult result : results) {
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
     * How one candidate configuration compared with the baseline.
     * <p>
     * <b>{@code recall} here is recall of the <em>baseline's</em> top-k</b> -- the {@code baseline} object in the response is the
     * referent -- and not recall against relevance judgements. A candidate can score 1.0 while both configurations return poor results.
     *
     * @param recall      the mean over queries of {@link RecallAtK} against the baseline's top-k
     * @param recallStats the distribution of the same per-query values. Per-query recall at small {@code k} is quantised to multiples
     *                    of {@code 1/k}, so these percentiles are step-like by nature rather than by accident.
     * @param recallValue value-based recall: the mean fraction of the baseline's window the candidate matched <em>in value</em> rather
     *                    than by id. A large gap between this and {@code recall} says the corpus has duplicates or dense ties, so the
     *                    id-based number is punishing the candidate for picking an equally good alternative. {@code null}, with
     *                    {@code recallValueSkipped} set, when scores are not real similarities.
     * @param fidelity    how much similarity the candidate gave up on the documents it did return, which recall alone cannot show
     * @param recallHistogramBinWidth present only when {@code recallHistogram} is binned rather than exact, in which case each
     *                    entry's {@code recall} means {@code [recall, recall + width)} -- except 1.0, which stays exact
     * @param recallHistogram the per-query recall values grouped for reading, ascending. Each entry's {@code recall} is an exact
     *                    observed value, or -- when {@code recallHistogramBinWidth} is present -- the lower edge of a half-open bin,
     *                    except for 1.0 which is always exact. The percentiles above tie heavily, so this is what shows the shape.
     * @param tookMs     wall-clock search time this candidate's runs reported. This is the {@code took} of the search response, i.e.
     *                   coordinator-measured query and fetch time; it is <em>not</em> end-to-end latency, since it excludes the client
     *                   round trip and the msearch queueing this action introduces. Comparable between baseline and candidates, not
     *                   against a client-side timing of the same query.
     * @param vectorOps  vector comparisons performed, summed across shards for each search. Unlike {@code tookMs} this is independent of
     *                   machine load and cache state, so it is the honest cost axis to plot recall against.
     * @param details    per-query breakdown, empty unless {@link KnnEvalSpec#isIncludeDetails()} was set
     */
    public record CandidateResult(
        EffectiveKnobs candidate,
        double recall,
        DoubleStats recallStats,
        List<RecallBucket> recallHistogram,
        @Nullable Double recallHistogramBinWidth,
        @Nullable Double recallValue,
        @Nullable DoubleStats recallValueStats,
        @Nullable String recallValueSkipped,
        @Nullable Fidelity fidelity,
        LongStats tookMs,
        LongStats vectorOps,
        Map<String, QueryDetail> details
    ) implements Writeable, ToXContentObject {

        static final ParseField CANDIDATE_FIELD = new ParseField("candidate");
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

        public CandidateResult(
            EffectiveKnobs candidate,
            double recall,
            DoubleStats recallStats,
            List<RecallBucket> recallHistogram,
            @Nullable Double recallHistogramBinWidth,
            @Nullable Double recallValue,
            @Nullable DoubleStats recallValueStats,
            @Nullable String recallValueSkipped,
            @Nullable Fidelity fidelity,
            LongStats tookMs,
            LongStats vectorOps,
            Map<String, QueryDetail> details
        ) {
            this.candidate = Objects.requireNonNull(candidate);
            this.recall = recall;
            this.recallStats = Objects.requireNonNull(recallStats);
            this.recallHistogram = List.copyOf(recallHistogram);
            this.recallHistogramBinWidth = recallHistogramBinWidth;
            this.recallValue = recallValue;
            this.recallValueStats = recallValueStats;
            this.recallValueSkipped = recallValueSkipped;
            this.fidelity = fidelity;
            this.tookMs = Objects.requireNonNull(tookMs);
            this.vectorOps = Objects.requireNonNull(vectorOps);
            this.details = Map.copyOf(details);
        }

        CandidateResult(StreamInput in) throws IOException {
            this(
                new EffectiveKnobs(in),
                in.readDouble(),
                new DoubleStats(in),
                in.readCollectionAsList(RecallBucket::new),
                in.readOptionalDouble(),
                in.readOptionalDouble(),
                in.readOptionalWriteable(DoubleStats::new),
                in.readOptionalString(),
                in.readOptionalWriteable(Fidelity::new),
                new LongStats(in),
                new LongStats(in),
                in.readMap(QueryDetail::new)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            candidate.writeTo(out);
            out.writeDouble(recall);
            recallStats.writeTo(out);
            out.writeCollection(recallHistogram);
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
            builder.field(CANDIDATE_FIELD.getPreferredName());
            candidate.toXContent(builder, params);
            builder.field(RECALL_FIELD.getPreferredName(), recall);
            builder.field(RECALL_STATS_FIELD.getPreferredName());
            recallStats.toXContent(builder, params);
            builder.startArray(RECALL_HISTOGRAM_FIELD.getPreferredName());
            for (RecallBucket bucket : recallHistogram) {
                bucket.toXContent(builder, params);
            }
            builder.endArray();
            if (recallHistogramBinWidth != null) {
                builder.field(RECALL_HISTOGRAM_BIN_WIDTH_FIELD.getPreferredName(), recallHistogramBinWidth);
            }
            // fidelity is the marker for the whole value-based group: absent when the caller did not ask for it
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
                for (Map.Entry<String, QueryDetail> detail : details.entrySet()) {
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
     * One query's reference hit list, reported once at the top level. Repeating it under every candidate made the response grow with the
     * number of candidates for no new information, and it is the same list every candidate is scored against.
     */
    public record BaselineDetail(List<Hit> hits) implements Writeable, ToXContentObject {

        static final ParseField HITS_FIELD = new ParseField("hits");

        public BaselineDetail(List<Hit> hits) {
            this.hits = List.copyOf(hits);
        }

        BaselineDetail(StreamInput in) throws IOException {
            this(in.readCollectionAsList(Hit::new));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(hits);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray(HITS_FIELD.getPreferredName());
            for (Hit hit : hits) {
                hit.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }
    }

    /**
     * The recall of one candidate configuration on one query, annotated so that a caller can see <em>which</em> documents the cheaper
     * configuration got wrong without cross-referencing two lists by hand.
     *
     * @param relevantRetrieved how many of the baseline's top-k the candidate also returned; equals the number of hits with a
     *                          non-null {@code baseline_rank}
     * @param relevant          the size of the baseline's top-k, i.e. the achievable maximum. {@code relevant - relevantRetrieved}
     *                          equals {@code missed.size()}
     * @param missed            the baseline hits the candidate did not return, in baseline rank order. Each carries the score and rank
     *                          the baseline gave it, so a caller can see how good the misses were without joining against
     *                          {@code baseline_details}
     * @param epsilonProfile    per-rank fidelity loss, {@code null} where the loss is unbounded or the baseline never reached that rank;
     *                          empty when fidelity was skipped for the field
     * @param recallValue       this query's value-based recall, or {@code null} when it could not be computed for the field
     * @param valueMatches      how many of the candidate's hits were as good in value as the baseline's worst accepted hit
     * @param incomplete        true when at least one rank had an unbounded loss, which is why that query is excluded from
     *                          {@code fidelity.max_epsilon} and counted in {@code fidelity.infinite_count} instead
     */
    public record QueryDetail(
        double recall,
        long relevantRetrieved,
        long relevant,
        List<RankedHit> hits,
        List<RankedHit> missed,
        List<Double> epsilonProfile,
        boolean incomplete,
        @Nullable Double recallValue,
        long valueMatches
    ) implements Writeable, ToXContentObject {

        static final ParseField RECALL_FIELD = new ParseField("recall");
        static final ParseField RELEVANT_RETRIEVED_FIELD = new ParseField("relevant_retrieved");
        static final ParseField RELEVANT_FIELD = new ParseField("relevant");
        static final ParseField HITS_FIELD = new ParseField("hits");
        static final ParseField MISSED_FIELD = new ParseField("missed");
        static final ParseField RECALL_VALUE_FIELD = new ParseField("recall_value");
        static final ParseField VALUE_MATCHES_FIELD = new ParseField("value_matches");
        static final ParseField EPSILON_PROFILE_FIELD = new ParseField("epsilon_profile");
        static final ParseField INCOMPLETE_FIELD = new ParseField("incomplete");

        public QueryDetail(
            double recall,
            long relevantRetrieved,
            long relevant,
            List<RankedHit> hits,
            List<RankedHit> missed,
            List<Double> epsilonProfile,
            boolean incomplete,
            @Nullable Double recallValue,
            long valueMatches
        ) {
            this.recall = recall;
            this.relevantRetrieved = relevantRetrieved;
            this.relevant = relevant;
            this.hits = List.copyOf(hits);
            this.missed = List.copyOf(missed);
            // nulls are meaningful here (unbounded loss, or a rank the baseline never reached), so this cannot be List#copyOf
            this.epsilonProfile = Collections.unmodifiableList(new ArrayList<>(epsilonProfile));
            this.incomplete = incomplete;
            this.recallValue = recallValue;
            this.valueMatches = valueMatches;
        }

        QueryDetail(StreamInput in) throws IOException {
            this(
                in.readDouble(),
                in.readVLong(),
                in.readVLong(),
                in.readCollectionAsList(RankedHit::new),
                in.readCollectionAsList(RankedHit::new),
                in.readCollectionAsList(StreamInput::readOptionalDouble),
                in.readBoolean(),
                in.readOptionalDouble(),
                in.readVLong()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(recall);
            out.writeVLong(relevantRetrieved);
            out.writeVLong(relevant);
            out.writeCollection(hits);
            out.writeCollection(missed);
            out.writeCollection(epsilonProfile, StreamOutput::writeOptionalDouble);
            out.writeBoolean(incomplete);
            out.writeOptionalDouble(recallValue);
            out.writeVLong(valueMatches);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(RECALL_FIELD.getPreferredName(), recall);
            builder.field(RELEVANT_RETRIEVED_FIELD.getPreferredName(), relevantRetrieved);
            builder.field(RELEVANT_FIELD.getPreferredName(), relevant);
            builder.startArray(HITS_FIELD.getPreferredName());
            for (RankedHit hit : hits) {
                hit.toXContent(builder, params);
            }
            builder.endArray();
            builder.startArray(MISSED_FIELD.getPreferredName());
            for (RankedHit hit : missed) {
                hit.toXContent(builder, params);
            }
            builder.endArray();
            if (recallValue != null) {
                builder.field(RECALL_VALUE_FIELD.getPreferredName(), recallValue);
                builder.field(VALUE_MATCHES_FIELD.getPreferredName(), valueMatches);
            }
            if (epsilonProfile.isEmpty() == false) {
                builder.startArray(EPSILON_PROFILE_FIELD.getPreferredName());
                for (Double epsilon : epsilonProfile) {
                    builder.value(epsilon);
                }
                builder.endArray();
                builder.field(INCOMPLETE_FIELD.getPreferredName(), incomplete);
            }
            builder.endObject();
            return builder;
        }
    }

    /**
     * How much similarity a candidate gave up on the documents it did return.
     * <p>
     * Recall counts documents; fidelity measures the gap. A candidate that swaps the true neighbour for one a hair behind it scores the
     * same recall as one that returns something unrelated, and only fidelity separates them. Zero loss at every rank is equivalent to
     * recall 1.0.
     * <p>
     * Two caveats worth repeating wherever this number is shown. The reference is the {@code baseline} configuration, not exact search:
     * a 100% {@code visit_percentage} run is a proxy for ground truth, so a loss of zero means "as good as the baseline", not
     * "optimal". And the metric needs real scores, so it is skipped -- {@link #skipped} carries the reason -- when the field's scores
     * are quantized estimates.
     *
     * @param maxEpsilon    distribution over queries of each query's worst per-rank loss; {@code p95} is the usual dashboard scalar
     * @param infiniteCount queries whose loss was unbounded at some rank, excluded from {@code maxEpsilon} rather than clamped
     * @param meanEpsilonByRank mean loss at each rank, over the queries with a finite loss there; {@code null} at a rank no query
     *                      reached. Losses concentrated in the tail ranks mean something very different from losses at rank 0.
     */
    public record Fidelity(@Nullable String skipped, @Nullable DoubleStats maxEpsilon, long infiniteCount, List<Double> meanEpsilonByRank)
        implements
            Writeable,
            ToXContentObject {

        static final ParseField SKIPPED_FIELD = new ParseField("skipped");
        static final ParseField MAX_EPSILON_FIELD = new ParseField("max_epsilon");
        static final ParseField INFINITE_COUNT_FIELD = new ParseField("infinite_count");
        static final ParseField MEAN_EPSILON_BY_RANK_FIELD = new ParseField("mean_epsilon_by_rank");

        public static Fidelity skipped(String reason) {
            return new Fidelity(Objects.requireNonNull(reason), null, 0, List.of());
        }

        public static Fidelity of(DoubleStats maxEpsilon, long infiniteCount, List<Double> meanEpsilonByRank) {
            return new Fidelity(null, Objects.requireNonNull(maxEpsilon), infiniteCount, meanEpsilonByRank);
        }

        public Fidelity(@Nullable String skipped, @Nullable DoubleStats maxEpsilon, long infiniteCount, List<Double> meanEpsilonByRank) {
            this.skipped = skipped;
            this.maxEpsilon = maxEpsilon;
            this.infiniteCount = infiniteCount;
            // a rank nobody reached is null, so this cannot be List#copyOf
            this.meanEpsilonByRank = Collections.unmodifiableList(new ArrayList<>(meanEpsilonByRank));
        }

        Fidelity(StreamInput in) throws IOException {
            this(
                in.readOptionalString(),
                in.readOptionalWriteable(DoubleStats::new),
                in.readVLong(),
                in.readCollectionAsList(StreamInput::readOptionalDouble)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(skipped);
            out.writeOptionalWriteable(maxEpsilon);
            out.writeVLong(infiniteCount);
            out.writeCollection(meanEpsilonByRank, StreamOutput::writeOptionalDouble);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (skipped != null) {
                builder.field(SKIPPED_FIELD.getPreferredName(), skipped);
                builder.endObject();
                return builder;
            }
            builder.field(MAX_EPSILON_FIELD.getPreferredName());
            maxEpsilon.toXContent(builder, params);
            builder.field(INFINITE_COUNT_FIELD.getPreferredName(), infiniteCount);
            builder.startArray(MEAN_EPSILON_BY_RANK_FIELD.getPreferredName());
            for (Double epsilon : meanEpsilonByRank) {
                builder.value(epsilon);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }
    }

    /**
     * One entry of a recall histogram: an observed per-query recall value and how many queries produced it.
     * <p>
     * When the histogram is binned, {@code recall} is the lower edge of a half-open bin, the same convention the {@code histogram}
     * aggregation uses for its {@code key}. The 1.0 entry is always an exact value.
     */
    public record RecallBucket(double recall, long count) implements Writeable, ToXContentObject {

        static final ParseField RECALL_FIELD = new ParseField("recall");
        static final ParseField COUNT_FIELD = new ParseField("count");

        /** How much of a recall value is significant for grouping; enough to keep 1/k apart for any usable k, and to merge fp noise. */
        private static final double GROUPING_SCALE = 10_000.0;

        /**
         * The largest {@code k} for which the exact values are still a readable histogram. Per-query recall is a multiple of
         * {@code 1/relevant}, so a run at this {@code k} can produce at most 21 distinct values; beyond it the exact form degenerates
         * into a list nearly as long as the query set.
         */
        static final int MAX_EXACT_K = 20;

        /** Bin width used once {@code k} is too large for the exact form. Twenty bins plus an exact entry for 1.0 is again 21. */
        static final double BIN_WIDTH = 0.05;

        private static final int BINS = (int) Math.round(1.0 / BIN_WIDTH);

        static boolean isBinned(int k) {
            return k > MAX_EXACT_K;
        }

        /**
         * Groups the per-query values, ascending, with no empty entries.
         * <p>
         * At small {@code k} the values are already discrete, so each distinct one gets its own entry. At large {@code k} they are dense
         * enough that the exact form stops being a summary, so each entry becomes the lower edge of a {@link #BIN_WIDTH} bin -- except
         * for 1.0, which stays exact so that "perfect" is never merged with "nearly perfect".
         */
        public static List<RecallBucket> histogram(List<Double> values, int k) {
            SortedMap<Double, Long> counts = new TreeMap<>();
            for (double value : values) {
                counts.merge(isBinned(k) ? lowerEdgeOf(value) : round(value), 1L, Long::sum);
            }
            List<RecallBucket> buckets = new ArrayList<>(counts.size());
            for (Map.Entry<Double, Long> count : counts.entrySet()) {
                buckets.add(new RecallBucket(count.getKey(), count.getValue()));
            }
            return buckets;
        }

        /** The lower edge of the bin a value falls in, or 1.0 for an exact match. */
        private static double lowerEdgeOf(double value) {
            if (value >= 1.0) {
                return 1.0;
            }
            // the epsilon keeps a value that should sit exactly on an edge (0.15 arriving as 0.1499999...) out of the bin below it
            int bin = Math.min(BINS - 1, (int) Math.floor(value / BIN_WIDTH + 1e-9));
            return round(bin * BIN_WIDTH);
        }

        /**
         * Rounds so that arithmetic which should have produced the same fraction ({@code 0.30000000000000004} against {@code 0.3})
         * lands in one bucket.
         */
        private static double round(double value) {
            return Math.round(value * GROUPING_SCALE) / GROUPING_SCALE;
        }

        RecallBucket(StreamInput in) throws IOException {
            this(in.readDouble(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(recall);
            out.writeVLong(count);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(RECALL_FIELD.getPreferredName(), recall);
            builder.field(COUNT_FIELD.getPreferredName(), count);
            builder.endObject();
            return builder;
        }
    }

    /**
     * Distribution of the per-query recall values behind one candidate.
     * <p>
     * The mean alone hides the shape: a candidate averaging 0.9 because every query scores 0.9 is a very different proposition from one
     * that scores 1.0 on most queries and 0.2 on a tail of them. Note that a per-query recall can only be a multiple of {@code 1/k}, so
     * at small {@code k} these percentiles are step-like by construction.
     *
     * @param count how many queries contributed; equals the candidate's {@code took_ms.count}
     */
    public record DoubleStats(double mean, double min, double p10, double p50, double p90, double p95, double max, long count)
        implements
            Writeable,
            ToXContentObject {

        static final ParseField MEAN_FIELD = new ParseField("mean");
        static final ParseField MIN_FIELD = new ParseField("min");
        static final ParseField P10_FIELD = new ParseField("p10");
        static final ParseField P50_FIELD = new ParseField("p50");
        static final ParseField P90_FIELD = new ParseField("p90");
        static final ParseField P95_FIELD = new ParseField("p95");
        static final ParseField MAX_FIELD = new ParseField("max");
        static final ParseField COUNT_FIELD = new ParseField("count");

        public static final DoubleStats EMPTY = new DoubleStats(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0);

        /** Exact nearest-rank percentiles over a sorted copy: one request's worth of queries is small enough not to need a sketch. */
        public static DoubleStats of(List<Double> values) {
            if (values.isEmpty()) {
                return EMPTY;
            }
            double[] sorted = values.stream().mapToDouble(Double::doubleValue).sorted().toArray();
            double sum = 0.0;
            for (double value : sorted) {
                sum += value;
            }
            return new DoubleStats(
                sum / sorted.length,
                sorted[0],
                nearestRank(sorted, 10),
                nearestRank(sorted, 50),
                nearestRank(sorted, 90),
                nearestRank(sorted, 95),
                sorted[sorted.length - 1],
                sorted.length
            );
        }

        private static double nearestRank(double[] sorted, int percentile) {
            int rank = (int) Math.ceil(percentile / 100.0 * sorted.length) - 1;
            return sorted[Math.min(Math.max(rank, 0), sorted.length - 1)];
        }

        DoubleStats(StreamInput in) throws IOException {
            this(
                in.readDouble(),
                in.readDouble(),
                in.readDouble(),
                in.readDouble(),
                in.readDouble(),
                in.readDouble(),
                in.readDouble(),
                in.readVLong()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(mean);
            out.writeDouble(min);
            out.writeDouble(p10);
            out.writeDouble(p50);
            out.writeDouble(p90);
            out.writeDouble(p95);
            out.writeDouble(max);
            out.writeVLong(count);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MEAN_FIELD.getPreferredName(), mean);
            builder.field(MIN_FIELD.getPreferredName(), min);
            builder.field(P10_FIELD.getPreferredName(), p10);
            builder.field(P50_FIELD.getPreferredName(), p50);
            builder.field(P90_FIELD.getPreferredName(), p90);
            builder.field(P95_FIELD.getPreferredName(), p95);
            builder.field(MAX_FIELD.getPreferredName(), max);
            builder.field(COUNT_FIELD.getPreferredName(), count);
            builder.endObject();
            return builder;
        }
    }

    /**
     * Distribution of one per-search measurement over all the searches behind a single configuration. Used for both {@code took_ms} and
     * {@code vector_ops}, which have to be read together with the recall figure to make sense of a sweep.
     *
     * @param count how many searches contributed; searches that failed are not counted
     */
    public record LongStats(double mean, long p50, long p95, long max, long sum, long count) implements Writeable, ToXContentObject {

        static final ParseField MEAN_FIELD = new ParseField("mean");
        static final ParseField P50_FIELD = new ParseField("p50");
        static final ParseField P95_FIELD = new ParseField("p95");
        static final ParseField MAX_FIELD = new ParseField("max");
        static final ParseField SUM_FIELD = new ParseField("sum");
        static final ParseField COUNT_FIELD = new ParseField("count");

        public static final LongStats EMPTY = new LongStats(0.0, 0, 0, 0, 0, 0);

        /**
         * Summarises the collected values. Percentiles use nearest-rank over a sorted copy rather than a sketch: the sample size here is
         * the number of queries in one request, which is small enough that exactness is cheaper than approximation.
         */
        public static LongStats of(List<Long> values) {
            if (values.isEmpty()) {
                return EMPTY;
            }
            long[] sorted = values.stream().mapToLong(Long::longValue).sorted().toArray();
            long sum = 0;
            for (long value : sorted) {
                sum += value;
            }
            return new LongStats(
                (double) sum / sorted.length,
                nearestRank(sorted, 50),
                nearestRank(sorted, 95),
                sorted[sorted.length - 1],
                sum,
                sorted.length
            );
        }

        private static long nearestRank(long[] sorted, int percentile) {
            int rank = (int) Math.ceil(percentile / 100.0 * sorted.length) - 1;
            return sorted[Math.min(Math.max(rank, 0), sorted.length - 1)];
        }

        LongStats(StreamInput in) throws IOException {
            this(in.readDouble(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(mean);
            out.writeVLong(p50);
            out.writeVLong(p95);
            out.writeVLong(max);
            out.writeVLong(sum);
            out.writeVLong(count);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MEAN_FIELD.getPreferredName(), mean);
            builder.field(P50_FIELD.getPreferredName(), p50);
            builder.field(P95_FIELD.getPreferredName(), p95);
            builder.field(MAX_FIELD.getPreferredName(), max);
            builder.field(SUM_FIELD.getPreferredName(), sum);
            builder.field(COUNT_FIELD.getPreferredName(), count);
            builder.endObject();
            return builder;
        }
    }

    /**
     * One knob set as it is echoed back, with what the search will actually do.
     *
     * @param effectiveNumCandidates the candidate window the shard will collect, or {@code null} when the field mapping could not be
     *                               read ({@code view_index_metadata} is what enables these two)
     * @param rescoreWindow          how many candidates get rescored on the real vectors; 0 when nothing is rescored
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

    /**
     * A document with the rank the baseline gave it. Used for both sides of a per-query detail: a candidate hit, where the rank may be
     * absent, and a missed baseline hit, where it never is.
     *
     * @param baselineRank the 0-based position of this document in the baseline's top-k, or {@code null} if the baseline did not return
     *                     it at all -- which is precisely the definition of a false positive for this metric
     */
    public record RankedHit(String id, float score, @Nullable Integer baselineRank) implements Writeable, ToXContentObject {

        static final ParseField BASELINE_RANK_FIELD = new ParseField("baseline_rank");

        RankedHit(StreamInput in) throws IOException {
            this(in.readString(), in.readFloat(), in.readOptionalVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            out.writeFloat(score);
            out.writeOptionalVInt(baselineRank);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Hit.ID_FIELD.getPreferredName(), id);
            builder.field(Hit.SCORE_FIELD.getPreferredName(), score);
            if (baselineRank == null) {
                builder.nullField(BASELINE_RANK_FIELD.getPreferredName());
            } else {
                builder.field(BASELINE_RANK_FIELD.getPreferredName(), baselineRank);
            }
            builder.endObject();
            return builder;
        }
    }

    /** A single returned document, reduced to the only two things recall estimation cares about. */
    public record Hit(String id, float score) implements Writeable, ToXContentObject {

        static final ParseField ID_FIELD = new ParseField("_id");
        static final ParseField SCORE_FIELD = new ParseField("_score");

        Hit(StreamInput in) throws IOException {
            this(in.readString(), in.readFloat());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            out.writeFloat(score);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(ID_FIELD.getPreferredName(), id);
            builder.field(SCORE_FIELD.getPreferredName(), score);
            builder.endObject();
            return builder;
        }
    }
}
