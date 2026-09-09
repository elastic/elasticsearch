/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.vectors.RescoreVectorBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * The set of approximate-search knobs that one {@code _knn_eval} run varies.
 * <p>
 * Recall estimation without brute-force ground truth works by running the <em>same</em> query vector against the <em>same</em> field
 * twice and only changing how much of the index the ANN search is allowed to look at. Everything that distinguishes the "baseline" run
 * from a "candidate" run therefore has to live in one small, self-contained object so that the two runs are provably identical in every
 * other respect. This class is that object.
 * <p>
 * Both knobs are optional; a {@code null} knob means "let the underlying kNN query apply its own default", which is what makes an
 * all-defaults candidate directly comparable to an all-defaults baseline.
 *
 * @see KnnEvalSpec
 */
public class KnnEvalKnobs implements Writeable, ToXContentObject {

    static final ParseField VISIT_PERCENTAGE_FIELD = new ParseField("visit_percentage");
    static final ParseField NUM_CANDIDATES_FIELD = new ParseField("num_candidates");
    static final ParseField OVERSAMPLE_FIELD = new ParseField("oversample");
    static final ParseField EXACT_FIELD = new ParseField("exact");

    private static final ConstructingObjectParser<KnnEvalKnobs, Void> PARSER = new ConstructingObjectParser<>(
        "knn_eval_knobs",
        args -> new KnnEvalKnobs((Float) args[0], (Integer) args[1], (Float) args[2], args[3] != null && (Boolean) args[3])
    );

    static {
        PARSER.declareFloat(ConstructingObjectParser.optionalConstructorArg(), VISIT_PERCENTAGE_FIELD);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), NUM_CANDIDATES_FIELD);
        PARSER.declareFloat(ConstructingObjectParser.optionalConstructorArg(), OVERSAMPLE_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), EXACT_FIELD);
    }

    @Nullable
    private final Float visitPercentage;
    @Nullable
    private final Integer numCandidates;
    @Nullable
    private final Float oversample;
    private final boolean exact;

    public KnnEvalKnobs(@Nullable Float visitPercentage, @Nullable Integer numCandidates, @Nullable Float oversample, boolean exact) {
        if (visitPercentage != null && (visitPercentage < 0.0f || visitPercentage > 100.0f)) {
            throw new IllegalArgumentException("[" + VISIT_PERCENTAGE_FIELD.getPreferredName() + "] must be between 0.0 and 100.0");
        }
        if (numCandidates != null && numCandidates < 1) {
            throw new IllegalArgumentException("[" + NUM_CANDIDATES_FIELD.getPreferredName() + "] must be greater than 0");
        }
        if (oversample != null && oversample < RescoreVectorBuilder.MIN_OVERSAMPLE) {
            // RescoreVectorBuilder also accepts exactly 0, meaning "no rescoring", but a run whose scores are quantized estimates is
            // not a useful reference, so the knob only takes real oversamples.
            throw new IllegalArgumentException(
                "[" + OVERSAMPLE_FIELD.getPreferredName() + "] must be at least " + RescoreVectorBuilder.MIN_OVERSAMPLE
            );
        }
        this.visitPercentage = visitPercentage;
        this.numCandidates = numCandidates;
        this.oversample = oversample;
        this.exact = exact;
    }

    KnnEvalKnobs(StreamInput in) throws IOException {
        this(in.readOptionalFloat(), in.readOptionalVInt(), in.readOptionalFloat(), in.readBoolean());
    }

    static KnnEvalKnobs fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Nullable
    public Float getVisitPercentage() {
        return visitPercentage;
    }

    @Nullable
    public Integer getNumCandidates() {
        return numCandidates;
    }

    /**
     * Rescoring factor for this run: the approximate search collects {@code oversample * k} candidates on the quantized vectors and then
     * rescores them on the real ones.
     * <p>
     * This is what lets a baseline be exact ground truth without a brute-force script_score. The mapping's default of 3 is not enough:
     * on a 2000-query sweep a 100%-visit baseline at oversample 3 missed 31 of the exact top-100 documents, because the quantized top-300
     * rescore window can drop a document whose true rank is inside k. At oversample 10 it missed none, so
     * {@code {"visit_percentage": 100, "oversample": 10}} is a practical exact reference. Left unset, the field mapping's own setting
     * applies.
     */
    @Nullable
    public Float getOversample() {
        return oversample;
    }

    /**
     * Score every document with a vector rather than searching approximately, producing true ground truth for certification runs.
     * <p>
     * Only a baseline may do this, and only on its own -- the approximate knobs have nothing to tune when nothing is approximated. The
     * cost is O(N x dims x 4 bytes) per query: roughly 95 ms per query over 500k 1024-dimensional vectors already in the page cache, and
     * seconds per query once 10M vectors have to come off disk. Pair it with a small {@code sample.size}.
     */
    public boolean isExact() {
        return exact;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalFloat(visitPercentage);
        out.writeOptionalVInt(numCandidates);
        out.writeOptionalFloat(oversample);
        out.writeBoolean(exact);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    /** The knob fields without their enclosing object, so that a response can render them alongside derived ones. */
    XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        if (visitPercentage != null) {
            builder.field(VISIT_PERCENTAGE_FIELD.getPreferredName(), visitPercentage);
        }
        if (numCandidates != null) {
            builder.field(NUM_CANDIDATES_FIELD.getPreferredName(), numCandidates);
        }
        if (oversample != null) {
            builder.field(OVERSAMPLE_FIELD.getPreferredName(), oversample);
        }
        if (exact) {
            builder.field(EXACT_FIELD.getPreferredName(), true);
        }
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        KnnEvalKnobs other = (KnnEvalKnobs) obj;
        return Objects.equals(visitPercentage, other.visitPercentage)
            && Objects.equals(numCandidates, other.numCandidates)
            && Objects.equals(oversample, other.oversample)
            && exact == other.exact;
    }

    @Override
    public int hashCode() {
        return Objects.hash(visitPercentage, numCandidates, oversample, exact);
    }
}
