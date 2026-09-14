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
 * Everything that distinguishes one {@code _knn_eval} run from another, in one object so that two runs are provably identical in every
 * other respect. A {@code null} knob means "let the kNN query apply its own default".
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
            // RescoreVectorBuilder also accepts 0 ("no rescoring"), but a run scoring quantized estimates is no use as a reference
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
     * Rescore window of {@code oversample * k} quantized candidates; the mapping's default (3) drops true top-k documents whose
     * quantized rank falls outside it, so a reference run wants a larger value. Unset applies the mapping's setting.
     */
    @Nullable
    public Float getOversample() {
        return oversample;
    }

    /**
     * Score every document with a vector, giving true ground truth. Baseline only and on its own, since the approximate knobs have
     * nothing to tune. Costs O(N x dims x 4 bytes) per query -- about 95 ms over 500k 1024-d vectors in cache, seconds off disk -- so
     * pair it with a small {@code sample.size}.
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

    /** Without the enclosing object, so a response can render these alongside derived fields. */
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
