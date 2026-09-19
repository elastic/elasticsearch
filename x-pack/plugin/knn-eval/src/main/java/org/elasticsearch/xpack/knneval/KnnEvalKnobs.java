/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.RescoreVectorBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/** Search settings varied between evaluation runs. A {@code null} value preserves the kNN search default. */
record KnnEvalKnobs(@Nullable Float visitPercentage, @Nullable Integer numCandidates, @Nullable Float rescoreOversample, boolean exact)
    implements
        Writeable,
        ToXContentObject {

    static final ParseField VISIT_PERCENTAGE_FIELD = KnnSearchBuilder.VISIT_PERCENTAGE_FIELD;
    static final ParseField NUM_CANDIDATES_FIELD = KnnSearchBuilder.NUM_CANDS_FIELD;
    static final ParseField RESCORE_VECTOR_FIELD = KnnSearchBuilder.RESCORE_VECTOR_FIELD;
    static final ParseField EXACT_FIELD = new ParseField("exact");

    private static final ConstructingObjectParser<KnnEvalKnobs, Void> PARSER = new ConstructingObjectParser<>(
        "knn_eval_knobs",
        args -> new KnnEvalKnobs(
            (Float) args[0],
            (Integer) args[1],
            args[2] == null ? null : ((RescoreVectorBuilder) args[2]).oversample(),
            args[3] != null && (Boolean) args[3]
        )
    );

    static {
        PARSER.declareFloat(ConstructingObjectParser.optionalConstructorArg(), VISIT_PERCENTAGE_FIELD);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), NUM_CANDIDATES_FIELD);
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> RescoreVectorBuilder.fromXContent(p),
            RESCORE_VECTOR_FIELD
        );
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), EXACT_FIELD);
    }

    /** Creates and validates one baseline or candidate configuration. */
    KnnEvalKnobs {
        if (visitPercentage != null && (visitPercentage < 0.0f || visitPercentage > 100.0f)) {
            throw new IllegalArgumentException("[" + VISIT_PERCENTAGE_FIELD.getPreferredName() + "] must be between 0.0 and 100.0");
        }
        if (numCandidates != null && numCandidates < 1) {
            throw new IllegalArgumentException("[" + NUM_CANDIDATES_FIELD.getPreferredName() + "] must be greater than 0");
        }
        if (rescoreOversample != null && rescoreOversample < RescoreVectorBuilder.MIN_OVERSAMPLE) {
            // RescoreVectorBuilder also accepts 0 ("no rescoring"), but a run scoring quantized estimates is no use as a reference
            throw new IllegalArgumentException(
                "["
                    + RESCORE_VECTOR_FIELD.getPreferredName()
                    + "."
                    + RescoreVectorBuilder.OVERSAMPLE_FIELD.getPreferredName()
                    + "] must be at least "
                    + RescoreVectorBuilder.MIN_OVERSAMPLE
            );
        }
        if (exact && (visitPercentage != null || numCandidates != null || rescoreOversample != null)) {
            throw new IllegalArgumentException(
                "["
                    + EXACT_FIELD.getPreferredName()
                    + "] cannot be combined with ["
                    + VISIT_PERCENTAGE_FIELD.getPreferredName()
                    + "], ["
                    + NUM_CANDIDATES_FIELD.getPreferredName()
                    + "] or ["
                    + RESCORE_VECTOR_FIELD.getPreferredName()
                    + "]"
            );
        }
    }

    KnnEvalKnobs(StreamInput in) throws IOException {
        this(in.readOptionalFloat(), in.readOptionalVInt(), in.readOptionalFloat(), in.readBoolean());
    }

    static KnnEvalKnobs fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /**
     * Percentage of DiskBBQ postings to visit. An explicit {@code 0} selects the codec's automatic visit calculation; it does not mean
     * that no vectors are visited.
     */
    @Nullable
    public Float getVisitPercentage() {
        return visitPercentage;
    }

    @Nullable
    public Integer getNumCandidates() {
        return numCandidates;
    }

    /** Returns the oversampling factor, or {@code null} to preserve the mapping setting. */
    @Nullable
    public Float getRescoreOversample() {
        return rescoreOversample;
    }

    /** Whether the baseline scores every vector exactly. */
    public boolean isExact() {
        return exact;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalFloat(visitPercentage);
        out.writeOptionalVInt(numCandidates);
        out.writeOptionalFloat(rescoreOversample);
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
        if (rescoreOversample != null) {
            builder.field(RESCORE_VECTOR_FIELD.getPreferredName(), new RescoreVectorBuilder(rescoreOversample));
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

}
