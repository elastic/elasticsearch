/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.queries;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class WeightedTokenThreshold implements Writeable, ToXContentFragment {
    public static final ParseField RATIO_THRESHOLD_FIELD = new ParseField("ratio_threshold");
    public static final ParseField WEIGHT_THRESHOLD_FIELD = new ParseField("weight_threshold");
    public static final ParseField ONLY_SCORE_PRUNED_TOKENS_FIELD = new ParseField("only_score_pruned_tokens");

    private final float ratioThreshold;
    private final float weightThreshold;
    private final boolean onlyScorePrunedTokens;

    public WeightedTokenThreshold(float ratioThreshold, float weightThreshold, boolean onlyScorePrunedTokens) {
        if (ratioThreshold < 1) {
            throw new IllegalArgumentException(
                "[" + RATIO_THRESHOLD_FIELD.getPreferredName() + "] must be greater or equal to 1, got " + ratioThreshold
            );
        }
        if (weightThreshold < 0 || weightThreshold > 1) {
            throw new IllegalArgumentException("[" + WEIGHT_THRESHOLD_FIELD.getPreferredName() + "] must be between 0 and 1");
        }
        this.ratioThreshold = ratioThreshold;
        this.weightThreshold = weightThreshold;
        this.onlyScorePrunedTokens = onlyScorePrunedTokens;
    }

    public WeightedTokenThreshold(StreamInput in) throws IOException {
        this.ratioThreshold = in.readFloat();
        this.weightThreshold = in.readFloat();
        this.onlyScorePrunedTokens = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeFloat(ratioThreshold);
        out.writeFloat(weightThreshold);
        out.writeBoolean(onlyScorePrunedTokens);
    }

    /**
     * Returns the frequency ratio threshold to apply on the query.
     * Tokens whose frequency is more than ratio_threshold times the average frequency of all tokens in the specified
     * field are considered outliers and may be subject to removal from the query.
     */
    public float getRatioThreshold() {
        return ratioThreshold;
    }

    /**
     * Returns the weight threshold to apply on the query.
     * Tokens whose weight is more than (weightThreshold * best_weight) of the highest weight in the query are not
     * considered outliers, even if their frequency exceeds the specified ratio_threshold.
     * This threshold ensures that important tokens, as indicated by their weight, are retained in the query.
     */
    public float getWeightThreshold() {
        return weightThreshold;
    }

    /**
     * Returns whether the filtering process retains tokens identified as non-relevant based on the specified thresholds
     * (ratio and weight). When {@code true}, only non-relevant tokens are considered for matching and scoring documents.
     * Enabling this option is valuable for re-scoring top hits retrieved from a {@link WeightedTokensQueryBuilder} with
     * active thresholds.
     */
    public boolean isOnlyScorePrunedTokens() {
        return onlyScorePrunedTokens;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WeightedTokenThreshold that = (WeightedTokenThreshold) o;
        return Float.compare(that.ratioThreshold, ratioThreshold) == 0
            && Float.compare(that.weightThreshold, weightThreshold) == 0
            && onlyScorePrunedTokens == that.onlyScorePrunedTokens;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ratioThreshold, weightThreshold, onlyScorePrunedTokens);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(RATIO_THRESHOLD_FIELD.getPreferredName(), ratioThreshold);
        builder.field(WEIGHT_THRESHOLD_FIELD.getPreferredName(), weightThreshold);
        if (onlyScorePrunedTokens) {
            builder.field(ONLY_SCORE_PRUNED_TOKENS_FIELD.getPreferredName(), onlyScorePrunedTokens);
        }
        return builder;
    }
}
