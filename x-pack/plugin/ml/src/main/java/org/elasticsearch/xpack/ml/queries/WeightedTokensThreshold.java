/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.queries;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class WeightedTokensThreshold implements Writeable, ToXContentObject {
    public static final ParseField TOKENS_THRESHOLD_FIELD = new ParseField("tokens_threshold");
    public static final ParseField RATIO_THRESHOLD_FIELD = new ParseField("ratio_threshold");
    public static final ParseField WEIGHT_THRESHOLD_FIELD = new ParseField("weight_threshold");
    public static final ParseField ONLY_SCORE_PRUNED_TOKENS_FIELD = new ParseField("only_score_pruned_tokens");

    public static final float DEFAULT_RATIO_THRESHOLD = 5;
    public static final float MAX_RATIO_THRESHOLD = 100;
    public static final float DEFAULT_WEIGHT_THRESHOLD = 0.4f;

    private final float ratioThreshold;
    private final float weightThreshold;
    private final boolean onlyScorePrunedTokens;

    public WeightedTokensThreshold() {
        this(DEFAULT_RATIO_THRESHOLD, DEFAULT_WEIGHT_THRESHOLD, false);
    }

    public WeightedTokensThreshold(float ratioThreshold, float weightThreshold, boolean onlyScorePrunedTokens) {
        if (ratioThreshold < 1 || ratioThreshold > MAX_RATIO_THRESHOLD) {
            throw new IllegalArgumentException(
                "["
                    + RATIO_THRESHOLD_FIELD.getPreferredName()
                    + "] must be between [1.0] and ["
                    + MAX_RATIO_THRESHOLD
                    + "], got "
                    + ratioThreshold
            );
        }
        if (weightThreshold < 0 || weightThreshold > 1) {
            throw new IllegalArgumentException("[" + WEIGHT_THRESHOLD_FIELD.getPreferredName() + "] must be between 0 and 1");
        }
        this.ratioThreshold = ratioThreshold;
        this.weightThreshold = weightThreshold;
        this.onlyScorePrunedTokens = onlyScorePrunedTokens;
    }

    public WeightedTokensThreshold(StreamInput in) throws IOException {
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
        WeightedTokensThreshold that = (WeightedTokensThreshold) o;
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
        builder.startObject(TOKENS_THRESHOLD_FIELD.getPreferredName());
        builder.field(RATIO_THRESHOLD_FIELD.getPreferredName(), ratioThreshold);
        builder.field(WEIGHT_THRESHOLD_FIELD.getPreferredName(), weightThreshold);
        if (onlyScorePrunedTokens) {
            builder.field(ONLY_SCORE_PRUNED_TOKENS_FIELD.getPreferredName(), onlyScorePrunedTokens);
        }
        builder.endObject();
        return builder;
    }

    public static WeightedTokensThreshold fromXContent(XContentParser parser) throws IOException {
        String currentFieldName = null;
        XContentParser.Token token;
        float ratioThreshold = DEFAULT_RATIO_THRESHOLD;
        float weightThreshold = DEFAULT_WEIGHT_THRESHOLD;
        boolean onlyScorePrunedTokens = false;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (RATIO_THRESHOLD_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    ratioThreshold = parser.intValue();
                } else if (WEIGHT_THRESHOLD_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    weightThreshold = parser.floatValue();
                } else if (ONLY_SCORE_PRUNED_TOKENS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    onlyScorePrunedTokens = parser.booleanValue();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + TOKENS_THRESHOLD_FIELD.getPreferredName() + "] does not support [" + currentFieldName + "]"
                    );
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[" + TOKENS_THRESHOLD_FIELD.getPreferredName() + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                );
            }
        }
        return new WeightedTokensThreshold(ratioThreshold, weightThreshold, onlyScorePrunedTokens);
    }
}
