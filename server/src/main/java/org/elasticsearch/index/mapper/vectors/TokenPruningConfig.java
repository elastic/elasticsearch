/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.MapXContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TokenPruningConfig implements Writeable, ToXContentObject {
    public static final String PRUNING_CONFIG_FIELD = "pruning_config";

    public static final ParseField TOKENS_FREQ_RATIO_THRESHOLD = new ParseField("tokens_freq_ratio_threshold");
    public static final ParseField TOKENS_WEIGHT_THRESHOLD = new ParseField("tokens_weight_threshold");
    public static final ParseField ONLY_SCORE_PRUNED_TOKENS_FIELD = new ParseField("only_score_pruned_tokens");

    // Tokens whose frequency is more than 5 times the average frequency of all tokens in the specified field are considered outliers.
    public static final float DEFAULT_TOKENS_FREQ_RATIO_THRESHOLD = 5;
    public static final float MIN_TOKENS_FREQ_RATIO_THRESHOLD = 1;
    public static final float MAX_TOKENS_FREQ_RATIO_THRESHOLD = 100;

    // A token's weight should be > 40% of the best weight in the query to be considered significant.
    public static final float DEFAULT_TOKENS_WEIGHT_THRESHOLD = 0.4f;
    public static final float MIN_TOKENS_WEIGHT_THRESHOLD = 0.0f;
    public static final float MAX_TOKENS_WEIGHT_THRESHOLD = 1.0f;

    private final float tokensFreqRatioThreshold;
    private final float tokensWeightThreshold;
    private final boolean onlyScorePrunedTokens;

    public TokenPruningConfig() {
        this(DEFAULT_TOKENS_FREQ_RATIO_THRESHOLD, DEFAULT_TOKENS_WEIGHT_THRESHOLD, false);
    }

    public TokenPruningConfig(float tokensFreqRatioThreshold, float tokensWeightThreshold, boolean onlyScorePrunedTokens) {
        if (tokensFreqRatioThreshold < 1 || tokensFreqRatioThreshold > MAX_TOKENS_FREQ_RATIO_THRESHOLD) {
            throw new IllegalArgumentException(
                "["
                    + TOKENS_FREQ_RATIO_THRESHOLD.getPreferredName()
                    + "] must be between [1] and ["
                    + String.format(Locale.ROOT, "%d", (int) MAX_TOKENS_FREQ_RATIO_THRESHOLD)
                    + "], got "
                    + tokensFreqRatioThreshold
            );
        }
        if (tokensWeightThreshold < 0 || tokensWeightThreshold > 1) {
            throw new IllegalArgumentException("[" + TOKENS_WEIGHT_THRESHOLD.getPreferredName() + "] must be between 0 and 1");
        }
        this.tokensFreqRatioThreshold = tokensFreqRatioThreshold;
        this.tokensWeightThreshold = tokensWeightThreshold;
        this.onlyScorePrunedTokens = onlyScorePrunedTokens;
    }

    public TokenPruningConfig(StreamInput in) throws IOException {
        this.tokensFreqRatioThreshold = in.readFloat();
        this.tokensWeightThreshold = in.readFloat();
        this.onlyScorePrunedTokens = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeFloat(tokensFreqRatioThreshold);
        out.writeFloat(tokensWeightThreshold);
        out.writeBoolean(onlyScorePrunedTokens);
    }

    /**
     * Returns the frequency ratio threshold to apply on the query.
     * Tokens whose frequency is more than ratio_threshold times the average frequency of all tokens in the specified
     * field are considered outliers and may be subject to removal from the query.
     */
    public float getTokensFreqRatioThreshold() {
        return tokensFreqRatioThreshold;
    }

    /**
     * Returns the weight threshold to apply on the query.
     * Tokens whose weight is more than (weightThreshold * best_weight) of the highest weight in the query are not
     * considered outliers, even if their frequency exceeds the specified ratio_threshold.
     * This threshold ensures that important tokens, as indicated by their weight, are retained in the query.
     */
    public float getTokensWeightThreshold() {
        return tokensWeightThreshold;
    }

    /**
     * Returns whether the filtering process retains tokens identified as non-relevant based on the specified thresholds
     * (ratio and weight). When {@code true}, only non-relevant tokens are considered for matching and scoring documents.
     * Enabling this option is valuable for re-scoring top hits retrieved from another {@link QueryBuilder} with
     * active thresholds.
     */
    public boolean isOnlyScorePrunedTokens() {
        return onlyScorePrunedTokens;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TokenPruningConfig that = (TokenPruningConfig) o;
        return Float.compare(that.tokensFreqRatioThreshold, tokensFreqRatioThreshold) == 0
            && Float.compare(that.tokensWeightThreshold, tokensWeightThreshold) == 0
            && onlyScorePrunedTokens == that.onlyScorePrunedTokens;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tokensFreqRatioThreshold, tokensWeightThreshold, onlyScorePrunedTokens);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TOKENS_FREQ_RATIO_THRESHOLD.getPreferredName(), tokensFreqRatioThreshold);
        builder.field(TOKENS_WEIGHT_THRESHOLD.getPreferredName(), tokensWeightThreshold);
        if (onlyScorePrunedTokens) {
            builder.field(ONLY_SCORE_PRUNED_TOKENS_FIELD.getPreferredName(), onlyScorePrunedTokens);
        }
        builder.endObject();
        return builder;
    }

    public static TokenPruningConfig fromXContent(XContentParser parser) throws IOException {
        String currentFieldName = null;
        XContentParser.Token token;
        float ratioThreshold = DEFAULT_TOKENS_FREQ_RATIO_THRESHOLD;
        float weightThreshold = DEFAULT_TOKENS_WEIGHT_THRESHOLD;
        boolean onlyScorePrunedTokens = false;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.START_OBJECT) {
                continue;
            }
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                if (Set.of(
                    TOKENS_FREQ_RATIO_THRESHOLD.getPreferredName(),
                    TOKENS_WEIGHT_THRESHOLD.getPreferredName(),
                    ONLY_SCORE_PRUNED_TOKENS_FIELD.getPreferredName()
                ).contains(currentFieldName) == false) {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + PRUNING_CONFIG_FIELD + "] unknown token [" + currentFieldName + "]"
                    );
                }
            } else if (token.isValue()) {
                if (TOKENS_FREQ_RATIO_THRESHOLD.match(currentFieldName, parser.getDeprecationHandler())) {
                    ratioThreshold = parser.intValue();
                } else if (TOKENS_WEIGHT_THRESHOLD.match(currentFieldName, parser.getDeprecationHandler())) {
                    weightThreshold = parser.floatValue();
                } else if (ONLY_SCORE_PRUNED_TOKENS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    onlyScorePrunedTokens = parser.booleanValue();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + PRUNING_CONFIG_FIELD + "] does not support [" + currentFieldName + "]"
                    );
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[" + PRUNING_CONFIG_FIELD + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                );
            }
        }
        return new TokenPruningConfig(ratioThreshold, weightThreshold, onlyScorePrunedTokens);
    }

    public static final ConstructingObjectParser<TokenPruningConfig, Void> PARSER = new ConstructingObjectParser<>(
        PRUNING_CONFIG_FIELD,
        args -> new TokenPruningConfig(
            args[0] == null ? DEFAULT_TOKENS_FREQ_RATIO_THRESHOLD : (Float) args[0],
            args[1] == null ? DEFAULT_TOKENS_WEIGHT_THRESHOLD : (Float) args[1],
            args[2] != null && (Boolean) args[2]
        )
    );

    static {
        PARSER.declareFloat(optionalConstructorArg(), TOKENS_FREQ_RATIO_THRESHOLD);
        PARSER.declareFloat(optionalConstructorArg(), TOKENS_WEIGHT_THRESHOLD);
        PARSER.declareBoolean(optionalConstructorArg(), ONLY_SCORE_PRUNED_TOKENS_FIELD);
    }

    public static TokenPruningConfig parseFromMap(Map<String, Object> pruningConfigMap) {
        if (pruningConfigMap == null) {
            return null;
        }

        try {
            XContentParser parser = new MapXContentParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                pruningConfigMap,
                XContentType.JSON
            );

            return PARSER.parse(parser, null);
        } catch (IOException ioEx) {
            throw new UncheckedIOException(ioEx);
        }
    }
}
