/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.queries;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults.WeightedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.ml.queries.WeightedTokensThreshold.*;

public class WeightedTokensQueryBuilder extends AbstractQueryBuilder<WeightedTokensQueryBuilder> {
    public static final String NAME = "weighted_tokens";

    public static final ParseField TOKENS_FIELD = new ParseField("tokens");
    private final String fieldName;
    private final List<WeightedToken> tokens;
    private final WeightedTokensThreshold threshold;

    public WeightedTokensQueryBuilder(String fieldName, List<WeightedToken> tokens) {
        this(fieldName, tokens, null);
    }

    public WeightedTokensQueryBuilder(String fieldName, List<WeightedToken> tokens, @Nullable WeightedTokensThreshold threshold) {
        this.fieldName = Objects.requireNonNull(fieldName, "[" + NAME + "] requires a fieldName");
        this.tokens = Objects.requireNonNull(tokens, "[" + NAME + "] requires tokens");
        this.threshold = threshold;
    }

    public WeightedTokensQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.tokens = in.readCollectionAsList(WeightedToken::new);
        this.threshold = in.readOptionalWriteable(WeightedTokensThreshold::new);
    }

    public String getFieldName() {
        return fieldName;
    }

    @Nullable
    public WeightedTokensThreshold getThreshold() {
        return threshold;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeCollection(tokens);
        out.writeOptionalWriteable(threshold);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(TOKENS_FIELD.getPreferredName(), tokens);
        threshold.toXContent(builder, params);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
        builder.endObject();
    }

    private float getAverageTokenFreqRatio(IndexReader reader, int fieldDocCount) throws IOException {
        int numUniqueTokens = 0;
        for (var leaf : reader.getContext().leaves()) {
            var terms = leaf.reader().terms(fieldName);
            if (terms != null) {
                numUniqueTokens = (int) Math.max(terms.size(), numUniqueTokens);
            }
        }
        if (numUniqueTokens == 0) {
            return 0;
        }
        return (float) reader.getSumDocFreq(fieldName) / fieldDocCount / numUniqueTokens;
    }

    /**
     * Returns true if the token should be queried based on the {@code ratioThreshold} and {@code weightThreshold}
     * set on the query.
     */
    private boolean shouldKeepToken(
        IndexReader reader,
        WeightedToken token,
        int fieldDocCount,
        float averageTokenFreqRatio,
        float bestWeight
    ) throws IOException {
        if (threshold == null) {
            return true;
        }
        int docFreq = reader.docFreq(new Term(fieldName, token.token()));
        if (docFreq == 0) {
            return false;
        }
        float tokenFreqRatio = (float) docFreq / fieldDocCount;
        return tokenFreqRatio < threshold.getRatioThreshold() * averageTokenFreqRatio
            || token.weight() > threshold.getWeightThreshold() * bestWeight;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        final MappedFieldType ft = context.getFieldType(fieldName);
        if (ft == null) {
            return new MatchNoDocsQuery("The \"" + getName() + "\" query is against a field that does not exist");
        }
        var qb = new BooleanQuery.Builder();
        int fieldDocCount = context.getIndexReader().getDocCount(fieldName);
        float bestWeight = 0f;
        for (var t : tokens) {
            bestWeight = Math.max(t.weight(), bestWeight);
        }
        float averageTokenFreqRatio = getAverageTokenFreqRatio(context.getIndexReader(), fieldDocCount);
        if (averageTokenFreqRatio == 0) {
            return new MatchNoDocsQuery("The \"" + getName() + "\" query is against an empty field");
        }
        for (var token : tokens) {
            boolean keep = shouldKeepToken(context.getIndexReader(), token, fieldDocCount, averageTokenFreqRatio, bestWeight) ^ threshold
                .isOnlyScorePrunedTokens();
            if (keep) {
                qb.add(new BoostQuery(ft.termQuery(token.token(), context), token.weight()), BooleanClause.Occur.SHOULD);
            }
        }
        return qb.build();
    }

    @Override
    protected boolean doEquals(WeightedTokensQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) && Objects.equals(threshold, other.threshold) && tokens.equals(other.tokens);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, tokens, threshold);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.WEIGHTED_TOKENS_QUERY_ADDED;
    }

    private static float parseWeight(String token, Object weight) {
        if (weight instanceof Number asNumber) {
            return asNumber.floatValue();
        }
        if (weight instanceof String asString) {
            return Float.valueOf(asString);
        }
        throw new IllegalArgumentException(
            "Illegal weight for token: [" + token + "], expected floating point got " + weight.getClass().getSimpleName()
        );
    }

    public static WeightedTokensQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String currentFieldName = null;
        String fieldName = null;
        List<WeightedToken> tokens = new ArrayList<>();
        Integer ratioThreshold = null;
        Float weightThreshold = 1f;
        boolean onlyScorePrunedTokens = false;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (RATIO_THRESHOLD_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        ratioThreshold = parser.intValue();
                    } else if (WEIGHT_THRESHOLD_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        weightThreshold = parser.floatValue();
                    } else if (TOKENS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        var tokensMap = parser.map();
                        for (var e : tokensMap.entrySet()) {
                            tokens.add(new WeightedToken(e.getKey(), parseWeight(e.getKey(), e.getValue())));
                        }
                    } else if (ONLY_SCORE_PRUNED_TOKENS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        onlyScorePrunedTokens = parser.booleanValue();
                    } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        boost = parser.floatValue();
                    } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        queryName = parser.text();
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "[" + NAME + "] query does not support [" + currentFieldName + "]"
                        );
                    }
                }
            } else {
                throw new IllegalArgumentException("invalid query");
            }
        }

        if (fieldName == null) {
            throw new ParsingException(parser.getTokenLocation(), "No fieldname specified for query");
        }

        var qb = new WeightedTokensQueryBuilder(
            fieldName,
            tokens,
            new WeightedTokensThreshold(ratioThreshold, weightThreshold, onlyScorePrunedTokens)
        );
        qb.queryName(queryName);
        qb.boost(boost);
        return qb;
    }
}
