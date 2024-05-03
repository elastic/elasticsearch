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
import org.elasticsearch.ElasticsearchParseException;
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

import static org.elasticsearch.xpack.ml.queries.TextExpansionQueryBuilder.AllowedFieldType;
import static org.elasticsearch.xpack.ml.queries.TextExpansionQueryBuilder.PRUNING_CONFIG;

public class WeightedTokensQueryBuilder extends AbstractQueryBuilder<WeightedTokensQueryBuilder> {
    public static final String NAME = "weighted_tokens";

    public static final ParseField TOKENS_FIELD = new ParseField("tokens");
    private final String fieldName;
    private final List<WeightedToken> tokens;
    @Nullable
    private final TokenPruningConfig tokenPruningConfig;

    public WeightedTokensQueryBuilder(String fieldName, List<WeightedToken> tokens) {
        this(fieldName, tokens, null);
    }

    public WeightedTokensQueryBuilder(String fieldName, List<WeightedToken> tokens, @Nullable TokenPruningConfig tokenPruningConfig) {
        this.fieldName = Objects.requireNonNull(fieldName, "[" + NAME + "] requires a fieldName");
        this.tokens = Objects.requireNonNull(tokens, "[" + NAME + "] requires tokens");
        if (tokens.isEmpty()) {
            throw new IllegalArgumentException("[" + NAME + "] requires at least one token");
        }
        this.tokenPruningConfig = tokenPruningConfig;
    }

    public WeightedTokensQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.tokens = in.readCollectionAsList(WeightedToken::new);
        this.tokenPruningConfig = in.readOptionalWriteable(TokenPruningConfig::new);
    }

    public String getFieldName() {
        return fieldName;
    }

    @Nullable
    public TokenPruningConfig getTokenPruningConfig() {
        return tokenPruningConfig;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeCollection(tokens);
        out.writeOptionalWriteable(tokenPruningConfig);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.startObject(TOKENS_FIELD.getPreferredName());
        for (var token : tokens) {
            token.toXContent(builder, params);
        }
        builder.endObject();
        if (tokenPruningConfig != null) {
            builder.field(PRUNING_CONFIG.getPreferredName(), tokenPruningConfig);
        }
        boostAndQueryNameToXContent(builder);
        builder.endObject();
        builder.endObject();
    }

    /**
     * We calculate the maximum number of unique tokens for any shard of data. The maximum is used to compute
     * average token frequency since we don't have a unique inter-segment token count.
     * Once we have the maximum number of unique tokens, we use the total count of tokens in the index to calculate
     * the average frequency ratio.
     *
     * @param reader
     * @param fieldDocCount
     * @return float
     * @throws IOException
     */
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
     * Returns true if the token should be queried based on the {@code tokensFreqRatioThreshold} and {@code tokensWeightThreshold}
     * set on the query.
     */
    private boolean shouldKeepToken(
        IndexReader reader,
        WeightedToken token,
        int fieldDocCount,
        float averageTokenFreqRatio,
        float bestWeight
    ) throws IOException {
        if (this.tokenPruningConfig == null) {
            return true;
        }
        int docFreq = reader.docFreq(new Term(fieldName, token.token()));
        if (docFreq == 0) {
            return false;
        }
        float tokenFreqRatio = (float) docFreq / fieldDocCount;
        return tokenFreqRatio < this.tokenPruningConfig.getTokensFreqRatioThreshold() * averageTokenFreqRatio
            || token.weight() > this.tokenPruningConfig.getTokensWeightThreshold() * bestWeight;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        final MappedFieldType ft = context.getFieldType(fieldName);
        if (ft == null) {
            return new MatchNoDocsQuery("The \"" + getName() + "\" query is against a field that does not exist");
        }

        final String fieldTypeName = ft.typeName();
        if (AllowedFieldType.isFieldTypeAllowed(fieldTypeName) == false) {
            throw new ElasticsearchParseException(
                "["
                    + fieldTypeName
                    + "]"
                    + " is not an appropriate field type for this query. "
                    + "Allowed field types are ["
                    + AllowedFieldType.getAllowedFieldTypesAsString()
                    + "]."
            );
        }

        return (this.tokenPruningConfig == null)
            ? queryBuilderWithAllTokens(tokens, ft, context)
            : queryBuilderWithPrunedTokens(tokens, ft, context);
    }

    private Query queryBuilderWithAllTokens(List<WeightedToken> tokens, MappedFieldType ft, SearchExecutionContext context) {
        var qb = new BooleanQuery.Builder();

        for (var token : tokens) {
            qb.add(new BoostQuery(ft.termQuery(token.token(), context), token.weight()), BooleanClause.Occur.SHOULD);
        }
        return qb.setMinimumNumberShouldMatch(1).build();
    }

    private Query queryBuilderWithPrunedTokens(List<WeightedToken> tokens, MappedFieldType ft, SearchExecutionContext context)
        throws IOException {
        var qb = new BooleanQuery.Builder();
        int fieldDocCount = context.getIndexReader().getDocCount(fieldName);
        float bestWeight = tokens.stream().map(WeightedToken::weight).reduce(0f, Math::max);
        float averageTokenFreqRatio = getAverageTokenFreqRatio(context.getIndexReader(), fieldDocCount);
        if (averageTokenFreqRatio == 0) {
            return new MatchNoDocsQuery("The \"" + getName() + "\" query is against an empty field");
        }

        for (var token : tokens) {
            boolean keep = shouldKeepToken(context.getIndexReader(), token, fieldDocCount, averageTokenFreqRatio, bestWeight);
            keep ^= this.tokenPruningConfig.isOnlyScorePrunedTokens();
            if (keep) {
                qb.add(new BoostQuery(ft.termQuery(token.token(), context), token.weight()), BooleanClause.Occur.SHOULD);
            }
        }

        return qb.setMinimumNumberShouldMatch(1).build();
    }

    @Override
    protected boolean doEquals(WeightedTokensQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(tokenPruningConfig, other.tokenPruningConfig)
            && tokens.equals(other.tokens);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, tokens, tokenPruningConfig);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.TEXT_EXPANSION_TOKEN_PRUNING_CONFIG_ADDED;
    }

    private static float parseWeight(String token, Object weight) throws IOException {
        if (weight instanceof Number asNumber) {
            return asNumber.floatValue();
        }
        if (weight instanceof String asString) {
            return Float.parseFloat(asString);
        }
        throw new ElasticsearchParseException(
            "Illegal weight for token: [" + token + "], expected floating point got " + weight.getClass().getSimpleName()
        );
    }

    public static WeightedTokensQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String currentFieldName = null;
        String fieldName = null;
        List<WeightedToken> tokens = new ArrayList<>();
        TokenPruningConfig tokenPruningConfig = null;
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
                    } else if (PRUNING_CONFIG.match(currentFieldName, parser.getDeprecationHandler())) {
                        if (token != XContentParser.Token.START_OBJECT) {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "[" + PRUNING_CONFIG.getPreferredName() + "] should be an object"
                            );
                        }
                        tokenPruningConfig = TokenPruningConfig.fromXContent(parser);
                    } else if (TOKENS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        var tokensMap = parser.map();
                        for (var e : tokensMap.entrySet()) {
                            tokens.add(new WeightedToken(e.getKey(), parseWeight(e.getKey(), e.getValue())));
                        }
                    } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        boost = parser.floatValue();
                    } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        queryName = parser.text();
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "unknown field [" + currentFieldName + "]");
                    }
                }
            } else {
                throw new IllegalArgumentException("invalid query");
            }
        }

        if (fieldName == null) {
            throw new ParsingException(parser.getTokenLocation(), "No fieldname specified for query");
        }

        var qb = new WeightedTokensQueryBuilder(fieldName, tokens, tokenPruningConfig);
        qb.queryName(queryName);
        qb.boost(boost);
        return qb;
    }
}
