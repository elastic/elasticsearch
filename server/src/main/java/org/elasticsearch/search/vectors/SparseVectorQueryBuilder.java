/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.search.vectors;

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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SparseVectorQueryBuilder extends AbstractQueryBuilder<SparseVectorQueryBuilder> {
    public static final String NAME = "sparse_vector";
    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");
    public static final ParseField PRUNE_FIELD = new ParseField("prune");
    public static final ParseField PRUNING_CONFIG_FIELD = new ParseField("pruning_config");

    private static final boolean DEFAULT_PRUNE = false;
    private final String fieldName;
    private final List<WeightedToken> tokens;
    private final Boolean shouldPruneTokens;
    @Nullable
    private final TokenPruningConfig tokenPruningConfig;

    public SparseVectorQueryBuilder(String fieldName, List<WeightedToken> tokens) {
        this(fieldName, tokens, DEFAULT_PRUNE, null);
    }

    public SparseVectorQueryBuilder(
        String fieldName,
        List<WeightedToken> tokens,
        @Nullable Boolean shouldPruneTokens,
        @Nullable TokenPruningConfig tokenPruningConfig
    ) {
        this.fieldName = Objects.requireNonNull(fieldName, "[" + NAME + "] requires a fieldName");
        this.shouldPruneTokens = (shouldPruneTokens != null ? shouldPruneTokens : DEFAULT_PRUNE);
        this.tokens = Objects.requireNonNull(tokens, "[" + NAME + "] requires tokens");
        if (tokens.isEmpty()) {
            throw new IllegalArgumentException("[" + NAME + "] requires at least one token");
        }
        this.tokenPruningConfig = tokenPruningConfig;
    }

    public SparseVectorQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.shouldPruneTokens = in.readOptionalBoolean();
        this.tokens = in.readOptionalCollectionAsList(WeightedToken::new);
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
        out.writeOptionalBoolean(shouldPruneTokens);
        out.writeOptionalCollection(tokens);
        out.writeOptionalWriteable(tokenPruningConfig);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(FIELD_FIELD.getPreferredName(), fieldName);
        builder.startObject(QUERY_VECTOR_FIELD.getPreferredName());
        for (var token : tokens) {
            token.toXContent(builder, params);
        }
        builder.endObject();
        builder.field(PRUNE_FIELD.getPreferredName(), shouldPruneTokens);
        if (tokenPruningConfig != null) {
            builder.field(PRUNING_CONFIG_FIELD.getPreferredName(), tokenPruningConfig);
        }
        boostAndQueryNameToXContent(builder);
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
        if (fieldTypeName.equals("sparse_vector") == false) {
            throw new ElasticsearchParseException("[" + fieldTypeName + "] must be a [sparse_vector] field type.");
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
    protected boolean doEquals(SparseVectorQueryBuilder other) {
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

    private static final ConstructingObjectParser<SparseVectorQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME, a -> {
        String fieldName = (String) a[0];
        @SuppressWarnings("unchecked")
        Map<String, Double> weightedTokenMap = (Map<String, Double>) a[1];
        List<WeightedToken> weightedTokens = weightedTokenMap.entrySet()
            .stream()
            .map(e -> new WeightedToken(e.getKey(), e.getValue().floatValue()))
            .toList();
        Boolean shouldPruneTokens = (Boolean) a[2];
        TokenPruningConfig tokenPruningConfig = (TokenPruningConfig) a[3];
        return new SparseVectorQueryBuilder(fieldName, weightedTokens, shouldPruneTokens, tokenPruningConfig);
    });
    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), QUERY_VECTOR_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), PRUNE_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> TokenPruningConfig.fromXContent(p), PRUNING_CONFIG_FIELD);
        declareStandardFields(PARSER);
    }

    public static SparseVectorQueryBuilder fromXContent(XContentParser parser) {
        try {
            return PARSER.apply(parser, null);
        } catch (IllegalArgumentException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }
    }
}
