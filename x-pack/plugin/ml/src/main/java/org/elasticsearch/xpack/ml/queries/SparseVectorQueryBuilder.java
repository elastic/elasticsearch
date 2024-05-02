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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.ml.action.CoordinatedInferenceAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigUpdate;
import org.elasticsearch.xpack.core.ml.search.TokenPruningConfig;
import org.elasticsearch.xpack.core.ml.search.WeightedToken;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class SparseVectorQueryBuilder extends AbstractQueryBuilder<SparseVectorQueryBuilder> {
    public static final String NAME = "sparse_vector";
    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");
    public static final ParseField INFERENCE_ID_FIELD = new ParseField("inference_id");
    public static final ParseField TEXT_FIELD = new ParseField("text");
    public static final ParseField PRUNE_FIELD = new ParseField("prune");
    public static final ParseField PRUNING_CONFIG_FIELD = new ParseField("pruning_config");

    private static final boolean DEFAULT_PRUNE = false;
    private final String fieldName;
    private final List<WeightedToken> tokens;
    private final String inferenceId;
    private final String text;
    private final Boolean shouldPruneTokens;

    private SetOnce<TextExpansionResults> weightedTokensSupplier;

    @Nullable
    private final TokenPruningConfig tokenPruningConfig;

    public SparseVectorQueryBuilder(String fieldName, String inferenceId, String text) {
        this(fieldName, null, inferenceId, text, DEFAULT_PRUNE, null);
    }

    public SparseVectorQueryBuilder(String fieldName, List<WeightedToken> queryVector) {
        this(fieldName, queryVector, null, null, DEFAULT_PRUNE, null);
    }

    public SparseVectorQueryBuilder(
        String fieldName,
        @Nullable List<WeightedToken> tokens,
        @Nullable String inferenceId,
        @Nullable String text,
        @Nullable Boolean shouldPruneTokens,
        @Nullable TokenPruningConfig tokenPruningConfig
    ) {
        this.fieldName = Objects.requireNonNull(fieldName, "[" + NAME + "] requires a fieldName");
        this.shouldPruneTokens = (shouldPruneTokens != null ? shouldPruneTokens : DEFAULT_PRUNE);
        this.tokens = tokens;
        this.inferenceId = inferenceId;
        this.text = text;
        this.tokenPruningConfig = tokenPruningConfig;

        if (tokens == null ^ inferenceId == null == false) {
            throw new IllegalArgumentException(
                "[" + NAME + "] requires one of " + QUERY_VECTOR_FIELD.getPreferredName() + " or " + INFERENCE_ID_FIELD.getPreferredName()
            );
        }
        // TODO additional validation
    }

    public SparseVectorQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.shouldPruneTokens = in.readOptionalBoolean();
        this.tokens = in.readOptionalCollectionAsList(WeightedToken::new);
        this.inferenceId = in.readOptionalString();
        this.text = in.readOptionalString();
        this.tokenPruningConfig = in.readOptionalWriteable(TokenPruningConfig::new);
    }

    public SparseVectorQueryBuilder(SparseVectorQueryBuilder other, SetOnce<TextExpansionResults> weightedTokensSupplier) {
        this.fieldName = other.fieldName;
        this.shouldPruneTokens = other.shouldPruneTokens;
        this.tokens = other.tokens;
        this.inferenceId = other.inferenceId;
        this.text = other.text;
        this.tokenPruningConfig = other.tokenPruningConfig;
        this.weightedTokensSupplier = weightedTokensSupplier;
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
        out.writeOptionalString(inferenceId);
        out.writeOptionalString(text);
        out.writeOptionalWriteable(tokenPruningConfig);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(FIELD_FIELD.getPreferredName(), fieldName);
        if (tokens != null) {
            builder.startObject(QUERY_VECTOR_FIELD.getPreferredName());
            for (var token : tokens) {
                token.toXContent(builder, params);
            }
            builder.endObject();
        } else {
            builder.field(INFERENCE_ID_FIELD.getPreferredName(), inferenceId);
            builder.field(TEXT_FIELD.getPreferredName(), text);
        }
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

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {

        if (tokens != null) {
            // Weighted tokens
            return weightedTokensToQuery(fieldName, tokens);
        } else {
            // Inference
            if (weightedTokensSupplier != null) {
                TextExpansionResults textExpansionResults = weightedTokensSupplier.get();
                if (textExpansionResults == null) {
                    return this;
                }
                return weightedTokensToQuery(fieldName, textExpansionResults.getWeightedTokens());
            }

            // Get model ID from inference ID
            GetInferenceModelAction.Request getInferenceModelActionRequest = new GetInferenceModelAction.Request(
                inferenceId,
                TaskType.SPARSE_EMBEDDING
            );

            queryRewriteContext.registerAsyncAction(
                (client, listener) -> executeAsyncWithOrigin(
                    client,
                    ML_ORIGIN,
                    GetInferenceModelAction.INSTANCE,
                    getInferenceModelActionRequest,
                    new ActionListener<>() {
                        @Override
                        public void onResponse(GetInferenceModelAction.Response response) {
                            String modelId = response.getModels().get(0).getInferenceEntityId();
                            CoordinatedInferenceAction.Request inferRequest = CoordinatedInferenceAction.Request.forTextInput(
                                modelId,
                                List.of(text),
                                TextExpansionConfigUpdate.EMPTY_UPDATE,
                                false,
                                InferModelAction.Request.DEFAULT_TIMEOUT_FOR_API
                            );
                            inferRequest.setHighPriority(true);
                            inferRequest.setPrefixType(TrainedModelPrefixStrings.PrefixType.SEARCH);

                            queryRewriteContext.registerAsyncAction(
                                (client1, listener1) -> executeAsyncWithOrigin(
                                    client1,
                                    ML_ORIGIN,
                                    InferenceAction.INSTANCE,
                                    inferRequest,
                                    ActionListener.wrap(inferenceResponse -> {
                                        if (inferenceResponse.getResults().asMap().isEmpty()) {
                                            listener1.onFailure(new IllegalStateException("inference response contain no results"));
                                            return;
                                        }

                                        if (inferenceResponse.getResults()
                                            .transformToLegacyFormat()
                                            .get(0) instanceof TextExpansionResults textExpansionResults) {
                                            weightedTokensSupplier.set(textExpansionResults);
                                            listener1.onResponse(null);
                                        } else if (inferenceResponse.getResults()
                                            .transformToLegacyFormat()
                                            .get(0) instanceof WarningInferenceResults warning) {
                                                listener1.onFailure(new IllegalStateException(warning.getWarning()));
                                            } else {
                                                listener1.onFailure(
                                                    new IllegalStateException(
                                                        "expected a result of type ["
                                                            + TextExpansionResults.NAME
                                                            + "] received ["
                                                            + inferenceResponse.getResults()
                                                                .transformToLegacyFormat()
                                                                .get(0)
                                                                .getWriteableName()
                                                            + "]. Is ["
                                                            + inferenceId
                                                            + "] a compatible inferenceId?"
                                                    )
                                                );
                                            }
                                    }, listener1::onFailure)
                                )
                            );
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    }
                )
            );

            // queryRewriteContext.registerAsyncAction(
            // (client, listener) -> executeAsyncWithOrigin(
            // client,
            // ML_ORIGIN,
            // GetInferenceModelAction.INSTANCE,
            // getInferenceModelActionRequest,
            // ActionListener.wrap(getInferenceModelActionResponse -> {
            // if (getInferenceModelActionResponse.getModels().isEmpty()) {
            // listener.onFailure(new IllegalStateException("Inference ID not found: " + inferenceId));
            // return;
            // }
            // String modelId = getInferenceModelActionResponse.getModels().get(0).getInferenceEntityId();
            // CoordinatedInferenceAction.Request inferRequest = CoordinatedInferenceAction.Request.forTextInput(
            // modelId,
            // List.of(text),
            // TextExpansionConfigUpdate.EMPTY_UPDATE,
            // false,
            // InferModelAction.Request.DEFAULT_TIMEOUT_FOR_API
            // );
            // inferRequest.setHighPriority(true);
            // inferRequest.setPrefixType(TrainedModelPrefixStrings.PrefixType.SEARCH);
            //
            // queryRewriteContext.registerAsyncAction(
            // (client1, listener1) -> executeAsyncWithOrigin(
            // client1,
            // ML_ORIGIN,
            // InferenceAction.INSTANCE,
            // inferRequest,
            // ActionListener.wrap(inferenceResponse -> {
            // if (inferenceResponse.getResults().asMap().isEmpty()) {
            // listener1.onFailure(new IllegalStateException("inference response contain no results"));
            // return;
            // }
            //
            // if (inferenceResponse.getResults()
            // .transformToLegacyFormat()
            // .get(0) instanceof TextExpansionResults textExpansionResults) {
            // weightedTokensSupplier.set(textExpansionResults);
            // listener1.onResponse(null);
            // } else if (inferenceResponse.getResults()
            // .transformToLegacyFormat()
            // .get(0) instanceof WarningInferenceResults warning) {
            // listener1.onFailure(new IllegalStateException(warning.getWarning()));
            // } else {
            // listener1.onFailure(
            // new IllegalStateException(
            // "expected a result of type ["
            // + TextExpansionResults.NAME
            // + "] received ["
            // + inferenceResponse.getResults().transformToLegacyFormat().get(0).getWriteableName()
            // + "]. Is ["
            // + inferenceId
            // + "] a compatible inferenceId?"
            // )
            // );
            // }
            // }, listener1::onFailure)
            // )
            // );
            // listener.onResponse(null);
            // }, listener::onFailure)
            // )
            // );

            return new SparseVectorQueryBuilder(this, weightedTokensSupplier);
        }
    }

    private QueryBuilder weightedTokensToQuery(String fieldName, List<WeightedToken> weightedTokens) {
        // TODO support pruning config
        var boolQuery = QueryBuilders.boolQuery();
        for (var weightedToken : weightedTokens) {
            boolQuery.should(QueryBuilders.termQuery(fieldName, weightedToken.token()).boost(weightedToken.weight()));
        }
        boolQuery.minimumShouldMatch(1);
        boolQuery.boost(boost);
        boolQuery.queryName(queryName);
        return boolQuery;
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
        List<WeightedToken> weightedTokens = weightedTokenMap != null
            ? weightedTokenMap.entrySet().stream().map(e -> new WeightedToken(e.getKey(), e.getValue().floatValue())).toList()
            : null;
        String inferenceId = (String) a[2];
        String text = (String) a[3];
        Boolean shouldPruneTokens = (Boolean) a[4];
        TokenPruningConfig tokenPruningConfig = (TokenPruningConfig) a[5];
        return new SparseVectorQueryBuilder(fieldName, weightedTokens, inferenceId, text, shouldPruneTokens, tokenPruningConfig);
    });
    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), QUERY_VECTOR_FIELD);
        PARSER.declareString(optionalConstructorArg(), INFERENCE_ID_FIELD);
        PARSER.declareString(optionalConstructorArg(), TEXT_FIELD);
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
