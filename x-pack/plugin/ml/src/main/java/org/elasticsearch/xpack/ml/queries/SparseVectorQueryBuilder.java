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
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.CoordinatedInferenceAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigUpdate;
import org.elasticsearch.xpack.core.ml.search.TokenPruningConfig;
import org.elasticsearch.xpack.core.ml.search.WeightedToken;
import org.elasticsearch.xpack.core.ml.search.WeightedTokensQueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class SparseVectorQueryBuilder extends AbstractQueryBuilder<SparseVectorQueryBuilder> {
    public static final String NAME = "sparse_vector";
    public static final String ALLOWED_FIELD_TYPE = "sparse_vector";
    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");
    public static final ParseField INFERENCE_ID_FIELD = new ParseField("inference_id");
    public static final ParseField TEXT_FIELD = new ParseField("text");
    public static final ParseField PRUNE_FIELD = new ParseField("prune");
    public static final ParseField PRUNING_CONFIG_FIELD = new ParseField("pruning_config");

    private static final boolean DEFAULT_PRUNE = false;
    private final String fieldName;
    private final List<WeightedToken> queryVectors;
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
        @Nullable List<WeightedToken> queryVectors,
        @Nullable String inferenceId,
        @Nullable String text,
        @Nullable Boolean shouldPruneTokens,
        @Nullable TokenPruningConfig tokenPruningConfig
    ) {
        this.fieldName = Objects.requireNonNull(fieldName, "[" + NAME + "] requires a field");
        this.shouldPruneTokens = (shouldPruneTokens != null ? shouldPruneTokens : DEFAULT_PRUNE);
        this.queryVectors = queryVectors;
        this.inferenceId = inferenceId;
        this.text = text;
        this.tokenPruningConfig = tokenPruningConfig;

        if (queryVectors == null ^ inferenceId == null == false) {
            throw new IllegalArgumentException(
                "["
                    + NAME
                    + "] requires one of ["
                    + QUERY_VECTOR_FIELD.getPreferredName()
                    + "] or ["
                    + INFERENCE_ID_FIELD.getPreferredName()
                    + "]"
            );
        }
        if (inferenceId != null && text == null) {
            throw new IllegalArgumentException(
                "["
                    + NAME
                    + "] requires ["
                    + TEXT_FIELD.getPreferredName()
                    + "] when ["
                    + INFERENCE_ID_FIELD.getPreferredName()
                    + "] is specified"
            );
        }
    }

    public SparseVectorQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.shouldPruneTokens = in.readOptionalBoolean();
        this.queryVectors = in.readOptionalCollectionAsList(WeightedToken::new);
        this.inferenceId = in.readOptionalString();
        this.text = in.readOptionalString();
        this.tokenPruningConfig = in.readOptionalWriteable(TokenPruningConfig::new);
    }

    public SparseVectorQueryBuilder(SparseVectorQueryBuilder other, SetOnce<TextExpansionResults> weightedTokensSupplier) {
        this.fieldName = other.fieldName;
        this.shouldPruneTokens = other.shouldPruneTokens;
        this.queryVectors = other.queryVectors;
        this.inferenceId = other.inferenceId;
        this.text = other.text;
        this.tokenPruningConfig = other.tokenPruningConfig;
        this.weightedTokensSupplier = weightedTokensSupplier;
    }

    public String getFieldName() {
        return fieldName;
    }

    public List<WeightedToken> getQueryVectors() {
        return queryVectors;
    }

    public boolean shouldPruneTokens() {
        return shouldPruneTokens;
    }

    public TokenPruningConfig getTokenPruningConfig() {
        return tokenPruningConfig;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeOptionalBoolean(shouldPruneTokens);
        out.writeOptionalCollection(queryVectors);
        out.writeOptionalString(inferenceId);
        out.writeOptionalString(text);
        out.writeOptionalWriteable(tokenPruningConfig);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(FIELD_FIELD.getPreferredName(), fieldName);
        if (queryVectors != null) {
            builder.startObject(QUERY_VECTOR_FIELD.getPreferredName());
            for (var token : queryVectors) {
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
        if (queryVectors == null) {
            return new MatchNoDocsQuery("Empty query vectors");
        }

        final MappedFieldType ft = context.getFieldType(fieldName);
        if (ft == null) {
            return new MatchNoDocsQuery("The \"" + getName() + "\" query is against a field that does not exist");
        }

        final String fieldTypeName = ft.typeName();
        if (fieldTypeName.equals(ALLOWED_FIELD_TYPE) == false) {
            throw new ElasticsearchParseException(
                "field [" + fieldName + "] must be type [" + ALLOWED_FIELD_TYPE + "] but is type [" + fieldTypeName + "]"
            );
        }

        return (this.tokenPruningConfig == null)
            ? queryBuilderWithAllTokens(queryVectors, ft, context)
            : queryBuilderWithPrunedTokens(queryVectors, ft, context);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {

        // Note: We return a WeightedTokensQueryBuilder here instead of a SparseVectorQueryBuilder, so that this
        // query will work when search.check_ccs_compatibility is set to true. This will eventually be removed
        // when it is no longer needed for compatibility.
        if (queryVectors != null) {
            // TODO return `this`
            return new WeightedTokensQueryBuilder(fieldName, queryVectors, tokenPruningConfig);
        } else if (weightedTokensSupplier != null) {
            TextExpansionResults textExpansionResults = weightedTokensSupplier.get();
            if (textExpansionResults == null) {
                return this;
            }

            // TODO - Replace this with a new instance of SparseVectorQueryBuilder that uses `textExpansionResults.getWeightedTokens()`
            // as the input query vectors
            return new WeightedTokensQueryBuilder(fieldName, textExpansionResults.getWeightedTokens(), tokenPruningConfig);
        }

        CoordinatedInferenceAction.Request inferRequest = CoordinatedInferenceAction.Request.forTextInput(
            inferenceId,
            List.of(text),
            TextExpansionConfigUpdate.EMPTY_UPDATE,
            false,
            InferModelAction.Request.DEFAULT_TIMEOUT_FOR_API
        );
        inferRequest.setHighPriority(true);
        inferRequest.setPrefixType(TrainedModelPrefixStrings.PrefixType.SEARCH);

        SetOnce<TextExpansionResults> textExpansionResultsSupplier = new SetOnce<>();
        queryRewriteContext.registerAsyncAction(
            (client, listener) -> executeAsyncWithOrigin(
                client,
                ML_ORIGIN,
                CoordinatedInferenceAction.INSTANCE,
                inferRequest,
                ActionListener.wrap(inferenceResponse -> {

                    if (inferenceResponse.getInferenceResults().isEmpty()) {
                        listener.onFailure(new IllegalStateException("inference response contain no results"));
                        return;
                    }

                    if (inferenceResponse.getInferenceResults().get(0) instanceof TextExpansionResults textExpansionResults) {
                        textExpansionResultsSupplier.set(textExpansionResults);
                        listener.onResponse(null);
                    } else if (inferenceResponse.getInferenceResults().get(0) instanceof WarningInferenceResults warning) {
                        listener.onFailure(new IllegalStateException(warning.getWarning()));
                    } else {
                        listener.onFailure(
                            new IllegalStateException(
                                "expected a result of type ["
                                    + TextExpansionResults.NAME
                                    + "] received ["
                                    + inferenceResponse.getInferenceResults().get(0).getWriteableName()
                                    + "]. Is ["
                                    + inferenceId
                                    + "] a compatible model?"
                            )
                        );
                    }
                }, listener::onFailure)
            )
        );

        return new SparseVectorQueryBuilder(this, textExpansionResultsSupplier);
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
            && Objects.equals(queryVectors, other.queryVectors);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, queryVectors, tokenPruningConfig);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.SPARSE_VECTOR_QUERY_ADDED;
    }

    private static final ConstructingObjectParser<SparseVectorQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME, a -> {
        String fieldName = (String) a[0];
        @SuppressWarnings("unchecked")
        List<WeightedToken> weightedTokens = parseWeightedTokens((Map<String, Object>) a[1]);
        String inferenceId = (String) a[2];
        String text = (String) a[3];
        Boolean shouldPruneTokens = (Boolean) a[4];
        TokenPruningConfig tokenPruningConfig = (TokenPruningConfig) a[5];
        return new SparseVectorQueryBuilder(fieldName, weightedTokens, inferenceId, text, shouldPruneTokens, tokenPruningConfig);
    });

    private static List<WeightedToken> parseWeightedTokens(Map<String, Object> weightedTokenMap) {
        List<WeightedToken> weightedTokens = null;
        if (weightedTokenMap != null) {
            weightedTokens = new ArrayList<>();
            for (Map.Entry<String, Object> entry : weightedTokenMap.entrySet()) {
                String token = entry.getKey();
                Object weight = entry.getValue();
                if (weight instanceof Float) {
                    WeightedToken weightedToken = new WeightedToken(token, (Float) entry.getValue());
                    weightedTokens.add(weightedToken);
                } else if (weight instanceof Double) {
                    WeightedToken weightedToken = new WeightedToken(token, ((Double) entry.getValue()).floatValue());
                    weightedTokens.add(weightedToken);
                } else {
                    throw new IllegalArgumentException("Weight must be a float or double");
                }
            }
        }
        return weightedTokens;
    }

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
