/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.search;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.TokenPruningConfig;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.CoordinatedInferenceAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigUpdate;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.ml.search.WeightedTokensQueryBuilder.PRUNING_CONFIG;

/**
 * @deprecated Replaced by sparse_vector query
 */
@Deprecated
public class TextExpansionQueryBuilder extends AbstractQueryBuilder<TextExpansionQueryBuilder> {

    public static final String NAME = "text_expansion";
    public static final ParseField MODEL_TEXT = new ParseField("model_text");
    public static final ParseField MODEL_ID = new ParseField("model_id");

    private final String fieldName;
    private final String modelText;
    private final String modelId;
    private SetOnce<TextExpansionResults> weightedTokensSupplier;
    private final TokenPruningConfig tokenPruningConfig;

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ParseField.class);
    public static final String TEXT_EXPANSION_DEPRECATION_MESSAGE = NAME + " is deprecated. Use sparse_vector instead.";

    public TextExpansionQueryBuilder(String fieldName, String modelText, String modelId) {
        this(fieldName, modelText, modelId, null);
    }

    public TextExpansionQueryBuilder(String fieldName, String modelText, String modelId, @Nullable TokenPruningConfig tokenPruningConfig) {
        if (fieldName == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a fieldName");
        }
        if (modelText == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a " + MODEL_TEXT.getPreferredName() + " value");
        }
        if (modelId == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a " + MODEL_ID.getPreferredName() + " value");
        }
        this.fieldName = fieldName;
        this.modelText = modelText;
        this.modelId = modelId;
        this.tokenPruningConfig = tokenPruningConfig;
    }

    public TextExpansionQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.modelText = in.readString();
        this.modelId = in.readString();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            this.tokenPruningConfig = in.readOptionalWriteable(TokenPruningConfig::new);
        } else {
            this.tokenPruningConfig = null;
        }
    }

    private TextExpansionQueryBuilder(TextExpansionQueryBuilder other, SetOnce<TextExpansionResults> weightedTokensSupplier) {
        this.fieldName = other.fieldName;
        this.modelText = other.modelText;
        this.modelId = other.modelId;
        this.tokenPruningConfig = other.tokenPruningConfig;
        this.boost = other.boost;
        this.queryName = other.queryName;
        this.weightedTokensSupplier = weightedTokensSupplier;
    }

    String getFieldName() {
        return fieldName;
    }

    public TokenPruningConfig getTokenPruningConfig() {
        return tokenPruningConfig;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_8_0;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (weightedTokensSupplier != null) {
            throw new IllegalStateException("token supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }
        out.writeString(fieldName);
        out.writeString(modelText);
        out.writeString(modelId);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            out.writeOptionalWriteable(tokenPruningConfig);
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(MODEL_TEXT.getPreferredName(), modelText);
        builder.field(MODEL_ID.getPreferredName(), modelId);
        if (tokenPruningConfig != null) {
            builder.field(PRUNING_CONFIG.getPreferredName(), tokenPruningConfig);
        }
        boostAndQueryNameToXContent(builder);
        builder.endObject();
        builder.endObject();
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        if (weightedTokensSupplier != null) {
            if (weightedTokensSupplier.get() == null) {
                return this;
            }
            return weightedTokensToQuery(fieldName, weightedTokensSupplier.get());
        }

        CoordinatedInferenceAction.Request inferRequest = CoordinatedInferenceAction.Request.forTextInput(
            modelId,
            List.of(modelText),
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
                                    + modelId
                                    + "] a compatible model?"
                            )
                        );
                    }
                }, listener::onFailure)
            )
        );

        return new TextExpansionQueryBuilder(this, textExpansionResultsSupplier);
    }

    private QueryBuilder weightedTokensToQuery(String fieldName, TextExpansionResults textExpansionResults) {
        if (tokenPruningConfig != null) {
            WeightedTokensQueryBuilder weightedTokensQueryBuilder = new WeightedTokensQueryBuilder(
                fieldName,
                textExpansionResults.getWeightedTokens(),
                tokenPruningConfig
            );
            weightedTokensQueryBuilder.queryName(queryName);
            weightedTokensQueryBuilder.boost(boost);
            return weightedTokensQueryBuilder;
        }
        // Note: Weighted tokens queries were introduced in 8.13.0. To support mixed version clusters prior to 8.13.0,
        // if no token pruning configuration is specified we fall back to a boolean query.
        // TODO this should be updated to always use a WeightedTokensQueryBuilder once it's in all supported versions.
        var boolQuery = QueryBuilders.boolQuery();
        for (var weightedToken : textExpansionResults.getWeightedTokens()) {
            boolQuery.should(QueryBuilders.termQuery(fieldName, weightedToken.token()).boost(weightedToken.weight()));
        }
        boolQuery.minimumShouldMatch(1);
        boolQuery.boost(boost);
        boolQuery.queryName(queryName);
        return boolQuery;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) {
        throw new IllegalStateException("text_expansion should have been rewritten to another query type");
    }

    @Override
    protected boolean doEquals(TextExpansionQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(modelText, other.modelText)
            && Objects.equals(modelId, other.modelId)
            && Objects.equals(tokenPruningConfig, other.tokenPruningConfig)
            && Objects.equals(weightedTokensSupplier, other.weightedTokensSupplier);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, modelText, modelId, tokenPruningConfig, weightedTokensSupplier);
    }

    public static TextExpansionQueryBuilder fromXContent(XContentParser parser) throws IOException {

        deprecationLogger.warn(DeprecationCategory.API, NAME, TEXT_EXPANSION_DEPRECATION_MESSAGE);

        String fieldName = null;
        String modelText = null;
        String modelId = null;
        TokenPruningConfig tokenPruningConfig = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        String currentFieldName = null;
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
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if (PRUNING_CONFIG.match(currentFieldName, parser.getDeprecationHandler())) {
                            tokenPruningConfig = TokenPruningConfig.fromXContent(parser);
                        } else {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                            );
                        }
                    } else if (token.isValue()) {
                        if (MODEL_TEXT.match(currentFieldName, parser.getDeprecationHandler())) {
                            modelText = parser.text();
                        } else if (MODEL_ID.match(currentFieldName, parser.getDeprecationHandler())) {
                            modelId = parser.text();
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
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                        );
                    }
                }
            } else {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, parser.currentName());
                fieldName = parser.currentName();
                modelText = parser.text();
            }
        }

        if (modelText == null) {
            throw new ParsingException(parser.getTokenLocation(), "No text specified for text query");
        }

        if (fieldName == null) {
            throw new ParsingException(parser.getTokenLocation(), "No fieldname specified for query");
        }

        TextExpansionQueryBuilder queryBuilder = new TextExpansionQueryBuilder(fieldName, modelText, modelId, tokenPruningConfig);
        queryBuilder.queryName(queryName);
        queryBuilder.boost(boost);
        return queryBuilder;
    }
}
