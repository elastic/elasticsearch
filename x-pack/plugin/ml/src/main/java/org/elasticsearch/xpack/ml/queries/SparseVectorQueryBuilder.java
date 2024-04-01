/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.queries;

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
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults.VectorDimension;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigUpdate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class SparseVectorQueryBuilder extends AbstractQueryBuilder<SparseVectorQueryBuilder> {

    public static final String NAME = "sparse_vector";
    public static final ParseField PRUNING_CONFIG = new ParseField("pruning_config");
    public static final ParseField MODEL_TEXT = new ParseField("model_text");
    public static final ParseField MODEL_ID = new ParseField("model_id");
    public static final ParseField VECTOR_DIMENSIONS = new ParseField("vector_dimensions");

    private final String fieldName;
    private final String modelText;
    private final String modelId;
    private final List<VectorDimension> vectorDimensions;
    private SetOnce<TextExpansionResults> vectorDimensionsSupplier;
    private final TokenPruningConfig tokenPruningConfig;

    public SparseVectorQueryBuilder(
        String fieldName,
        String modelText,
        @Nullable String modelId,
        @Nullable List<VectorDimension> vectorDimensions
    ) {
        this(fieldName, modelText, modelId, vectorDimensions, null);
    }

    public SparseVectorQueryBuilder(
        String fieldName,
        String modelText,
        @Nullable String modelId,
        @Nullable List<VectorDimension> vectorDimensions,
        @Nullable TokenPruningConfig tokenPruningConfig
    ) {
        if (fieldName == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a fieldName");
        }

        if ((vectorDimensions == null) == (modelId == null)) {
            throw new IllegalArgumentException(
                "[" + NAME + "] requires one of [" + MODEL_ID.getPreferredName() + "], or [" + VECTOR_DIMENSIONS.getPreferredName() + "]"
            );
        }
        if (modelId != null && modelText == null) {
            throw new IllegalArgumentException(
                "[" + NAME + "] requires [" + MODEL_TEXT.getPreferredName() + "] when [" + MODEL_ID.getPreferredName() + "] is specified"
            );
        }

        this.fieldName = fieldName;
        this.modelText = modelText;
        this.modelId = modelId;
        this.vectorDimensions = vectorDimensions;
        this.tokenPruningConfig = tokenPruningConfig;
    }

    public SparseVectorQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.modelText = in.readString();
        this.modelId = in.readOptionalString();
        this.tokenPruningConfig = in.readOptionalWriteable(TokenPruningConfig::new);
        this.vectorDimensions = in.readOptionalCollectionAsList(VectorDimension::new);
    }

    private SparseVectorQueryBuilder(SparseVectorQueryBuilder other, SetOnce<TextExpansionResults> vectorDimensionsSupplier) {
        this.fieldName = other.fieldName;
        this.modelText = other.modelText;
        this.modelId = other.modelId;
        this.vectorDimensions = other.vectorDimensions;
        this.tokenPruningConfig = other.tokenPruningConfig;
        this.boost = other.boost;
        this.queryName = other.queryName;
        this.vectorDimensionsSupplier = vectorDimensionsSupplier;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ADD_SPARSE_VECTOR_QUERY;
    }

    public TokenPruningConfig getTokenPruningConfig() {
        return tokenPruningConfig;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (vectorDimensionsSupplier != null) {
            throw new IllegalStateException(
                "vector dimension supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?"
            );
        }
        out.writeString(fieldName);
        out.writeString(modelText);
        out.writeOptionalString(modelId);
        out.writeOptionalWriteable(tokenPruningConfig);
        out.writeOptionalCollection(vectorDimensions, StreamOutput::writeWriteable);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(MODEL_TEXT.getPreferredName(), modelText);
        if (modelId != null) {
            builder.field(MODEL_ID.getPreferredName(), modelId);
        }
        if (tokenPruningConfig != null) {
            builder.field(PRUNING_CONFIG.getPreferredName(), tokenPruningConfig);
        }
        if (vectorDimensions != null) {
            builder.startObject(VECTOR_DIMENSIONS.getPreferredName());
            for (var vectorDimension : vectorDimensions) {
                vectorDimension.toXContent(builder, params);
            }
            builder.endObject();
        }
        boostAndQueryNameToXContent(builder);
        builder.endObject();
        builder.endObject();
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        if (vectorDimensionsSupplier != null) {
            if (vectorDimensionsSupplier.get() == null) {
                return this;
            }
            return textExpansionResultsToQuery(fieldName, vectorDimensionsSupplier.get());
        }

        if (modelId != null) {
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

            return new SparseVectorQueryBuilder(this, textExpansionResultsSupplier);

        } else {
            return vectorDimensionsToQuery(fieldName, vectorDimensions);
        }

    }

    private QueryBuilder vectorDimensionsToQuery(String fieldName, List<VectorDimension> vectorDimensions) {
        if (tokenPruningConfig != null) {
            VectorDimensionsQueryBuilder vectorDimensionsQueryBuilder = new VectorDimensionsQueryBuilder(
                fieldName,
                vectorDimensions,
                tokenPruningConfig
            );
            vectorDimensionsQueryBuilder.queryName(queryName);
            vectorDimensionsQueryBuilder.boost(boost);
            return vectorDimensionsQueryBuilder;
        }
        // Note: Weighted tokens queries were introduced in 8.13.0. To support mixed version clusters prior to 8.13.0,
        // if no token pruning configuration is specified we fall back to a boolean query.
        // TODO this should be updated to always use a WeightedTokensQueryBuilder once it's in all supported versions.
        var boolQuery = QueryBuilders.boolQuery();
        for (var weightedToken : vectorDimensions) {
            boolQuery.should(QueryBuilders.termQuery(fieldName, weightedToken.token()).boost(weightedToken.weight()));
        }
        boolQuery.minimumShouldMatch(1);
        boolQuery.boost(boost);
        boolQuery.queryName(queryName);
        return boolQuery;
    }

    private QueryBuilder textExpansionResultsToQuery(String fieldName, TextExpansionResults textExpansionResults) {
        return vectorDimensionsToQuery(fieldName, textExpansionResults.getVectorDimensions());
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) {
        throw new IllegalStateException("sparse_vector should have been rewritten to another query type");
    }

    @Override
    protected boolean doEquals(SparseVectorQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(modelText, other.modelText)
            && Objects.equals(modelId, other.modelId)
            && Objects.equals(tokenPruningConfig, other.tokenPruningConfig)
            && Objects.equals(vectorDimensions, other.vectorDimensions)
            && Objects.equals(vectorDimensionsSupplier, other.vectorDimensionsSupplier);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, modelText, modelId, tokenPruningConfig, vectorDimensions, vectorDimensionsSupplier);
    }

    public static SparseVectorQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        String modelText = null;
        String modelId = null;
        List<VectorDimension> vectorDimensions = new ArrayList<>();
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
                        } else if (VECTOR_DIMENSIONS.match(currentFieldName, parser.getDeprecationHandler())) {
                            var vectorDimensionsMap = parser.map();
                            for (var e : vectorDimensionsMap.entrySet()) {
                                vectorDimensions.add(new VectorDimension(e.getKey(), parseWeight(e.getKey(), e.getValue())));
                            }
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

        if (fieldName == null) {
            throw new ParsingException(parser.getTokenLocation(), "No fieldname specified for query");
        }

        SparseVectorQueryBuilder queryBuilder = new SparseVectorQueryBuilder(
            fieldName,
            modelText,
            modelId,
            vectorDimensions.isEmpty() ? null : vectorDimensions,
            tokenPruningConfig
        );
        queryBuilder.queryName(queryName);
        queryBuilder.boost(boost);
        return queryBuilder;
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
}
