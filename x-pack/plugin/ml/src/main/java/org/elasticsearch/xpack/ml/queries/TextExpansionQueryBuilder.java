/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.queries;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigUpdate;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TextExpansionQueryBuilder extends AbstractQueryBuilder<TextExpansionQueryBuilder> {

    public static final String NAME = "text_expansion";
    public static final ParseField MODEL_TEXT = new ParseField("model_text");
    public static final ParseField MODEL_ID = new ParseField("model_id");

    private final String fieldName;
    private final String modelText;
    private final String modelId;
    private SetOnce<TextExpansionResults> weightedTokensSupplier;

    public TextExpansionQueryBuilder(String fieldName, String modelText, String modelId) {
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
    }

    public TextExpansionQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.modelText = in.readString();
        this.modelId = in.readString();
    }

    private TextExpansionQueryBuilder(TextExpansionQueryBuilder other, SetOnce<TextExpansionResults> weightedTokensSupplier) {
        this.fieldName = other.fieldName;
        this.modelText = other.modelText;
        this.modelId = other.modelId;
        this.boost = other.boost;
        this.queryName = other.queryName;
        this.weightedTokensSupplier = weightedTokensSupplier;
    }

    String getFieldName() {
        return fieldName;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_8_0;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (weightedTokensSupplier != null) {
            throw new IllegalStateException("token supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }
        out.writeString(fieldName);
        out.writeString(modelText);
        out.writeString(modelId);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(MODEL_TEXT.getPreferredName(), modelText);
        builder.field(MODEL_ID.getPreferredName(), modelId);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
        builder.endObject();
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (weightedTokensSupplier != null) {
            if (weightedTokensSupplier.get() == null) {
                return this;
            }
            return weightedTokensToQuery(fieldName, weightedTokensSupplier.get(), queryRewriteContext);
        }

        InferModelAction.Request inferRequest = InferModelAction.Request.forTextInput(
            modelId,
            TextExpansionConfigUpdate.EMPTY_UPDATE,
            List.of(modelText)
        );
        inferRequest.setHighPriority(true);

        SetOnce<TextExpansionResults> textExpansionResultsSupplier = new SetOnce<>();
        queryRewriteContext.registerAsyncAction(
            (client, listener) -> executeAsyncWithOrigin(
                client,
                ML_ORIGIN,
                InferModelAction.INSTANCE,
                inferRequest,
                listener.delegateFailureAndWrap((delegate, inferenceResponse) -> {

                    if (inferenceResponse.getInferenceResults().isEmpty()) {
                        delegate.onFailure(new IllegalStateException("inference response contain no results"));
                        return;
                    }

                    if (inferenceResponse.getInferenceResults().get(0) instanceof TextExpansionResults textExpansionResults) {
                        textExpansionResultsSupplier.set(textExpansionResults);
                        delegate.onResponse(null);
                    } else if (inferenceResponse.getInferenceResults().get(0) instanceof WarningInferenceResults warning) {
                        delegate.onFailure(new IllegalStateException(warning.getWarning()));
                    } else {
                        delegate.onFailure(
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
                })
            )
        );

        return new TextExpansionQueryBuilder(this, textExpansionResultsSupplier);
    }

    static BoolQueryBuilder weightedTokensToQuery(
        String fieldName,
        TextExpansionResults textExpansionResults,
        QueryRewriteContext queryRewriteContext
    ) throws IOException {
        var boolQuery = QueryBuilders.boolQuery();
        for (var weightedToken : textExpansionResults.getWeightedTokens()) {
            boolQuery.should(QueryBuilders.termQuery(fieldName, weightedToken.token()).boost(weightedToken.weight()));
        }
        boolQuery.minimumShouldMatch(1);
        return boolQuery;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        throw new IllegalStateException("text_expansion should have been rewritten to another query type");
    }

    @Override
    protected boolean doEquals(TextExpansionQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(modelText, other.modelText)
            && Objects.equals(modelId, other.modelId)
            && Objects.equals(weightedTokensSupplier, other.weightedTokensSupplier);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, modelText, modelId, weightedTokensSupplier);
    }

    public static TextExpansionQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        String modelText = null;
        String modelId = null;
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

        TextExpansionQueryBuilder queryBuilder = new TextExpansionQueryBuilder(fieldName, modelText, modelId);
        queryBuilder.queryName(queryName);
        queryBuilder.boost(boost);
        return queryBuilder;
    }
}
