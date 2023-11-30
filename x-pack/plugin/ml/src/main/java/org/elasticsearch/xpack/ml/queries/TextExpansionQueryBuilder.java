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
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
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
import static org.elasticsearch.xpack.ml.queries.WeightedTokensQueryBuilder.RATIO_THRESHOLD_FIELD;
import static org.elasticsearch.xpack.ml.queries.WeightedTokensQueryBuilder.WEIGHT_THRESHOLD_FIELD;

public class TextExpansionQueryBuilder extends AbstractQueryBuilder<TextExpansionQueryBuilder> {

    public static final String NAME = "text_expansion";
    public static final ParseField MODEL_TEXT = new ParseField("model_text");
    public static final ParseField MODEL_ID = new ParseField("model_id");

    private final String fieldName;
    private final String modelText;
    private final String modelId;
    private SetOnce<TextExpansionResults> weightedTokensSupplier;
    private final int ratioThreshold;
    private final float weightThreshold;

    public TextExpansionQueryBuilder(String fieldName, String modelText, String modelId) {
       this(fieldName, modelText, modelId, -1, 1f);
    }

    public TextExpansionQueryBuilder(String fieldName, String modelText, String modelId, int ratioThreshold, float weightThreshold) {
        if (fieldName == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a fieldName");
        }
        if (modelText == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a " + MODEL_TEXT.getPreferredName() + " value");
        }
        if (modelId == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a " + MODEL_ID.getPreferredName() + " value");
        }
        if (weightThreshold < 0 || weightThreshold > 1) {
            throw new IllegalArgumentException("[" + NAME + "] requires the "
                    + WEIGHT_THRESHOLD_FIELD.getPreferredName() + " to be between 0 and 1, got " + weightThreshold);
        }

        this.fieldName = fieldName;
        this.modelText = modelText;
        this.modelId = modelId;
        this.ratioThreshold = ratioThreshold;
        this.weightThreshold = weightThreshold;
    }

    public TextExpansionQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.modelText = in.readString();
        this.modelId = in.readString();
        if (in.getTransportVersion().onOrAfter(TransportVersions.WEIGHTED_TOKENS_QUERY_ADDED)) {
            this.ratioThreshold = in.readInt();
            this.weightThreshold = in.readFloat();
        } else {
            this.ratioThreshold = 0;
            this.weightThreshold = 1f;
        }
    }

    private TextExpansionQueryBuilder(TextExpansionQueryBuilder other, SetOnce<TextExpansionResults> weightedTokensSupplier) {
        this.fieldName = other.fieldName;
        this.modelText = other.modelText;
        this.modelId = other.modelId;
        this.ratioThreshold = other.ratioThreshold;
        this.weightThreshold = other.weightThreshold;
        this.boost = other.boost;
        this.queryName = other.queryName;
        this.weightedTokensSupplier = weightedTokensSupplier;
    }

    String getFieldName() {
        return fieldName;
    }

    /**
     * Returns the frequency ratio threshold to apply on the query.
     * Tokens whose frequency is more than ratio_threshold times the average frequency of all tokens in the specified
     * field are considered outliers and may be subject to removal from the query.
     */
    public int getRatioThreshold() {
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
        if (out.getTransportVersion().onOrAfter(TransportVersions.WEIGHTED_TOKENS_QUERY_ADDED)) {
            out.writeInt(ratioThreshold);
            out.writeFloat(weightThreshold);
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(MODEL_TEXT.getPreferredName(), modelText);
        builder.field(MODEL_ID.getPreferredName(), modelId);
        if (ratioThreshold > 0) {
            builder.field(RATIO_THRESHOLD_FIELD.getPreferredName(), ratioThreshold);
            builder.field(WEIGHT_THRESHOLD_FIELD.getPreferredName(), weightThreshold);
        }
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
        queryRewriteContext.registerAsyncAction((client, listener) -> {
            executeAsyncWithOrigin(
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
            );
        });

        return new TextExpansionQueryBuilder(this, textExpansionResultsSupplier);
    }

    private QueryBuilder weightedTokensToQuery(
        String fieldName,
        TextExpansionResults textExpansionResults,
        QueryRewriteContext queryRewriteContext
    ) throws IOException {
        if (ratioThreshold > 0) {
            return new WeightedTokensQueryBuilder(fieldName, textExpansionResults.getWeightedTokens(),
                    ratioThreshold, weightThreshold);
        }

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
            && ratioThreshold == other.ratioThreshold
            && Float.compare(weightThreshold, other.weightThreshold) == 0
            && Objects.equals(weightedTokensSupplier, other.weightedTokensSupplier);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, modelText, modelId, ratioThreshold, weightThreshold, weightedTokensSupplier);
    }

    public static TextExpansionQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        String modelText = null;
        String modelId = null;
        int ratioThreshold = 0;
        float weightThreshold = 1f;
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
                        } else if (RATIO_THRESHOLD_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            ratioThreshold = parser.intValue();
                        } else if (WEIGHT_THRESHOLD_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            weightThreshold = parser.floatValue();
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

        TextExpansionQueryBuilder queryBuilder = new TextExpansionQueryBuilder(fieldName, modelText,
                modelId, ratioThreshold, weightThreshold);
        queryBuilder.queryName(queryName);
        queryBuilder.boost(boost);
        return queryBuilder;
    }
}
