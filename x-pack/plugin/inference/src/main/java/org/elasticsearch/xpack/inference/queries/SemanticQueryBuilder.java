/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.InferenceProviderRegistry;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.SemanticTextModelSettings;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class SemanticQueryBuilder extends AbstractQueryBuilder<SemanticQueryBuilder> {
    public static final String NAME = "semantic_query";

    private static final ParseField QUERY_FIELD = new ParseField("query");

    private final String fieldName;
    private final String query;

    private SetOnce<InferenceServiceResults> inferenceResultsSupplier;
    private SetOnce<SemanticTextModelSettings> modelSettingsSupplier;

    public SemanticQueryBuilder(String fieldName, String query) {
        if (fieldName == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a fieldName");
        }
        if (query == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a " + QUERY_FIELD.getPreferredName() + " value");
        }
        this.fieldName = fieldName;
        this.query = query;
    }

    public SemanticQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.query = in.readString();
    }

    private SemanticQueryBuilder(
        SemanticQueryBuilder other,
        SetOnce<InferenceServiceResults> inferenceResultsSupplier,
        SetOnce<SemanticTextModelSettings> modelSettingsSupplier
    ) {
        this.fieldName = other.fieldName;
        this.query = other.query;
        this.boost = other.boost;
        this.queryName = other.queryName;
        this.inferenceResultsSupplier = inferenceResultsSupplier;
        this.modelSettingsSupplier = modelSettingsSupplier;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.SEMANTIC_TEXT_FIELD_ADDED;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeString(query);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(QUERY_FIELD.getPreferredName(), query);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
        builder.endObject();
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        if (inferenceResultsSupplier != null) {
            return this;
        }

        Set<String> modelsForField = queryRewriteContext.getModelsForField(fieldName);
        if (modelsForField.isEmpty()) {
            throw new IllegalArgumentException("Field [" + fieldName + "] is not a semantic_text field type");
        }

        if (modelsForField.size() > 1) {
            // TODO: Handle multi-index semantic queries
            throw new IllegalArgumentException("Field [" + fieldName + "] has multiple models associated with it");
        }

        String inferenceId = modelsForField.iterator().next();

        modelSettingsSupplier = new SetOnce<>();
        inferenceResultsSupplier = new SetOnce<>();
        queryRewriteContext.registerAsyncAction((client, listener) -> {
            InferenceProviderRegistry.getInstance(
                List.of(inferenceId),
                queryRewriteContext.getModelRegistry(),
                queryRewriteContext.getInferenceServiceRegistry(),
                listener.delegateFailureAndWrap((l, inferenceProviderRegistry) -> {
                    modelSettingsSupplier.set(new SemanticTextModelSettings(inferenceProviderRegistry.getModel(inferenceId)));
                    performInference(inferenceId, inferenceProviderRegistry, l);
                })
            );
        });

        return new SemanticQueryBuilder(this, inferenceResultsSupplier, modelSettingsSupplier);
    }

    private void performInference(String inferenceId, InferenceProviderRegistry inferenceProviderRegistry, ActionListener<?> listener) {
        inferenceProviderRegistry.getInfereceService(inferenceId)
            .infer(
                inferenceProviderRegistry.getModel(inferenceId),
                List.of(query),
                Map.of(),
                InputType.SEARCH,
                listener.delegateFailureAndWrap((l, inferenceServiceResults) -> {
                    inferenceResultsSupplier.set(inferenceServiceResults);
                    l.onResponse(null);
                })
            );
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        InferenceServiceResults inferenceServiceResults = inferenceResultsSupplier.get();
        if (inferenceServiceResults == null) {
            throw new IllegalArgumentException("Inference results supplier for field [" + fieldName + "] is empty");
        }

        List<? extends InferenceResults> inferenceResultsList = inferenceServiceResults.transformToCoordinationFormat();
        if (inferenceResultsList.isEmpty()) {
            throw new IllegalArgumentException("No inference results retrieved for field [" + fieldName + "]");
        } else if (inferenceResultsList.size() > 1) {
            // TODO: How to handle multiple inference results?
            throw new IllegalArgumentException(inferenceResultsList.size() + " inference results retrieved for field [" + fieldName + "]");
        }

        InferenceResults inferenceResults = inferenceResultsList.get(0);
        MappedFieldType fieldType = context.getFieldType(fieldName);
        if (fieldType instanceof SemanticTextFieldMapper.SemanticTextFieldType semanticTextFieldType) {
            return semanticTextFieldType.semanticQuery(inferenceResults, modelSettingsSupplier.get(), context);
        }

        // TODO: Better exception type to throw here?
        throw new IllegalArgumentException(
            "Field [" + fieldName + "] is not registered as a " + SemanticTextFieldMapper.CONTENT_TYPE + " field type"
        );
    }

    @Override
    protected boolean doEquals(SemanticQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(query, other.query)
            && Objects.equals(inferenceResultsSupplier, other.inferenceResultsSupplier);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, query, inferenceResultsSupplier);
    }

    public static SemanticQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        String query = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;

        String currentFieldName = null;
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                for (token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if (QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            query = parser.text();
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
            }
        }

        if (fieldName == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] no field name specified");
        }
        if (query == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] no query specified");
        }

        SemanticQueryBuilder queryBuilder = new SemanticQueryBuilder(fieldName, query);
        queryBuilder.queryName(queryName);
        queryBuilder.boost(boost);
        return queryBuilder;
    }
}
