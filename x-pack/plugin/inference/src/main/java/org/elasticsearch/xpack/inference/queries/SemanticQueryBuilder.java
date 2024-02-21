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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class SemanticQueryBuilder extends AbstractQueryBuilder<SemanticQueryBuilder> {
    public static final String NAME = "semantic_query";

    private static final ParseField QUERY_FIELD = new ParseField("query");

    private final String fieldName;
    private final String query;

    private SetOnce<InferenceServiceResults> inferenceResultsSupplier;

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

    private SemanticQueryBuilder(SemanticQueryBuilder other, SetOnce<InferenceServiceResults> inferenceResultsSupplier) {
        this.fieldName = other.fieldName;
        this.query = other.query;
        this.inferenceResultsSupplier = inferenceResultsSupplier;
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
            throw new IllegalArgumentException("field [" + fieldName + "] is not a semantic_text field type");
        }

        if (modelsForField.size() > 1) {
            // TODO: Handle multi-index semantic queries
            throw new IllegalArgumentException("field [" + fieldName + "] has multiple models associated with it");
        }

        // TODO: How to determine task type?
        InferenceAction.Request inferenceRequest = new InferenceAction.Request(
            TaskType.SPARSE_EMBEDDING,
            modelsForField.iterator().next(),
            List.of(query),
            Map.of(),
            InputType.SEARCH
        );

        SetOnce<InferenceServiceResults> inferenceResultsSupplier = new SetOnce<>();
        queryRewriteContext.registerAsyncAction((client, listener) -> executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            InferenceAction.INSTANCE,
            inferenceRequest,
            listener.delegateFailureAndWrap((l, inferenceResponse) -> {
                inferenceResultsSupplier.set(inferenceResponse.getResults());
                l.onResponse(null);
            })
        ));

        return new SemanticQueryBuilder(this, inferenceResultsSupplier);
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        // TODO: Implement
        return null;
    }

    @Override
    protected boolean doEquals(SemanticQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) && Objects.equals(query, other.query);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, query);
    }
}
