/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.query;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.ObjectParser.fromList;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction.Request.DEFAULT_TIMEOUT;

public class SemanticQueryBuilder extends AbstractQueryBuilder<SemanticQueryBuilder> {

    public static final String NAME = "semantic";

    private static final ParseField QUERY = new ParseField("query");
    private static final ParseField FIELDS = new ParseField("field_name");
    private static final ParseField EMBEDDING_MODEL = new ParseField("embedding_model");
    private static final ParseField QUERY_EMBEDDING = new ParseField("query_embedding");
    private static final ParseField INFERENCE_FIELD_NAME = new ParseField("inference_field_name");
    private static final ParseField NUM_CANDIDATES = new ParseField("num_candidates");

    private static final ObjectParser<SemanticQueryBuilder, Void> PARSER = new ObjectParser<>(NAME, SemanticQueryBuilder::new);

    static {
        PARSER.declareStringArray(fromList(String.class, SemanticQueryBuilder::setFieldNames), FIELDS);
        PARSER.declareString(SemanticQueryBuilder::setQuery, QUERY);
        PARSER.declareString(SemanticQueryBuilder::setEmbeddingModelId, EMBEDDING_MODEL);
        PARSER.declareDoubleArray(SemanticQueryBuilder::setQueryEmbedding, QUERY_EMBEDDING);
        PARSER.declareString(SemanticQueryBuilder::setInferenceFieldName, INFERENCE_FIELD_NAME);
        PARSER.declareInt(SemanticQueryBuilder::setNumCandidates, NUM_CANDIDATES);
    }

    public static SemanticQueryBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private String[] fieldNames = new String[0];
    private String query;
    private String embeddingModelId;
    private String inferenceFieldName = "text_field";
    private float[] queryEmbedding;
    private final Supplier<float[]> queryEmbeddingSupplier;
    private int numCandidates = 10;

    public SemanticQueryBuilder(StreamInput in) throws IOException {
        super(in);
        queryEmbeddingSupplier = null;
        fieldNames = in.readStringArray();
        embeddingModelId = in.readOptionalString();
        query = in.readOptionalString();
        inferenceFieldName = in.readString();
        if (in.readBoolean()) {
            queryEmbedding = in.readFloatArray();
        }
        numCandidates = in.readVInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (queryEmbeddingSupplier != null) {
            throw new IllegalStateException("supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }
        out.writeStringArray(fieldNames);
        out.writeOptionalString(embeddingModelId);
        out.writeOptionalString(query);
        out.writeString(inferenceFieldName);
        if (queryEmbedding != null) {
            out.writeBoolean(true);
            out.writeFloatArray(queryEmbedding);
        } else {
            out.writeBoolean(false);
        }
        out.writeVInt(numCandidates);
    }

    public SemanticQueryBuilder() {
        queryEmbeddingSupplier = null;
    }

    private SemanticQueryBuilder(Supplier<float[]> queryEmbeddingSupplier) {
        this.queryEmbeddingSupplier = queryEmbeddingSupplier;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public SemanticQueryBuilder setFieldNames(String... fieldNames) {
        this.fieldNames = fieldNames;
        return this;
    }

    public String getQuery() {
        return query;
    }

    public SemanticQueryBuilder setQuery(String query) {
        this.query = query;
        return this;
    }

    public String getEmbeddingModelId() {
        return embeddingModelId;
    }

    public SemanticQueryBuilder setEmbeddingModelId(String embeddingModelId) {
        this.embeddingModelId = embeddingModelId;
        return this;
    }

    private SemanticQueryBuilder setQueryEmbedding(List<Double> embedding) {
        float[] f = new float[embedding.size()];
        int i = 0;
        for (Double d : embedding) {
            f[i++] = d.floatValue();
        }
        this.queryEmbedding = f;
        return this;
    }

    public SemanticQueryBuilder setQueryEmbedding(float[] embedding) {
        this.queryEmbedding = embedding;
        return this;
    }

    public String getInferenceFieldName() {
        return inferenceFieldName;
    }

    public SemanticQueryBuilder setInferenceFieldName(String inferenceFieldName) {
        this.inferenceFieldName = inferenceFieldName;
        return this;
    }

    public int getNumCandidates() {
        return numCandidates;
    }

    public SemanticQueryBuilder setNumCandidates(int numCandidates) {
        this.numCandidates = numCandidates;
        return this;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_3_0;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray(FIELDS.getPreferredName());
        for (String field : fieldNames) {
            builder.value(field);
        }
        builder.endArray();
        if (embeddingModelId != null) {
            builder.field(EMBEDDING_MODEL.getPreferredName(), embeddingModelId);
        }
        if (query != null) {
            builder.field(QUERY.getPreferredName(), query);
        }
        builder.field(INFERENCE_FIELD_NAME.getPreferredName(), inferenceFieldName);
        if (queryEmbedding != null) {
            builder.field(QUERY_EMBEDDING.getPreferredName(), queryEmbedding);
        }
        builder.field(NUM_CANDIDATES.getPreferredName(), numCandidates);
        boostAndQueryNameToXContent(builder);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (query == null) {
            throw new IllegalArgumentException("[query] must be provided");
        }
        if (embeddingModelId == null && queryEmbedding == null) {
            throw new IllegalArgumentException("[embedding_model_id] or [query_embedding] must be provided");
        }
        if (queryEmbeddingSupplier != null) {
            final float[] embedding = queryEmbeddingSupplier.get();
            if (embedding == null) {
                throw new IllegalArgumentException("unexpected null embedding");
            }
            return new SemanticQueryBuilder().setQuery(query)
                .setEmbeddingModelId(embeddingModelId)
                .setQueryEmbedding(embedding)
                .setFieldNames(fieldNames);
        } else if (queryEmbedding == null) {
            SetOnce<float[]> supplier = new SetOnce<>();
            queryRewriteContext.registerAsyncAction((client, listener) -> {
                InferTrainedModelDeploymentAction.Request request = new InferTrainedModelDeploymentAction.Request(
                    embeddingModelId,
                    null,
                    List.of(Map.of(inferenceFieldName, query)),
                    DEFAULT_TIMEOUT
                );
                executeAsyncWithOrigin(client, ML_ORIGIN, InferTrainedModelDeploymentAction.INSTANCE, request, ActionListener.wrap(r -> {
                    if (r.getResults()instanceof TextEmbeddingResults embeddingResults) {
                        float[] f = new float[embeddingResults.getInference().length];
                        for (int i = 0; i < embeddingResults.getInference().length; i++) {
                            f[i] = (float) embeddingResults.getInference()[i];
                        }
                        supplier.set(f);
                        listener.onResponse(null);
                    } else if (r.getResults()instanceof WarningInferenceResults warningInferenceResults) {
                        listener.onFailure(
                            new IllegalArgumentException(
                                "Failed to infer model ["
                                    + embeddingModelId
                                    + "]. Inference returned warning: "
                                    + warningInferenceResults.getWarning()
                            )
                        );
                    } else {
                        listener.onFailure(
                            new IllegalArgumentException(
                                "Failed to infer model ["
                                    + embeddingModelId
                                    + "]. Inference returned unexpected results: "
                                    + r.getResults().getWriteableName()
                            )
                        );
                    }
                }, listener::onFailure));
            });
            return new SemanticQueryBuilder(supplier::get).setQuery(query).setEmbeddingModelId(embeddingModelId).setFieldNames(fieldNames);
        }
        return super.doRewrite(queryRewriteContext);
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        if (queryEmbedding == null || queryEmbeddingSupplier != null) {
            throw new UnsupportedOperationException("query must be rewritten first");
        }
        if (fieldNames.length == 0) {
            throw new UnsupportedOperationException("Must supply field_name values");
        }
        List<KnnVectorQuery> queries = new ArrayList<>();
        for (String field : fieldNames) {
            MappedFieldType fieldType = context.getFieldType(field);
            if (fieldType == null) {
                throw new IllegalArgumentException("field [" + field + "] does not exist in the mapping");
            }
            if (fieldType.typeName().equals("dense_vector") == false) {
                throw new IllegalArgumentException("[" + NAME + "] queries are only supported on [dense_vector] fields");
            }
            queries.add(new KnnVectorQuery(field, queryEmbedding, numCandidates));
        }
        if (queries.size() == 1) {
            return queries.get(0);
        } else {
            BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
            for (KnnVectorQuery q : queries) {
                booleanQuery.add(q, BooleanClause.Occur.SHOULD);
            }
            booleanQuery.setMinimumNumberShouldMatch(1);
            return booleanQuery.build();
        }
    }

    @Override
    protected boolean doEquals(SemanticQueryBuilder other) {
        return Objects.equals(other.embeddingModelId, embeddingModelId)
            && Objects.equals(query, other.query)
            && Objects.equals(numCandidates, other.numCandidates)
            && Arrays.equals(fieldNames, other.fieldNames);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, numCandidates, embeddingModelId, Arrays.hashCode(fieldNames));
    }
}
