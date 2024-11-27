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
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class SemanticQueryBuilder extends AbstractQueryBuilder<SemanticQueryBuilder> {
    // **** THE semantic_text.inner_hits CLUSTER FEATURE IS DEFUNCT, NEVER USE IT ****
    public static final NodeFeature SEMANTIC_TEXT_INNER_HITS = new NodeFeature("semantic_text.inner_hits");

    public static final String NAME = "semantic";

    private static final ParseField FIELD_FIELD = new ParseField("field");
    private static final ParseField QUERY_FIELD = new ParseField("query");

    private static final ConstructingObjectParser<SemanticQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        args -> new SemanticQueryBuilder((String) args[0], (String) args[1])
    );

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareString(constructorArg(), QUERY_FIELD);
        declareStandardFields(PARSER);
    }

    private final String fieldName;
    private final String query;
    private final SetOnce<InferenceServiceResults> inferenceResultsSupplier;
    private final InferenceResults inferenceResults;
    private final boolean noInferenceResults;

    public SemanticQueryBuilder(String fieldName, String query) {
        if (fieldName == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a " + FIELD_FIELD.getPreferredName() + " value");
        }
        if (query == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a " + QUERY_FIELD.getPreferredName() + " value");
        }
        this.fieldName = fieldName;
        this.query = query;
        this.inferenceResults = null;
        this.inferenceResultsSupplier = null;
        this.noInferenceResults = false;
    }

    public SemanticQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.query = in.readString();
        this.inferenceResults = in.readOptionalNamedWriteable(InferenceResults.class);
        this.noInferenceResults = in.readBoolean();
        this.inferenceResultsSupplier = null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (inferenceResultsSupplier != null) {
            throw new IllegalStateException("Inference results supplier is set. Missing a rewriteAndFetch?");
        }
        out.writeString(fieldName);
        out.writeString(query);
        out.writeOptionalNamedWriteable(inferenceResults);
        out.writeBoolean(noInferenceResults);
    }

    private SemanticQueryBuilder(
        SemanticQueryBuilder other,
        SetOnce<InferenceServiceResults> inferenceResultsSupplier,
        InferenceResults inferenceResults,
        boolean noInferenceResults
    ) {
        this.fieldName = other.fieldName;
        this.query = other.query;
        this.boost = other.boost;
        this.queryName = other.queryName;
        this.inferenceResultsSupplier = inferenceResultsSupplier;
        this.inferenceResults = inferenceResults;
        this.noInferenceResults = noInferenceResults;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_15_0;
    }

    public static SemanticQueryBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(FIELD_FIELD.getPreferredName(), fieldName);
        builder.field(QUERY_FIELD.getPreferredName(), query);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        SearchExecutionContext searchExecutionContext = queryRewriteContext.convertToSearchExecutionContext();
        if (searchExecutionContext != null) {
            return doRewriteBuildSemanticQuery(searchExecutionContext);
        }

        return doRewriteGetInferenceResults(queryRewriteContext);
    }

    private QueryBuilder doRewriteBuildSemanticQuery(SearchExecutionContext searchExecutionContext) {
        MappedFieldType fieldType = searchExecutionContext.getFieldType(fieldName);
        if (fieldType == null) {
            return new MatchNoneQueryBuilder();
        } else if (fieldType instanceof SemanticTextFieldMapper.SemanticTextFieldType semanticTextFieldType) {
            if (inferenceResults == null) {
                // This should never happen, but throw on it in case it ever does
                throw new IllegalStateException(
                    "No inference results set for [" + semanticTextFieldType.typeName() + "] field [" + fieldName + "]"
                );
            }

            return semanticTextFieldType.semanticQuery(inferenceResults, searchExecutionContext.requestSize(), boost(), queryName());
        } else {
            throw new IllegalArgumentException(
                "Field [" + fieldName + "] of type [" + fieldType.typeName() + "] does not support " + NAME + " queries"
            );
        }
    }

    private SemanticQueryBuilder doRewriteGetInferenceResults(QueryRewriteContext queryRewriteContext) {
        if (inferenceResults != null || noInferenceResults) {
            return this;
        }

        if (inferenceResultsSupplier != null) {
            InferenceResults inferenceResults = validateAndConvertInferenceResults(inferenceResultsSupplier, fieldName);
            return inferenceResults != null ? new SemanticQueryBuilder(this, null, inferenceResults, noInferenceResults) : this;
        }

        ResolvedIndices resolvedIndices = queryRewriteContext.getResolvedIndices();
        if (resolvedIndices == null) {
            throw new IllegalStateException(
                "Rewriting on the coordinator node requires a query rewrite context with non-null resolved indices"
            );
        } else if (resolvedIndices.getRemoteClusterIndices().isEmpty() == false) {
            throw new IllegalArgumentException(NAME + " query does not support cross-cluster search");
        }

        String inferenceId = getInferenceIdForForField(resolvedIndices.getConcreteLocalIndicesMetadata().values(), fieldName);
        SetOnce<InferenceServiceResults> inferenceResultsSupplier = new SetOnce<>();
        boolean noInferenceResults = false;
        if (inferenceId != null) {
            InferenceAction.Request inferenceRequest = new InferenceAction.Request(
                TaskType.ANY,
                inferenceId,
                null,
                List.of(query),
                Map.of(),
                InputType.SEARCH,
                InferModelAction.Request.DEFAULT_TIMEOUT_FOR_API,
                false
            );

            queryRewriteContext.registerAsyncAction(
                (client, listener) -> executeAsyncWithOrigin(
                    client,
                    ML_ORIGIN,
                    InferenceAction.INSTANCE,
                    inferenceRequest,
                    listener.delegateFailureAndWrap((l, inferenceResponse) -> {
                        inferenceResultsSupplier.set(inferenceResponse.getResults());
                        l.onResponse(null);
                    })
                )
            );
        } else {
            // The inference ID can be null if either the field name or index name(s) are invalid (or both).
            // If this happens, we set the "no inference results" flag to true so the rewrite process can continue.
            // Invalid index names will be handled in the transport layer, when the query is sent to the shard.
            // Invalid field names will be handled when the query is re-written on the shard, where we have access to the index mappings.
            noInferenceResults = true;
        }

        return new SemanticQueryBuilder(this, noInferenceResults ? null : inferenceResultsSupplier, null, noInferenceResults);
    }

    private static InferenceResults validateAndConvertInferenceResults(
        SetOnce<InferenceServiceResults> inferenceResultsSupplier,
        String fieldName
    ) {
        InferenceServiceResults inferenceServiceResults = inferenceResultsSupplier.get();
        if (inferenceServiceResults == null) {
            return null;
        }

        List<? extends InferenceResults> inferenceResultsList = inferenceServiceResults.transformToCoordinationFormat();
        if (inferenceResultsList.isEmpty()) {
            throw new IllegalArgumentException("No inference results retrieved for field [" + fieldName + "]");
        } else if (inferenceResultsList.size() > 1) {
            // The inference call should truncate if the query is too large.
            // Thus, if we receive more than one inference result, it is a server-side error.
            throw new IllegalStateException(inferenceResultsList.size() + " inference results retrieved for field [" + fieldName + "]");
        }

        InferenceResults inferenceResults = inferenceResultsList.get(0);
        if (inferenceResults instanceof ErrorInferenceResults errorInferenceResults) {
            throw new IllegalStateException(
                "Field [" + fieldName + "] query inference error: " + errorInferenceResults.getException().getMessage(),
                errorInferenceResults.getException()
            );
        } else if (inferenceResults instanceof WarningInferenceResults warningInferenceResults) {
            throw new IllegalStateException("Field [" + fieldName + "] query inference warning: " + warningInferenceResults.getWarning());
        } else if (inferenceResults instanceof TextExpansionResults == false
            && inferenceResults instanceof MlTextEmbeddingResults == false) {
                throw new IllegalArgumentException(
                    "Field ["
                        + fieldName
                        + "] expected query inference results to be of type ["
                        + TextExpansionResults.NAME
                        + "] or ["
                        + MlTextEmbeddingResults.NAME
                        + "], got ["
                        + inferenceResults.getWriteableName()
                        + "]. Has the inference endpoint configuration changed?"
                );
            }

        return inferenceResults;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        throw new IllegalStateException(NAME + " should have been rewritten to another query type");
    }

    private static String getInferenceIdForForField(Collection<IndexMetadata> indexMetadataCollection, String fieldName) {
        String inferenceId = null;
        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            InferenceFieldMetadata inferenceFieldMetadata = indexMetadata.getInferenceFields().get(fieldName);
            String indexInferenceId = inferenceFieldMetadata != null ? inferenceFieldMetadata.getSearchInferenceId() : null;
            if (indexInferenceId != null) {
                if (inferenceId != null && inferenceId.equals(indexInferenceId) == false) {
                    throw new IllegalArgumentException("Field [" + fieldName + "] has multiple inference IDs associated with it");
                }

                inferenceId = indexInferenceId;
            }
        }

        return inferenceId;
    }

    @Override
    protected boolean doEquals(SemanticQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(query, other.query)
            && Objects.equals(inferenceResults, other.inferenceResults);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, query, inferenceResults);
    }
}
