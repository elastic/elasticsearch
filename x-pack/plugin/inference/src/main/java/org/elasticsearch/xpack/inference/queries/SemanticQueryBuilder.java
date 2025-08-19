/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.FilteredCCSQueryBuilder;
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
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

// TODO: Remove noInferenceResults

public class SemanticQueryBuilder extends AbstractQueryBuilder<SemanticQueryBuilder>
    implements
        FilteredCCSQueryBuilder<SemanticQueryBuilder> {
    public static final String NAME = "semantic";

    private static final ParseField FIELD_FIELD = new ParseField("field");
    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField LENIENT_FIELD = new ParseField("lenient");

    private static final ConstructingObjectParser<SemanticQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        args -> new SemanticQueryBuilder((String) args[0], (String) args[1], (Boolean) args[2])
    );

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareString(constructorArg(), QUERY_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), LENIENT_FIELD);
        declareStandardFields(PARSER);
    }

    private final String fieldName;
    private final String query;
    private final EmbeddingsProvider embeddingsProvider;
    private final boolean noInferenceResults;
    private final Boolean lenient;

    private Supplier<ModelRegistry> modelRegistrySupplier = () -> null;

    public SemanticQueryBuilder(String fieldName, String query) {
        this(fieldName, query, null);
    }

    public SemanticQueryBuilder(String fieldName, String query, Boolean lenient) {
        if (fieldName == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a " + FIELD_FIELD.getPreferredName() + " value");
        }
        if (query == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a " + QUERY_FIELD.getPreferredName() + " value");
        }
        this.fieldName = fieldName;
        this.query = query;
        this.embeddingsProvider = null;
        this.noInferenceResults = false;
        this.lenient = lenient;
    }

    public SemanticQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.query = in.readString();
        if (in.getTransportVersion().onOrAfter(TransportVersions.SEMANTIC_QUERY_MULTIPLE_INFERENCE_IDS)) {
            this.embeddingsProvider = in.readOptionalNamedWriteable(EmbeddingsProvider.class);
        } else {
            InferenceResults inferenceResults = in.readOptionalNamedWriteable(InferenceResults.class);
            if (inferenceResults != null) {
                this.embeddingsProvider = new SingleEmbeddingsProvider(inferenceResults);
            } else {
                this.embeddingsProvider = null;
            }
        }
        this.noInferenceResults = in.readBoolean();
        if (in.getTransportVersion().onOrAfter(TransportVersions.SEMANTIC_QUERY_LENIENT)) {
            this.lenient = in.readOptionalBoolean();
        } else {
            this.lenient = null;
        }
    }

    public void setModelRegistrySupplier(Supplier<ModelRegistry> supplier) {
        modelRegistrySupplier = supplier;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeString(query);
        if (out.getTransportVersion().onOrAfter(TransportVersions.SEMANTIC_QUERY_MULTIPLE_INFERENCE_IDS)) {
            out.writeOptionalNamedWriteable(embeddingsProvider);
        } else {
            // TODO: Handle multiple inference IDs in a mixed-version cluster
            throw new UnsupportedOperationException("Handle old transport versions");
        }
        out.writeBoolean(noInferenceResults);
        if (out.getTransportVersion().onOrAfter(TransportVersions.SEMANTIC_QUERY_LENIENT)) {
            out.writeOptionalBoolean(lenient);
        }
    }

    private SemanticQueryBuilder(SemanticQueryBuilder other, EmbeddingsProvider embeddingsProvider, boolean noInferenceResults) {
        this.fieldName = other.fieldName;
        this.query = other.query;
        this.boost = other.boost;
        this.queryName = other.queryName;
        this.embeddingsProvider = embeddingsProvider;
        this.noInferenceResults = noInferenceResults;
        this.lenient = other.lenient;
        this.modelRegistrySupplier = other.modelRegistrySupplier;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getQuery() {
        return query;
    }

    @Override
    public SemanticQueryBuilder filter() {
        // Clear any attached embeddings
        return new SemanticQueryBuilder(this, null, false);
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
        if (lenient != null) {
            builder.field(LENIENT_FIELD.getPreferredName(), lenient);
        }
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
            if (embeddingsProvider == null) {
                // This should never happen, but throw on it in case it ever does
                throw new IllegalStateException(
                    "No inference results set for [" + semanticTextFieldType.typeName() + "] field [" + fieldName + "]"
                );
            }

            ModelRegistry modelRegistry = modelRegistrySupplier.get();
            if (modelRegistry == null) {
                throw new IllegalStateException("Model registry has not been set");
            }

            String inferenceId = semanticTextFieldType.getSearchInferenceId();
            InferenceResults inferenceResults = embeddingsProvider.getEmbeddings(inferenceId);

            // TODO: Handle ErrorInferenceResults and WarningInferenceResults
            if (inferenceResults == null) {
                throw new IllegalStateException(
                    "No inference results set for ["
                        + semanticTextFieldType.typeName()
                        + "] field ["
                        + fieldName
                        + "] with inference ID ["
                        + inferenceId
                        + "]"
                );
            }

            return semanticTextFieldType.semanticQuery(inferenceResults, searchExecutionContext.requestSize(), boost(), queryName());
        } else if (lenient != null && lenient) {
            return new MatchNoneQueryBuilder();
        } else {
            throw new IllegalArgumentException(
                "Field [" + fieldName + "] of type [" + fieldType.typeName() + "] does not support " + NAME + " queries"
            );
        }
    }

    private SemanticQueryBuilder doRewriteGetInferenceResults(QueryRewriteContext queryRewriteContext) {
        // Check that we are performing a coordinator node rewrite
        // TODO: Clean up how we perform this check
        if (queryRewriteContext.getClass() != QueryRewriteContext.class) {
            return this;
        }

        ResolvedIndices resolvedIndices = queryRewriteContext.getResolvedIndices();
        if (resolvedIndices == null) {
            throw new IllegalStateException(
                "Rewriting on the coordinator node requires a query rewrite context with non-null resolved indices"
            );
        } else if (resolvedIndices.getRemoteClusterIndices().isEmpty() == false) {
            if (queryRewriteContext.isCcsMinimizeRoundtrips() != true) {
                throw new IllegalArgumentException(NAME + " query supports CCS only when ccs_minimize_roundtrips=true");
            }
        }

        MapEmbeddingsProvider currentEmbeddingsProvider;
        if (embeddingsProvider != null) {
            if (embeddingsProvider instanceof MapEmbeddingsProvider mapEmbeddingsProvider) {
                currentEmbeddingsProvider = mapEmbeddingsProvider;
            } else {
                throw new IllegalStateException("Current embeddings provider should be a MapEmbeddingsProvider");
            }
        } else {
            currentEmbeddingsProvider = new MapEmbeddingsProvider();
        }

        boolean modified = false;
        if (queryRewriteContext.hasAsyncActions() == false) {
            ModelRegistry modelRegistry = modelRegistrySupplier.get();
            if (modelRegistry == null) {
                throw new IllegalStateException("Model registry has not been set");
            }

            Set<String> inferenceIds = getInferenceIdsForForField(resolvedIndices.getConcreteLocalIndicesMetadata().values(), fieldName);
            for (String inferenceId : inferenceIds) {
                if (currentEmbeddingsProvider.getEmbeddings(inferenceId) == null) {
                    InferenceAction.Request inferenceRequest = new InferenceAction.Request(
                        TaskType.ANY,
                        inferenceId,
                        null,
                        null,
                        null,
                        List.of(query),
                        Map.of(),
                        InputType.INTERNAL_SEARCH,
                        null,
                        false
                    );

                    queryRewriteContext.registerAsyncAction(
                        (client, listener) -> executeAsyncWithOrigin(
                            client,
                            ML_ORIGIN,
                            InferenceAction.INSTANCE,
                            inferenceRequest,
                            listener.delegateFailureAndWrap((l, inferenceResponse) -> {
                                currentEmbeddingsProvider.addEmbeddings(
                                    inferenceId,
                                    validateAndConvertInferenceResults(inferenceResponse.getResults(), fieldName, inferenceId)
                                );
                                l.onResponse(null);
                            })
                        )
                    );

                    modified = true;
                }
            }
        }

        return modified ? new SemanticQueryBuilder(this, currentEmbeddingsProvider, false) : this;
    }

    private static InferenceResults validateAndConvertInferenceResults(
        InferenceServiceResults inferenceServiceResults,
        String fieldName,
        String inferenceId
    ) {
        List<? extends InferenceResults> inferenceResultsList = inferenceServiceResults.transformToCoordinationFormat();
        if (inferenceResultsList.isEmpty()) {
            return new ErrorInferenceResults(
                new IllegalArgumentException(
                    "No inference results retrieved for field [" + fieldName + "] with inference ID [" + inferenceId + "]"
                )
            );
        } else if (inferenceResultsList.size() > 1) {
            // The inference call should truncate if the query is too large.
            // Thus, if we receive more than one inference result, it is a server-side error.
            return new ErrorInferenceResults(
                new IllegalStateException(
                    inferenceResultsList.size()
                        + " inference results retrieved for field ["
                        + fieldName
                        + "] with inference ID ["
                        + inferenceId
                        + "]"
                )
            );
        }

        InferenceResults inferenceResults = inferenceResultsList.get(0);
        if (inferenceResults instanceof TextExpansionResults == false
            && inferenceResults instanceof MlTextEmbeddingResults == false
            && inferenceResults instanceof ErrorInferenceResults == false
            && inferenceResults instanceof WarningInferenceResults == false) {
            return new ErrorInferenceResults(
                new IllegalArgumentException(
                    "Field ["
                        + fieldName
                        + "] with inference ID ["
                        + inferenceId
                        + "] expected query inference results to be of type ["
                        + TextExpansionResults.NAME
                        + "] or ["
                        + MlTextEmbeddingResults.NAME
                        + "], got ["
                        + inferenceResults.getWriteableName()
                        + "]. Has the inference endpoint configuration changed?"
                )
            );
        }

        return inferenceResults;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        throw new IllegalStateException(NAME + " should have been rewritten to another query type");
    }

    private static Set<String> getInferenceIdsForForField(Collection<IndexMetadata> indexMetadataCollection, String fieldName) {
        Set<String> inferenceIds = new HashSet<>();
        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            InferenceFieldMetadata inferenceFieldMetadata = indexMetadata.getInferenceFields().get(fieldName);
            String indexInferenceId = inferenceFieldMetadata != null ? inferenceFieldMetadata.getSearchInferenceId() : null;
            if (indexInferenceId != null) {
                inferenceIds.add(indexInferenceId);
            }
        }

        return inferenceIds;
    }

    @Override
    protected boolean doEquals(SemanticQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(query, other.query)
            && Objects.equals(embeddingsProvider, other.embeddingsProvider)
            && Objects.equals(noInferenceResults, other.noInferenceResults)
            && Objects.equals(lenient, other.lenient);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, query, embeddingsProvider, noInferenceResults, lenient);
    }
}
