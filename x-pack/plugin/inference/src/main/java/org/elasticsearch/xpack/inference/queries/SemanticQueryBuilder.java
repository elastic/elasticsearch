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
import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.inference.InferenceException;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.elasticsearch.TransportVersions.INFERENCE_RESULTS_MAP_WITH_CLUSTER_ALIAS;
import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class SemanticQueryBuilder extends AbstractQueryBuilder<SemanticQueryBuilder> {
    public static final String NAME = "semantic";

    public static final NodeFeature SEMANTIC_QUERY_MULTIPLE_INFERENCE_IDS = new NodeFeature("semantic_query.multiple_inference_ids");
    public static final NodeFeature SEMANTIC_QUERY_FILTER_FIELD_CAPS_FIX = new NodeFeature("semantic_query.filter_field_caps_fix");

    static final TransportVersion SEMANTIC_QUERY_MULTIPLE_INFERENCE_IDS_TV = TransportVersion.fromName(
        "semantic_query_multiple_inference_ids"
    );

    // Use a placeholder inference ID that will never overlap with a real inference endpoint (user-created or internal)
    private static final String PLACEHOLDER_INFERENCE_ID = "$PLACEHOLDER";

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
    private final Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap;
    private final Boolean lenient;

    public SemanticQueryBuilder(String fieldName, String query) {
        this(fieldName, query, null);
    }

    public SemanticQueryBuilder(String fieldName, String query, Boolean lenient) {
        this(fieldName, query, lenient, null);
    }

    protected SemanticQueryBuilder(
        String fieldName,
        String query,
        Boolean lenient,
        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap
    ) {
        if (fieldName == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a " + FIELD_FIELD.getPreferredName() + " value");
        }
        if (query == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a " + QUERY_FIELD.getPreferredName() + " value");
        }
        this.fieldName = fieldName;
        this.query = query;
        this.inferenceResultsMap = inferenceResultsMap != null ? Map.copyOf(inferenceResultsMap) : null;
        this.lenient = lenient;
    }

    public SemanticQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.query = in.readString();
        if (in.getTransportVersion().supports(INFERENCE_RESULTS_MAP_WITH_CLUSTER_ALIAS)) {
            this.inferenceResultsMap = in.readOptional(
                i1 -> i1.readImmutableMap(FullyQualifiedInferenceId::new, i2 -> i2.readNamedWriteable(InferenceResults.class))
            );
        } else if (in.getTransportVersion().supports(SEMANTIC_QUERY_MULTIPLE_INFERENCE_IDS_TV)) {
            this.inferenceResultsMap = convertFromBwcInferenceResultsMap(
                in.readOptional(i1 -> i1.readImmutableMap(i2 -> i2.readNamedWriteable(InferenceResults.class)))
            );
        } else {
            InferenceResults inferenceResults = in.readOptionalNamedWriteable(InferenceResults.class);
            this.inferenceResultsMap = inferenceResults != null ? buildSingleResultInferenceResultsMap(inferenceResults) : null;
            in.readBoolean(); // Discard noInferenceResults, it is no longer necessary
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.SEMANTIC_QUERY_LENIENT)) {
            this.lenient = in.readOptionalBoolean();
        } else {
            this.lenient = null;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeString(query);
        if (out.getTransportVersion().supports(INFERENCE_RESULTS_MAP_WITH_CLUSTER_ALIAS)) {
            out.writeOptional(
                (o, v) -> o.writeMap(v, StreamOutput::writeWriteable, StreamOutput::writeNamedWriteable),
                inferenceResultsMap
            );
        } else if (out.getTransportVersion().supports(SEMANTIC_QUERY_MULTIPLE_INFERENCE_IDS_TV)) {
            out.writeOptional((o1, v) -> o1.writeMap(v, (o2, id) -> {
                if (id.clusterAlias().equals(LOCAL_CLUSTER_GROUP_KEY) == false) {
                    throw new IllegalArgumentException("Cannot serialize remote cluster inference results in a mixed-version cluster");
                }
                o2.writeString(id.inferenceId());
            }, StreamOutput::writeNamedWriteable), inferenceResultsMap);
        } else {
            InferenceResults inferenceResults = null;
            if (inferenceResultsMap != null) {
                if (inferenceResultsMap.size() > 1) {
                    throw new IllegalArgumentException("Cannot query multiple inference IDs in a mixed-version cluster");
                } else if (inferenceResultsMap.size() == 1) {
                    inferenceResults = inferenceResultsMap.values().iterator().next();
                }
            }

            out.writeOptionalNamedWriteable(inferenceResults);
            out.writeBoolean(inferenceResults == null);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.SEMANTIC_QUERY_LENIENT)) {
            out.writeOptionalBoolean(lenient);
        }
    }

    private SemanticQueryBuilder(SemanticQueryBuilder other, Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap) {
        this.fieldName = other.fieldName;
        this.query = other.query;
        this.boost = other.boost;
        this.queryName = other.queryName;
        // No need to copy the map here since this is only called internally. We can safely assume that the caller will not modify the map.
        this.inferenceResultsMap = inferenceResultsMap;
        this.lenient = other.lenient;
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
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_15_0;
    }

    public static SemanticQueryBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    /**
     * <p>
     * Get inference results for the provided query using the provided inference IDs. The inference IDs are fully qualified by the
     * cluster alias in the provided {@link QueryRewriteContext}.
     * </p>
     * <p>
     * This method will return an inference results map that will be asynchronously populated with inference results. If the provided
     * inference results map already contains all required inference results, the same map instance will be returned. Otherwise, a new map
     * instance will be returned. It is guaranteed that a non-null map instance will be returned.
     * </p>
     *
     * @param queryRewriteContext The query rewrite context
     * @param inferenceIds The inference IDs to use to generate inference results
     * @param inferenceResultsMap The initial inference results map
     * @param query The query to generate inference results for
     * @return An inference results map
     */
    static Map<FullyQualifiedInferenceId, InferenceResults> getInferenceResults(
        QueryRewriteContext queryRewriteContext,
        Set<String> inferenceIds,
        @Nullable Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap,
        @Nullable String query
    ) {
        boolean modifiedInferenceResultsMap = false;
        Map<FullyQualifiedInferenceId, InferenceResults> currentInferenceResultsMap = inferenceResultsMap != null
            ? inferenceResultsMap
            : Map.of();

        if (query != null) {
            for (String inferenceId : inferenceIds) {
                FullyQualifiedInferenceId fullyQualifiedInferenceId = new FullyQualifiedInferenceId(
                    queryRewriteContext.getLocalClusterAlias(),
                    inferenceId
                );
                if (currentInferenceResultsMap.containsKey(fullyQualifiedInferenceId) == false) {
                    if (modifiedInferenceResultsMap == false) {
                        // Copy the inference results map to ensure it is mutable and thread safe
                        currentInferenceResultsMap = new ConcurrentHashMap<>(currentInferenceResultsMap);
                        modifiedInferenceResultsMap = true;
                    }

                    registerInferenceAsyncAction(
                        queryRewriteContext,
                        ((ConcurrentHashMap<FullyQualifiedInferenceId, InferenceResults>) currentInferenceResultsMap),
                        query,
                        inferenceId
                    );
                }
            }
        }

        return currentInferenceResultsMap;
    }

    static void registerInferenceAsyncAction(
        QueryRewriteContext queryRewriteContext,
        ConcurrentHashMap<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap,
        String query,
        String inferenceId
    ) {
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
                    inferenceResultsMap.put(
                        new FullyQualifiedInferenceId(queryRewriteContext.getLocalClusterAlias(), inferenceId),
                        validateAndConvertInferenceResults(inferenceResponse.getResults(), inferenceId)
                    );
                    l.onResponse(null);
                })
            )
        );
    }

    static Map<FullyQualifiedInferenceId, InferenceResults> convertFromBwcInferenceResultsMap(
        Map<String, InferenceResults> inferenceResultsMap
    ) {
        Map<FullyQualifiedInferenceId, InferenceResults> converted = null;
        if (inferenceResultsMap != null) {
            converted = Collections.unmodifiableMap(
                inferenceResultsMap.entrySet()
                    .stream()
                    .collect(Collectors.toMap(e -> new FullyQualifiedInferenceId(LOCAL_CLUSTER_GROUP_KEY, e.getKey()), Map.Entry::getValue))
            );
        }
        return converted;
    }

    /**
     * Build an inference results map to store a single inference result that is not associated with an inference ID.
     *
     * @param inferenceResults The inference result
     * @return An inference results map
     */
    static Map<FullyQualifiedInferenceId, InferenceResults> buildSingleResultInferenceResultsMap(InferenceResults inferenceResults) {
        return Map.of(new FullyQualifiedInferenceId(LOCAL_CLUSTER_GROUP_KEY, PLACEHOLDER_INFERENCE_ID), inferenceResults);
    }

    /**
     * Extract an inference result not associated with an inference ID from an inference results map. Returns null if no such inference
     * result exists in the map.
     *
     * @param inferenceResultsMap The inference results map
     * @return The inference result
     */
    private static InferenceResults getSingleInferenceResult(Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap) {
        return inferenceResultsMap.get(new FullyQualifiedInferenceId(LOCAL_CLUSTER_GROUP_KEY, PLACEHOLDER_INFERENCE_ID));
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

        ResolvedIndices resolvedIndices = queryRewriteContext.getResolvedIndices();
        if (resolvedIndices != null) {
            return doRewriteGetInferenceResults(queryRewriteContext);
        }

        return this;
    }

    private QueryBuilder doRewriteBuildSemanticQuery(SearchExecutionContext searchExecutionContext) {
        MappedFieldType fieldType = searchExecutionContext.getFieldType(fieldName);
        if (fieldType == null) {
            return new MatchNoneQueryBuilder();
        } else if (fieldType instanceof SemanticTextFieldMapper.SemanticTextFieldType semanticTextFieldType) {
            if (inferenceResultsMap == null) {
                // This should never happen, but throw on it in case it ever does
                throw new IllegalStateException(
                    "No inference results set for [" + semanticTextFieldType.typeName() + "] field [" + fieldName + "]"
                );
            }

            String inferenceId = semanticTextFieldType.getSearchInferenceId();
            InferenceResults inferenceResults = getSingleInferenceResult(inferenceResultsMap);
            if (inferenceResults == null) {
                inferenceResults = inferenceResultsMap.get(
                    new FullyQualifiedInferenceId(searchExecutionContext.getLocalClusterAlias(), inferenceId)
                );
            }

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
        ResolvedIndices resolvedIndices = queryRewriteContext.getResolvedIndices();
        if (resolvedIndices.getRemoteClusterIndices().isEmpty() == false) {
            throw new IllegalArgumentException(NAME + " query does not support cross-cluster search");
        }

        SemanticQueryBuilder rewritten = this;
        if (queryRewriteContext.hasAsyncActions() == false) {
            Set<String> inferenceIds = getInferenceIdsForForField(resolvedIndices.getConcreteLocalIndicesMetadata().values(), fieldName);
            Map<FullyQualifiedInferenceId, InferenceResults> modifiedInferenceResultsMap = getInferenceResults(
                queryRewriteContext,
                inferenceIds,
                inferenceResultsMap,
                query
            );

            if (modifiedInferenceResultsMap == inferenceResultsMap) {
                // The inference results map is fully populated, so we can perform error checking
                inferenceResultsErrorCheck(modifiedInferenceResultsMap);
            } else {
                rewritten = new SemanticQueryBuilder(this, modifiedInferenceResultsMap);
            }
        }

        return rewritten;
    }

    private static InferenceResults validateAndConvertInferenceResults(
        InferenceServiceResults inferenceServiceResults,
        String inferenceId
    ) {
        List<? extends InferenceResults> inferenceResultsList = inferenceServiceResults.transformToCoordinationFormat();
        if (inferenceResultsList.isEmpty()) {
            return new ErrorInferenceResults(
                new IllegalArgumentException("No query inference results retrieved for inference ID [" + inferenceId + "]")
            );
        } else if (inferenceResultsList.size() > 1) {
            // We don't chunk queries, so there should always be one inference result.
            // Thus, if we receive more than one inference result, it is a server-side error.
            return new ErrorInferenceResults(
                new IllegalStateException(
                    inferenceResultsList.size() + " query inference results retrieved for inference ID [" + inferenceId + "]"
                )
            );
        }

        InferenceResults inferenceResults = inferenceResultsList.getFirst();
        if (inferenceResults instanceof TextExpansionResults == false
            && inferenceResults instanceof MlTextEmbeddingResults == false
            && inferenceResults instanceof ErrorInferenceResults == false
            && inferenceResults instanceof WarningInferenceResults == false) {
            return new ErrorInferenceResults(
                new IllegalArgumentException(
                    "Expected query inference results to be of type ["
                        + TextExpansionResults.NAME
                        + "] or ["
                        + MlTextEmbeddingResults.NAME
                        + "], got ["
                        + inferenceResults.getWriteableName()
                        + "]. Has the inference endpoint ["
                        + inferenceId
                        + "] configuration changed?"
                )
            );
        }

        return inferenceResults;
    }

    private void inferenceResultsErrorCheck(Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap) {
        for (var entry : inferenceResultsMap.entrySet()) {
            String inferenceId = entry.getKey().inferenceId();
            InferenceResults inferenceResults = entry.getValue();

            if (inferenceResults instanceof ErrorInferenceResults errorInferenceResults) {
                // Use InferenceException here so that the status code is set by the cause
                throw new InferenceException(
                    "Field [" + fieldName + "] with inference ID [" + inferenceId + "] query inference error",
                    errorInferenceResults.getException()
                );
            } else if (inferenceResults instanceof WarningInferenceResults warningInferenceResults) {
                throw new IllegalStateException(
                    "Field ["
                        + fieldName
                        + "] with inference ID ["
                        + inferenceId
                        + "] query inference warning: "
                        + warningInferenceResults.getWarning()
                );
            }
        }
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
            && Objects.equals(inferenceResultsMap, other.inferenceResultsMap);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, query, inferenceResultsMap);
    }
}
