/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.inference.InferenceException;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.getMatchingInferenceFields;
import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
import static org.elasticsearch.xpack.inference.queries.InferenceQueryUtils.ccsMinimizeRoundTripsFalseSupportCheck;
import static org.elasticsearch.xpack.inference.queries.InferenceQueryUtils.getDefaultFields;
import static org.elasticsearch.xpack.inference.queries.InferenceQueryUtils.getInferenceInfo;
import static org.elasticsearch.xpack.inference.queries.InferenceQueryUtils.getResultFromFuture;
import static org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder.SEMANTIC_SEARCH_CCS_SUPPORT;
import static org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder.convertFromBwcInferenceResultsMap;

/**
 * <p>
 * An internal {@link QueryBuilder} type that associates an original query builder with a map of inference results required successfully
 * query a {@link SemanticTextFieldMapper.SemanticTextFieldType}.
 * </p>
 * <p>
 * This query builder generates the necessary inference results during coordinator node rewrite and stores them for use during data node
 * rewrite. At that time, logic specific to each original query builder type is invoked to rewrite to a query appropriate for that index's
 * mappings.
 * </p>
 *
 * @param <T> The original query builder type
 */
public abstract class InterceptedInferenceQueryBuilder<T extends AbstractQueryBuilder<T>> extends AbstractQueryBuilder<
    InterceptedInferenceQueryBuilder<T>> {

    public static final NodeFeature NEW_SEMANTIC_QUERY_INTERCEPTORS = new NodeFeature("search.new_semantic_query_interceptors");

    static final TransportVersion INFERENCE_RESULTS_MAP_WITH_CLUSTER_ALIAS = TransportVersion.fromName(
        "inference_results_map_with_cluster_alias"
    );

    protected final T originalQuery;
    protected final Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap;
    protected final PlainActionFuture<InferenceQueryUtils.InferenceInfo> inferenceInfoFuture;
    protected final boolean interceptedCcsRequest;

    protected InterceptedInferenceQueryBuilder(T originalQuery) {
        this(originalQuery, null);
    }

    protected InterceptedInferenceQueryBuilder(T originalQuery, Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap) {
        Objects.requireNonNull(originalQuery, "original query must not be null");
        this.originalQuery = originalQuery;
        this.inferenceResultsMap = inferenceResultsMap != null ? Map.copyOf(inferenceResultsMap) : null;
        this.inferenceInfoFuture = null;
        this.interceptedCcsRequest = false;
    }

    @SuppressWarnings("unchecked")
    protected InterceptedInferenceQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.originalQuery = (T) in.readNamedWriteable(QueryBuilder.class);
        if (in.getTransportVersion().supports(INFERENCE_RESULTS_MAP_WITH_CLUSTER_ALIAS)) {
            this.inferenceResultsMap = in.readOptional(
                i1 -> i1.readImmutableMap(FullyQualifiedInferenceId::new, i2 -> i2.readNamedWriteable(InferenceResults.class))
            );
        } else {
            this.inferenceResultsMap = convertFromBwcInferenceResultsMap(
                in.readOptional(i1 -> i1.readImmutableMap(i2 -> i2.readNamedWriteable(InferenceResults.class)))
            );
        }
        if (in.getTransportVersion().supports(SEMANTIC_SEARCH_CCS_SUPPORT)) {
            this.interceptedCcsRequest = in.readBoolean();
        } else {
            this.interceptedCcsRequest = false;
        }

        this.inferenceInfoFuture = null;
    }

    protected InterceptedInferenceQueryBuilder(
        InterceptedInferenceQueryBuilder<T> other,
        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap,
        PlainActionFuture<InferenceQueryUtils.InferenceInfo> inferenceInfoFuture,
        boolean interceptedCcsRequest
    ) {
        this.originalQuery = other.originalQuery;
        this.inferenceResultsMap = inferenceResultsMap;
        this.inferenceInfoFuture = inferenceInfoFuture;
        this.interceptedCcsRequest = interceptedCcsRequest;
    }

    /**
     * <p>
     * Get the fields queried by the original query.
     * </p>
     * <p>
     * Multi-field queries should return a field map, where the map value is the boost applied to that field or field pattern.
     * Single-field queries should return a single-entry field map, where the map value is 1.0.
     * </p>
     * <p>
     * Implementations should <i>always</i> return a non-null map. If no fields are specified in the original query, an empty map should be
     * returned.
     * </p>
     *
     * @return A map of the fields queried by the original query
     */
    protected abstract Map<String, Float> getFields();

    /**
     * Get the original query's query text. If not available, {@code null} should be returned.
     *
     * @return The original query's query text
     */
    protected abstract String getQuery();

    /**
     * Rewrite to a backwards-compatible form of the query builder, depending on the value of
     * {@link QueryRewriteContext#getMinTransportVersion()}. If no rewrites are required, the implementation should return {@code this}.
     *
     * @param queryRewriteContext The query rewrite context
     * @return The query builder rewritten to a backwards-compatible form
     */
    protected abstract QueryBuilder doRewriteBwC(QueryRewriteContext queryRewriteContext) throws IOException;

    /**
     * Generate a copy of {@code this}.
     *
     * @param inferenceResultsMap         The inference results map
     * @param inferenceInfoFuture         The inference info future
     * @param interceptedCcsRequest       Flag indicating if this is a CCS request
     * @return A copy of {@code this} with the provided inference results map
     */
    protected abstract QueryBuilder copy(
        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap,
        PlainActionFuture<InferenceQueryUtils.InferenceInfo> inferenceInfoFuture,
        boolean interceptedCcsRequest
    );

    /**
     * Rewrite to a {@link QueryBuilder} appropriate for a specific index's mappings. The implementation can use
     * {@code indexMetadataContext} to get the index's mappings.
     *
     * @param inferenceFields A field map of the inference fields queried in this index. Every entry will be a concrete field.
     * @param nonInferenceFields A field map of the non-inference fields queried in this index. Entries may be concrete fields or
     *                           field patterns.
     * @param indexMetadataContext The index metadata context.
     * @return A query rewritten for the index's mappings.
     */
    protected abstract QueryBuilder queryFields(
        Map<String, Float> inferenceFields,
        Map<String, Float> nonInferenceFields,
        QueryRewriteContext indexMetadataContext
    );

    /**
     * If the implementation should resolve wildcards in field patterns to inference fields.
     */
    protected abstract boolean resolveWildcards();

    /**
     * If the implementation should fall back to the {@code index.query.default_field} index setting when
     * {@link InterceptedInferenceQueryBuilder#getFields()} returns an empty map.
     */
    protected abstract boolean useDefaultFields();

    /**
     * Get the query-time inference ID override. If not applicable or available, {@code null} should be returned.
     */
    protected FullyQualifiedInferenceId getInferenceIdOverride() {
        return null;
    }

    /**
     * Perform any custom pre-inference coordinator node validation.
     *
     * @param resolvedIndices The resolved indices
     * @return A boolean flag indicating if remote cluster inference info gathering can be skipped
     */
    protected boolean preInferenceCoordinatorNodeValidate(ResolvedIndices resolvedIndices) {
        return false;
    }

    /**
     * Perform any custom post-inference coordinator node validation.
     *
     * @param inferenceInfo The inference information
     */
    protected void postInferenceCoordinatorNodeValidate(InferenceQueryUtils.InferenceInfo inferenceInfo) {}

    protected T rewriteToOriginalQuery(Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap) {
        return originalQuery;
    }

    /**
     * A hook for subclasses to do additional rewriting and inference result fetching while we are on the coordinator node.
     * An example usage is {@link InterceptedInferenceKnnVectorQueryBuilder} which needs to rewrite the knn queries filters.
     */
    protected InterceptedInferenceQueryBuilder<T> customDoRewriteGetInferenceResults(QueryRewriteContext queryRewriteContext)
        throws IOException {
        return this;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(originalQuery);
        if (out.getTransportVersion().supports(INFERENCE_RESULTS_MAP_WITH_CLUSTER_ALIAS)) {
            out.writeOptional(
                (o, v) -> o.writeMap(v, StreamOutput::writeWriteable, StreamOutput::writeNamedWriteable),
                inferenceResultsMap
            );
        } else {
            out.writeOptional((o1, v) -> o1.writeMap(v, (o2, id) -> {
                if (id.clusterAlias().equals(LOCAL_CLUSTER_GROUP_KEY) == false) {
                    throw new IllegalArgumentException("Cannot serialize remote cluster inference results in a mixed-version cluster");
                }
                o2.writeString(id.inferenceId());
            }, StreamOutput::writeNamedWriteable), inferenceResultsMap);
        }
        if (out.getTransportVersion().supports(SEMANTIC_SEARCH_CCS_SUPPORT)) {
            out.writeBoolean(interceptedCcsRequest);
        } else if (interceptedCcsRequest) {
            throw new IllegalArgumentException(
                "One or more nodes does not support "
                    + originalQuery.getName()
                    + " query cross-cluster search when querying a ["
                    + SemanticTextFieldMapper.CONTENT_TYPE
                    + "] field. Please update all nodes to at least Elasticsearch "
                    + SEMANTIC_SEARCH_CCS_SUPPORT.toReleaseVersion()
                    + "."
            );
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(getName(), originalQuery);
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) {
        throw new UnsupportedOperationException("Query should be rewritten to a different type");
    }

    @Override
    protected boolean doEquals(InterceptedInferenceQueryBuilder<T> other) {
        // Exclude inferenceInfoFuture from equality because it is transient
        return Objects.equals(originalQuery, other.originalQuery)
            && Objects.equals(inferenceResultsMap, other.inferenceResultsMap)
            && Objects.equals(interceptedCcsRequest, other.interceptedCcsRequest);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(originalQuery, inferenceResultsMap, interceptedCcsRequest);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return originalQuery.getMinimalSupportedVersion();
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryRewriteContext indexMetadataContext = queryRewriteContext.convertToIndexMetadataContext();
        if (indexMetadataContext != null) {
            // We are performing an index metadata rewrite on the data node
            return doRewriteBuildQuery(indexMetadataContext);
        }

        ResolvedIndices resolvedIndices = queryRewriteContext.getResolvedIndices();
        if (resolvedIndices != null) {
            // We are preforming a coordinator node rewrite
            return doRewriteGetInferenceResults(queryRewriteContext);
        }

        return this;
    }

    private QueryBuilder doRewriteBuildQuery(QueryRewriteContext indexMetadataContext) {
        Map<String, Float> queryFields = getFields();
        if (useDefaultFields() && queryFields.isEmpty()) {
            queryFields = getDefaultFields(indexMetadataContext.getIndexSettings().getSettings());
        }

        Map<String, Float> inferenceFieldsToQuery = getInferenceFieldsMap(indexMetadataContext, queryFields, resolveWildcards());
        Map<String, Float> nonInferenceFieldsToQuery = new HashMap<>(queryFields);
        nonInferenceFieldsToQuery.keySet().removeAll(inferenceFieldsToQuery.keySet());

        return queryFields(inferenceFieldsToQuery, nonInferenceFieldsToQuery, indexMetadataContext);
    }

    private QueryBuilder doRewriteGetInferenceResults(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder rewrittenBwC = doRewriteBwC(queryRewriteContext);
        if (rewrittenBwC != this) {
            return rewrittenBwC;
        }

        boolean alwaysSkipRemotes = preInferenceCoordinatorNodeValidate(queryRewriteContext.getResolvedIndices());
        InterceptedInferenceQueryBuilder<T> rewritten = customDoRewriteGetInferenceResults(queryRewriteContext);
        return rewritten.doRewriteWaitForInferenceResults(queryRewriteContext, alwaysSkipRemotes);
    }

    private QueryBuilder doRewriteWaitForInferenceResults(QueryRewriteContext queryRewriteContext, boolean alwaysSkipRemotes) {
        ResolvedIndices resolvedIndices = queryRewriteContext.getResolvedIndices();
        if (inferenceInfoFuture != null) {
            InferenceQueryUtils.InferenceInfo inferenceInfo = getResultFromFuture(inferenceInfoFuture);
            if (inferenceInfo == null) {
                return this;
            }

            ccsMinimizeRoundTripsFalseSupportCheck(queryRewriteContext, inferenceInfo, originalQuery.getName());
            postInferenceCoordinatorNodeValidate(inferenceInfo);

            QueryBuilder rewritten = this;
            int inferenceFieldCount = inferenceInfo.inferenceFieldCount();
            var newInferenceResultsMap = inferenceInfo.inferenceResultsMap();
            if (inferenceFieldCount == 0 && interceptedCcsRequest == false) {
                // We aren't querying any inference fields and this query wasn't intercepted in a previous coordinator node rewrite.
                // Therefore, we don't need to intercept the query.
                rewritten = rewriteToOriginalQuery(newInferenceResultsMap);
            } else if (Objects.equals(inferenceResultsMap, newInferenceResultsMap) == false) {
                inferenceResultsErrorCheck(newInferenceResultsMap);
                boolean newInterceptedCcsRequest = this.interceptedCcsRequest
                    || resolvedIndices.getRemoteClusterIndices().isEmpty() == false;

                // Keep a reference to the future so that we can check that the inference results map doesn't change in further rewrite
                // cycles
                rewritten = copy(newInferenceResultsMap, inferenceInfoFuture, newInterceptedCcsRequest);
            }
            return rewritten;
        }

        PlainActionFuture<InferenceQueryUtils.InferenceInfo> newInferenceInfoFuture = new PlainActionFuture<>();
        InferenceQueryUtils.InferenceInfoRequest inferenceInfoRequest = new InferenceQueryUtils.InferenceInfoRequest(
            getFields(),
            getQuery(),
            inferenceResultsMap,
            getInferenceIdOverride(),
            resolveWildcards(),
            useDefaultFields(),
            alwaysSkipRemotes
        );
        getInferenceInfo(queryRewriteContext, inferenceInfoRequest, newInferenceInfoFuture);

        return copy(inferenceResultsMap, newInferenceInfoFuture, interceptedCcsRequest);
    }

    private static Map<String, Float> getInferenceFieldsMap(
        QueryRewriteContext indexMetadataContext,
        Map<String, Float> queryFields,
        boolean resolveWildcards
    ) {
        Map<String, InferenceFieldMetadata> indexInferenceFields = indexMetadataContext.getMappingLookup().inferenceFields();
        Map<InferenceFieldMetadata, Float> matchingInferenceFields = getMatchingInferenceFields(
            indexInferenceFields,
            queryFields,
            resolveWildcards
        );

        return matchingInferenceFields.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().getName(), Map.Entry::getValue));
    }

    private static void inferenceResultsErrorCheck(Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap) {
        for (var entry : inferenceResultsMap.entrySet()) {
            String inferenceId = entry.getKey().inferenceId();
            InferenceResults inferenceResults = entry.getValue();

            if (inferenceResults instanceof ErrorInferenceResults errorInferenceResults) {
                // Use InferenceException here so that the status code is set by the cause
                throw new InferenceException(
                    "Inference ID [" + inferenceId + "] query inference error",
                    errorInferenceResults.getException()
                );
            } else if (inferenceResults instanceof WarningInferenceResults warningInferenceResults) {
                throw new IllegalStateException(
                    "Inference ID [" + inferenceId + "] query inference warning: " + warningInferenceResults.getWarning()
                );
            }
        }
    }
}
