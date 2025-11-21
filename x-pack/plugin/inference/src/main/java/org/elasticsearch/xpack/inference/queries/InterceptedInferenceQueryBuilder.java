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
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.QueryParserHelper;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.inference.InferenceException;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.getMatchingInferenceFields;
import static org.elasticsearch.index.IndexSettings.DEFAULT_FIELD_SETTING;
import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
import static org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder.SEMANTIC_SEARCH_CCS_SUPPORT;
import static org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder.convertFromBwcInferenceResultsMap;
import static org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder.getInferenceResults;
import static org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder.getNewInferenceResultsFromSupplier;

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
    protected final SetOnce<Map<FullyQualifiedInferenceId, InferenceResults>> inferenceResultsMapSupplier;
    protected final boolean ccsRequest;

    protected InterceptedInferenceQueryBuilder(T originalQuery) {
        this(originalQuery, null);
    }

    protected InterceptedInferenceQueryBuilder(T originalQuery, Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap) {
        Objects.requireNonNull(originalQuery, "original query must not be null");
        this.originalQuery = originalQuery;
        this.inferenceResultsMap = inferenceResultsMap != null ? Map.copyOf(inferenceResultsMap) : null;
        this.inferenceResultsMapSupplier = null;
        this.ccsRequest = false;
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
            this.ccsRequest = in.readBoolean();
        } else {
            this.ccsRequest = false;
        }

        this.inferenceResultsMapSupplier = null;
    }

    protected InterceptedInferenceQueryBuilder(
        InterceptedInferenceQueryBuilder<T> other,
        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap,
        SetOnce<Map<FullyQualifiedInferenceId, InferenceResults>> inferenceResultsMapSupplier,
        boolean ccsRequest
    ) {
        this.originalQuery = other.originalQuery;
        this.inferenceResultsMap = inferenceResultsMap;
        this.inferenceResultsMapSupplier = inferenceResultsMapSupplier;
        this.ccsRequest = ccsRequest;
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
    protected abstract QueryBuilder doRewriteBwC(QueryRewriteContext queryRewriteContext);

    /**
     * Generate a copy of {@code this}.
     *
     * @param inferenceResultsMap The inference results map
     * @param inferenceResultsMapSupplier The inference results map supplier
     * @param ccsRequest Flag indicating if this is a CCS request
     * @return A copy of {@code this} with the provided inference results map
     */
    protected abstract QueryBuilder copy(
        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap,
        SetOnce<Map<FullyQualifiedInferenceId, InferenceResults>> inferenceResultsMapSupplier,
        boolean ccsRequest
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
     * Perform any custom coordinator node validation. This is executed prior to generating inference results.
     *
     * @param resolvedIndices The resolved indices
     */
    protected void coordinatorNodeValidate(ResolvedIndices resolvedIndices) {}

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (inferenceResultsMapSupplier != null) {
            throw new IllegalStateException(
                "inferenceResultsMapSupplier must be null, can't serialize suppliers, missing a rewriteAndFetch?"
            );
        }

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
            out.writeBoolean(ccsRequest);
        } else if (ccsRequest) {
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
        return Objects.equals(originalQuery, other.originalQuery)
            && Objects.equals(inferenceResultsMap, other.inferenceResultsMap)
            && Objects.equals(inferenceResultsMapSupplier, other.inferenceResultsMapSupplier)
            && Objects.equals(ccsRequest, other.ccsRequest);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(originalQuery, inferenceResultsMap, inferenceResultsMapSupplier, ccsRequest);
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

    private QueryBuilder doRewriteGetInferenceResults(QueryRewriteContext queryRewriteContext) {
        QueryBuilder rewrittenBwC = doRewriteBwC(queryRewriteContext);
        if (rewrittenBwC != this) {
            return rewrittenBwC;
        }

        // NOTE: This logic misses when ccs_minimize_roundtrips=false and only a remote cluster is querying a semantic text field.
        // In this case, the remote data node will receive the original query, which will in turn result in an error about querying an
        // unsupported field type.
        ResolvedIndices resolvedIndices = queryRewriteContext.getResolvedIndices();
        Set<FullyQualifiedInferenceId> inferenceIds = getInferenceIdsForFields(
            resolvedIndices.getConcreteLocalIndicesMetadata().values(),
            queryRewriteContext.getLocalClusterAlias(),
            getFields(),
            resolveWildcards(),
            useDefaultFields()
        );

        // If we are handling a CCS request, always retain the intercepted query logic so that we can get inference results generated on
        // the local cluster from the inference results map when rewriting on remote cluster data nodes. This can be necessary when:
        // - A query specifies an inference ID override
        // - Only non-inference fields are queried on the remote cluster
        if (inferenceIds.isEmpty() && this.ccsRequest == false) {
            // Not querying a semantic text field
            return originalQuery;
        }

        // Validate early to prevent partial failures
        coordinatorNodeValidate(resolvedIndices);

        boolean ccsRequest = this.ccsRequest || resolvedIndices.getRemoteClusterIndices().isEmpty() == false;
        if (ccsRequest && queryRewriteContext.isCcsMinimizeRoundTrips() == false) {
            throw new IllegalArgumentException(
                originalQuery.getName()
                    + " query does not support cross-cluster search when querying a ["
                    + SemanticTextFieldMapper.CONTENT_TYPE
                    + "] field when [ccs_minimize_roundtrips] is false"
            );
        }

        if (inferenceResultsMapSupplier != null) {
            // Additional inference results have already been requested, and we are waiting for them to continue the rewrite process
            return getNewInferenceResultsFromSupplier(inferenceResultsMapSupplier, this, m -> copy(m, null, ccsRequest));
        }

        FullyQualifiedInferenceId inferenceIdOverride = getInferenceIdOverride();
        if (inferenceIdOverride != null) {
            inferenceIds = Set.of(inferenceIdOverride);
        }

        SetOnce<Map<FullyQualifiedInferenceId, InferenceResults>> newInferenceResultsMapSupplier = getInferenceResults(
            queryRewriteContext,
            inferenceIds,
            inferenceResultsMap,
            getQuery()
        );

        QueryBuilder rewritten = this;
        if (newInferenceResultsMapSupplier == null) {
            // No additional inference results are required
            if (inferenceResultsMap != null) {
                // The inference results map is fully populated, so we can perform error checking
                inferenceResultsErrorCheck(inferenceResultsMap);
            } else {
                // No inference results have been collected yet, indicating we don't need any to rewrite this query.
                // This can happen when pre-computed inference results are provided by the user.
                // Set an empty inference results map so that rewriting can continue.
                rewritten = copy(Map.of(), null, ccsRequest);
            }
        } else {
            rewritten = copy(inferenceResultsMap, newInferenceResultsMapSupplier, ccsRequest);
        }

        return rewritten;
    }

    private static Set<FullyQualifiedInferenceId> getInferenceIdsForFields(
        Collection<IndexMetadata> indexMetadataCollection,
        String clusterAlias,
        Map<String, Float> fields,
        boolean resolveWildcards,
        boolean useDefaultFields
    ) {
        Set<FullyQualifiedInferenceId> fullyQualifiedInferenceIds = new HashSet<>();
        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            final Map<String, Float> indexQueryFields = (useDefaultFields && fields.isEmpty())
                ? getDefaultFields(indexMetadata.getSettings())
                : fields;

            Map<String, InferenceFieldMetadata> indexInferenceFields = indexMetadata.getInferenceFields();
            for (String indexQueryField : indexQueryFields.keySet()) {
                if (indexInferenceFields.containsKey(indexQueryField)) {
                    // No wildcards in field name
                    InferenceFieldMetadata inferenceFieldMetadata = indexInferenceFields.get(indexQueryField);
                    fullyQualifiedInferenceIds.add(
                        new FullyQualifiedInferenceId(clusterAlias, inferenceFieldMetadata.getSearchInferenceId())
                    );
                    continue;
                }
                if (resolveWildcards) {
                    if (Regex.isMatchAllPattern(indexQueryField)) {
                        indexInferenceFields.values()
                            .forEach(
                                ifm -> fullyQualifiedInferenceIds.add(
                                    new FullyQualifiedInferenceId(clusterAlias, ifm.getSearchInferenceId())
                                )
                            );
                    } else if (Regex.isSimpleMatchPattern(indexQueryField)) {
                        indexInferenceFields.values()
                            .stream()
                            .filter(ifm -> Regex.simpleMatch(indexQueryField, ifm.getName()))
                            .forEach(
                                ifm -> fullyQualifiedInferenceIds.add(
                                    new FullyQualifiedInferenceId(clusterAlias, ifm.getSearchInferenceId())
                                )
                            );
                    }
                }
            }
        }

        return fullyQualifiedInferenceIds;
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

    private static Map<String, Float> getDefaultFields(Settings settings) {
        List<String> defaultFieldsList = settings.getAsList(DEFAULT_FIELD_SETTING.getKey(), DEFAULT_FIELD_SETTING.getDefault(settings));
        return QueryParserHelper.parseFieldsAndWeights(defaultFieldsList);
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
