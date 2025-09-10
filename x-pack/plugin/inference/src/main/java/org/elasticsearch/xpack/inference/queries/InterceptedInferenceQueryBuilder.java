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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.QueryParserHelper;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.index.IndexSettings.DEFAULT_FIELD_SETTING;

public abstract class InterceptedInferenceQueryBuilder<T extends AbstractQueryBuilder<T>> extends AbstractQueryBuilder<
    InterceptedInferenceQueryBuilder<T>> {
    protected final T originalQuery;
    protected final Map<String, InferenceResults> inferenceResultsMap;

    protected InterceptedInferenceQueryBuilder(T originalQuery) {
        Objects.requireNonNull(originalQuery, "original query must not be null");
        this.originalQuery = originalQuery;
        this.inferenceResultsMap = null;
    }

    @SuppressWarnings("unchecked")
    protected InterceptedInferenceQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.originalQuery = (T) in.readNamedWriteable(QueryBuilder.class);
        this.inferenceResultsMap = in.readOptional(i1 -> i1.readImmutableMap(i2 -> i2.readNamedWriteable(InferenceResults.class)));
    }

    protected InterceptedInferenceQueryBuilder(
        InterceptedInferenceQueryBuilder<T> other,
        Map<String, InferenceResults> inferenceResultsMap
    ) {
        this.originalQuery = other.originalQuery;
        this.inferenceResultsMap = inferenceResultsMap;
    }

    protected abstract Map<String, Float> getFields();

    protected abstract String getQuery();

    protected abstract QueryBuilder doRewriteBwC(QueryRewriteContext queryRewriteContext);

    protected abstract QueryBuilder copy(Map<String, InferenceResults> inferenceResultsMap);

    protected abstract QueryBuilder queryFields(
        Map<String, Float> inferenceFields,
        Map<String, Float> nonInferenceFields,
        QueryRewriteContext indexMetadataContext
    );

    protected abstract boolean resolveWildcards();

    protected abstract boolean useDefaultFields();

    protected String getInferenceIdOverride() {
        return null;
    }

    protected void coordinatorNodeValidate(ResolvedIndices resolvedIndices) {}

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(originalQuery);
        out.writeOptional((o, v) -> o.writeMap(v, StreamOutput::writeNamedWriteable), inferenceResultsMap);
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
        return Objects.equals(originalQuery, other.originalQuery) && Objects.equals(inferenceResultsMap, other.inferenceResultsMap);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(originalQuery, inferenceResultsMap);
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
        if (this.inferenceResultsMap != null) {
            // TODO: Check for error inference results here?
            return this;
        }

        QueryBuilder rewrittenBwC = doRewriteBwC(queryRewriteContext);
        if (rewrittenBwC != this) {
            return rewrittenBwC;
        }

        // Validate early to prevent partial failures
        ResolvedIndices resolvedIndices = queryRewriteContext.getResolvedIndices();
        coordinatorNodeValidate(resolvedIndices);

        // NOTE: This logic misses when ccs_minimize_roundtrips=false and only a remote cluster is querying a semantic text field.
        // In this case, the remote data node will receive the original query, which will in turn result in an error about querying an
        // unsupported field type.
        Set<String> inferenceIds = getInferenceIdsForFields(
            resolvedIndices.getConcreteLocalIndicesMetadata().values(),
            getFields(),
            resolveWildcards(),
            useDefaultFields()
        );

        if (inferenceIds.isEmpty()) {
            // Not querying a semantic text field
            return originalQuery;
        }

        // TODO: Check for supported CCS mode here (once we support CCS)
        if (resolvedIndices.getRemoteClusterIndices().isEmpty() == false) {
            throw new IllegalArgumentException(
                originalQuery.getName()
                    + " query does not support cross-cluster search when querying a ["
                    + SemanticTextFieldMapper.CONTENT_TYPE
                    + "] field"
            );
        }

        String inferenceIdOverride = getInferenceIdOverride();
        if (inferenceIdOverride != null) {
            inferenceIds = Set.of(inferenceIdOverride);
        }

        // If the query is null, there's nothing to generate inference results for. This can happen if pre-computed inference results are
        // provided by the user.
        String query = getQuery();
        Map<String, InferenceResults> inferenceResultsMap = new ConcurrentHashMap<>();
        if (query != null) {
            for (String inferenceId : inferenceIds) {
                SemanticQueryBuilder.registerInferenceAsyncAction(queryRewriteContext, inferenceResultsMap, query, inferenceId);
            }
        }

        return copy(inferenceResultsMap);
    }

    private static Set<String> getInferenceIdsForFields(
        Collection<IndexMetadata> indexMetadataCollection,
        Map<String, Float> fields,
        boolean resolveWildcards,
        boolean useDefaultFields
    ) {
        Set<String> inferenceIds = new HashSet<>();
        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            final Map<String, Float> indexQueryFields = (useDefaultFields && fields.isEmpty())
                ? getDefaultFields(indexMetadata.getSettings())
                : fields;

            Map<String, InferenceFieldMetadata> indexInferenceFields = indexMetadata.getInferenceFields();
            for (String indexQueryField : indexQueryFields.keySet()) {
                if (indexInferenceFields.containsKey(indexQueryField)) {
                    // No wildcards in field name
                    InferenceFieldMetadata inferenceFieldMetadata = indexInferenceFields.get(indexQueryField);
                    inferenceIds.add(inferenceFieldMetadata.getSearchInferenceId());
                    continue;
                }
                if (resolveWildcards) {
                    if (Regex.isMatchAllPattern(indexQueryField)) {
                        indexInferenceFields.values().forEach(ifm -> inferenceIds.add(ifm.getSearchInferenceId()));
                    } else if (Regex.isSimpleMatchPattern(indexQueryField)) {
                        indexInferenceFields.values()
                            .stream()
                            .filter(ifm -> Regex.simpleMatch(indexQueryField, ifm.getName()))
                            .forEach(ifm -> inferenceIds.add(ifm.getSearchInferenceId()));
                    }
                }
            }
        }

        return inferenceIds;
    }

    private static Map<String, Float> getInferenceFieldsMap(
        QueryRewriteContext indexMetadataContext,
        Map<String, Float> queryFields,
        boolean resolveWildcards
    ) {
        Map<String, Float> inferenceFieldsToQuery = new HashMap<>();
        Map<String, InferenceFieldMetadata> indexInferenceFields = indexMetadataContext.getMappingLookup().inferenceFields();
        for (Map.Entry<String, Float> entry : queryFields.entrySet()) {
            String queryField = entry.getKey();
            Float weight = entry.getValue();

            if (indexInferenceFields.containsKey(queryField)) {
                // No wildcards in field name
                addToInferenceFieldsMap(inferenceFieldsToQuery, queryField, weight);
                continue;
            }
            if (resolveWildcards) {
                if (Regex.isMatchAllPattern(queryField)) {
                    indexInferenceFields.keySet().forEach(f -> addToInferenceFieldsMap(inferenceFieldsToQuery, f, weight));
                } else if (Regex.isSimpleMatchPattern(queryField)) {
                    indexInferenceFields.keySet()
                        .stream()
                        .filter(f -> Regex.simpleMatch(queryField, f))
                        .forEach(f -> addToInferenceFieldsMap(inferenceFieldsToQuery, f, weight));
                }
            }
        }

        return inferenceFieldsToQuery;
    }

    private static Map<String, Float> getDefaultFields(Settings settings) {
        List<String> defaultFieldsList = settings.getAsList(DEFAULT_FIELD_SETTING.getKey(), DEFAULT_FIELD_SETTING.getDefault(settings));
        return QueryParserHelper.parseFieldsAndWeights(defaultFieldsList);
    }

    private static void addToInferenceFieldsMap(Map<String, Float> inferenceFields, String field, Float weight) {
        inferenceFields.compute(field, (k, v) -> v == null ? weight : v * weight);
    }
}
