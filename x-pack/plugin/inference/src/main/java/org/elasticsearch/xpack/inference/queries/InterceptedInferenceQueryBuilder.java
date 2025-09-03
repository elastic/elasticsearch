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
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.QueryParserHelper;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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

    protected abstract QueryBuilder copy(Map<String, InferenceResults> inferenceResultsMap);

    protected abstract QueryBuilder queryFields(FieldsInfo fieldsInfo, QueryRewriteContext indexMetadataContext);

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
        // Use 8.15 here to indicate compatibility since the semantic query was introduced
        return TransportVersions.V_8_15_0;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryRewriteContext indexMetadataContext = queryRewriteContext.convertToIndexMetadataContext();
        if (indexMetadataContext != null) {
            // We are performing an index metadata rewrite on the data node
            FieldsInfo fieldsInfo = buildFieldsInfo(indexMetadataContext);
            return queryFields(fieldsInfo, indexMetadataContext);
        }

        ResolvedIndices resolvedIndices = queryRewriteContext.getResolvedIndices();
        if (resolvedIndices != null) {
            // We are preforming a coordinator node rewrite
            if (this.inferenceResultsMap != null) {
                return this;
            }

            // Validate early to prevent partial failures
            coordinatorNodeValidate(resolvedIndices);

            Set<String> inferenceIds = getInferenceIdsForFields(
                resolvedIndices.getConcreteLocalIndicesMetadata().values(),
                getFields().keySet(),
                resolveWildcards(),
                useDefaultFields()
            );

            // TODO: Check for supported CCS mode here

            // NOTE: This logic only works when ccs_minimize_roundtrips=true. It assumes that the remote cluster will perform a new
            // coordinator node rewrite, which will re-intercept the query and determine if a semantic text field is being queried.
            // When ccs_minimize_roundtrips=false and only a remote cluster is querying a semantic text field, it will result in the remote
            // data node receiving the naked original query, which will in turn result in an error about an unsupported field type.
            // Should we always wrap the query in a InterceptedQueryBuilder so that we can handle this case more gracefully?
            if (inferenceIds.isEmpty()) {
                // Not querying a semantic text field
                return originalQuery;
            }

            String inferenceIdOverride = getInferenceIdOverride();
            if (inferenceIdOverride != null) {
                inferenceIds = Set.of(inferenceIdOverride);
            }

            // If the query is null, there's nothing to generate embeddings for. This can happen if pre-computed embeddings are provided
            // by the user.
            String query = getQuery();
            Map<String, InferenceResults> inferenceResultsMap = new ConcurrentHashMap<>();
            if (query != null) {
                for (String inferenceId : inferenceIds) {
                    SemanticQueryBuilder.registerInferenceAsyncAction(queryRewriteContext, inferenceResultsMap, query, inferenceId);
                }
            }

            return copy(inferenceResultsMap);
        }

        return this;
    }

    private FieldsInfo buildFieldsInfo(QueryRewriteContext indexMetadataContext) {
        Map<String, Float> queryFields = getFields();
        if (useDefaultFields() && queryFields.isEmpty()) {
            queryFields = getDefaultFields(indexMetadataContext.getIndexSettings().getSettings());
        }

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
            if (resolveWildcards()) {
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

        Map<String, Float> nonInferenceFieldsToQuery = new HashMap<>(queryFields);
        nonInferenceFieldsToQuery.keySet().removeAll(inferenceFieldsToQuery.keySet());

        return new FieldsInfo(inferenceFieldsToQuery, nonInferenceFieldsToQuery);
    }

    private static Set<String> getInferenceIdsForFields(
        Collection<IndexMetadata> indexMetadataCollection,
        Set<String> fields,
        boolean resolveWildcards,
        boolean useDefaultFields
    ) {
        Set<String> inferenceIds = new HashSet<>();
        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            final Set<String> indexQueryFields = (useDefaultFields && fields.isEmpty())
                ? getDefaultFields(indexMetadata.getSettings()).keySet()
                : fields;
            Set<InferenceFieldMetadata> indexInferenceFields = indexMetadata.getInferenceFields()
                .entrySet()
                .stream()
                .filter(e -> fieldMatches(e.getKey(), indexQueryFields, resolveWildcards))
                .map(Map.Entry::getValue)
                .collect(Collectors.toUnmodifiableSet());

            indexInferenceFields.forEach(e -> inferenceIds.add(e.getSearchInferenceId()));
        }

        return inferenceIds;
    }

    private static boolean fieldMatches(String inferenceField, Set<String> queryFields, boolean resolveWildcards) {
        boolean match = queryFields.contains(inferenceField);
        if (match == false && resolveWildcards) {
            match = queryFields.stream()
                .anyMatch(s -> Regex.isMatchAllPattern(s) || (Regex.isSimpleMatchPattern(s) && Regex.simpleMatch(s, inferenceField)));
        }

        return match;
    }

    private static Map<String, Float> getDefaultFields(Settings settings) {
        List<String> defaultFieldsList = settings.getAsList(DEFAULT_FIELD_SETTING.getKey(), DEFAULT_FIELD_SETTING.getDefault(settings));
        return QueryParserHelper.parseFieldsAndWeights(defaultFieldsList);
    }

    private static void addToInferenceFieldsMap(Map<String, Float> inferenceFields, String field, Float weight) {
        inferenceFields.compute(field, (k, v) -> v == null ? weight : v * weight);
    }

    protected record FieldsInfo(Map<String, Float> nonInferenceFields, Map<String, Float> inferenceFields) {
        protected FieldsInfo(Map<String, Float> nonInferenceFields, Map<String, Float> inferenceFields) {
            this.nonInferenceFields = Collections.unmodifiableMap(nonInferenceFields);
            this.inferenceFields = Collections.unmodifiableMap(inferenceFields);
        }
    }
}
