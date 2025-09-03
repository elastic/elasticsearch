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
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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

    // TODO: Support multiple fields
    protected abstract String getFieldName();

    protected abstract Map<String, Float> getFields();

    protected abstract String getQuery();

    protected abstract QueryBuilder copy(Map<String, InferenceResults> inferenceResultsMap);

    protected abstract QueryBuilder querySemanticTextField(SemanticTextFieldMapper.SemanticTextFieldType semanticTextField);

    protected abstract QueryBuilder queryNonSemanticTextField(MappedFieldType fieldType);

    protected abstract boolean resolveWildcards();

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
            MappedFieldType fieldType = indexMetadataContext.getFieldType(getFieldName());
            if (fieldType == null) {
                return new MatchNoneQueryBuilder();
            } else if (fieldType instanceof SemanticTextFieldMapper.SemanticTextFieldType semanticTextField) {
                return querySemanticTextField(semanticTextField);
            } else {
                return queryNonSemanticTextField(fieldType);
            }
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
                resolveWildcards()
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

    private static Set<String> getInferenceIdsForFields(
        Collection<IndexMetadata> indexMetadataCollection,
        Set<String> fields,
        boolean resolveWildcards
    ) {
        Set<String> inferenceIds = new HashSet<>();
        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            Set<InferenceFieldMetadata> indexInferenceFields = indexMetadata.getInferenceFields()
                .entrySet()
                .stream()
                .filter(e -> InterceptedInferenceQueryBuilder.fieldMatches(e.getKey(), fields, resolveWildcards))
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
}
