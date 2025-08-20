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
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public abstract class InterceptedQueryBuilder<T extends AbstractQueryBuilder<T>> extends AbstractQueryBuilder<InterceptedQueryBuilder<T>> {
    protected T originalQuery;
    protected EmbeddingsProvider embeddingsProvider;

    protected InterceptedQueryBuilder(T originalQuery) {
        Objects.requireNonNull(originalQuery, "original query must not be null");
        this.originalQuery = originalQuery;
        this.embeddingsProvider = null;
    }

    protected InterceptedQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.originalQuery = in.readNamedWriteable(originalQueryClass());
        this.embeddingsProvider = in.readOptionalNamedWriteable(EmbeddingsProvider.class);
    }

    protected InterceptedQueryBuilder(InterceptedQueryBuilder<T> other, EmbeddingsProvider embeddingsProvider) {
        this.originalQuery = other.originalQuery;
        this.embeddingsProvider = embeddingsProvider;
    }

    protected abstract Class<T> originalQueryClass();

    // TODO: Support multiple fields
    protected abstract String getFieldName();

    protected abstract String getQuery();

    protected abstract QueryBuilder copy(EmbeddingsProvider embeddingsProvider);

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(originalQuery);
        out.writeOptionalNamedWriteable(embeddingsProvider);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(getName(), originalQuery);
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        throw new UnsupportedOperationException("Query should be rewritten to a different type");
    }

    @Override
    protected boolean doEquals(InterceptedQueryBuilder<T> other) {
        return Objects.equals(originalQuery, other.originalQuery) && Objects.equals(embeddingsProvider, other.embeddingsProvider);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(originalQuery, embeddingsProvider);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.CCS_COMPATIBLE_QUERY_INTERCEPTORS;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryRewriteContext indexMetadataContext = queryRewriteContext.convertToIndexMetadataContext();
        if (indexMetadataContext != null) {
            // We are performing an index metadata rewrite on the data node
            MappedFieldType fieldType = indexMetadataContext.getFieldType(getFieldName());
            if (fieldType == null) {
                return new MatchNoneQueryBuilder();
            } else if (fieldType instanceof SemanticTextFieldMapper.SemanticTextFieldType) {
                return new SemanticQueryBuilder(getFieldName(), getQuery(), null, embeddingsProvider);
            } else {
                // TODO: Sometimes need to modify the original query before returning it
                // For example, need to add embeddings to knn query
                return originalQuery;
            }
        }

        ResolvedIndices resolvedIndices = queryRewriteContext.getResolvedIndices();
        if (resolvedIndices != null) {
            // We are preforming a coordinator node rewrite
            if (this.embeddingsProvider != null) {
                return this;
            }

            // TODO: Need to get query-time inference ID from knn & sparse_vector queries
            Set<String> inferenceIds = getInferenceIdsForForField(
                resolvedIndices.getConcreteLocalIndicesMetadata().values(),
                getFieldName()
            );

            // TODO: There are still holes in this logic. Need to handle if only remote side queries a semantic text field when
            // ccs_minimize_roundtrips=false
            if (inferenceIds.isEmpty() == false
                && resolvedIndices.getRemoteClusterIndices().isEmpty() == false
                && queryRewriteContext.isCcsMinimizeRoundtrips() != true) {
                throw new IllegalArgumentException(
                    "When querying a ["
                        + SemanticTextFieldMapper.CONTENT_TYPE
                        + "] field, "
                        + getName()
                        + " query supports CCS only when ccs_minimize_roundtrips=true"
                );
            }

            // NOTE: This logic only works when ccs_minimize_roundtrips=true. It assumes that the remote cluster will perform a new
            // coordinator node rewrite, which will re-intercept the query and determine if a semantic text field is being queried.
            // When ccs_minimize_roundtrips=false and only a remote cluster is querying a semantic text field, it will result in the remote
            // data node receiving the naked original query, which will in turn result in an error about an unsupported field type.
            // Should we always wrap the query in a InterceptedQueryBuilder so that we can handle this case more gracefully?
            if (inferenceIds.isEmpty()) {
                // Not querying a semantic text field
                return originalQuery;
            }

            MapEmbeddingsProvider embeddingsProvider = new MapEmbeddingsProvider();
            for (String inferenceId : inferenceIds) {
                SemanticQueryBuilder.registerInferenceAsyncAction(
                    queryRewriteContext,
                    embeddingsProvider,
                    getFieldName(),
                    getQuery(),
                    inferenceId
                );
            }

            return copy(embeddingsProvider);
        }

        return this;
    }

    // TODO: Support multiple fields
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
}
