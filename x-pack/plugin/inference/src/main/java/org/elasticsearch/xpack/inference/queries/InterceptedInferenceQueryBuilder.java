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
import org.elasticsearch.common.io.stream.Writeable;
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
import java.util.stream.Collectors;

import static org.elasticsearch.index.IndexSettings.DEFAULT_FIELD_SETTING;

public abstract class InterceptedInferenceQueryBuilder<T extends AbstractQueryBuilder<T>> extends AbstractQueryBuilder<
    InterceptedInferenceQueryBuilder<T>> {
    protected final T originalQuery;
    protected final Map<String, InferenceResults> inferenceResultsMap;

    /**
     * Per-index map of inference field info
     * Outer map:
     *   Key: index name
     *   Value: Inference field info map
     * Inner map:
     *   Key: Inference field name
     *   Value: Inference field info
     */
    protected final Map<String, Map<String, InferenceFieldInfo>> inferenceFieldInfoMap;

    protected InterceptedInferenceQueryBuilder(T originalQuery) {
        Objects.requireNonNull(originalQuery, "original query must not be null");
        this.originalQuery = originalQuery;
        this.inferenceResultsMap = null;
        this.inferenceFieldInfoMap = null;
    }

    @SuppressWarnings("unchecked")
    protected InterceptedInferenceQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.originalQuery = (T) in.readNamedWriteable(QueryBuilder.class);
        this.inferenceResultsMap = in.readOptional(i1 -> i1.readImmutableMap(i2 -> i2.readNamedWriteable(InferenceResults.class)));
        this.inferenceFieldInfoMap = in.readOptional(i1 -> i1.readImmutableMap(i2 -> i2.readImmutableMap(InferenceFieldInfo::new)));
    }

    protected InterceptedInferenceQueryBuilder(
        InterceptedInferenceQueryBuilder<T> other,
        Map<String, InferenceResults> inferenceResultsMap,
        Map<String, Map<String, InferenceFieldInfo>> inferenceFieldInfoMap
    ) {
        this.originalQuery = other.originalQuery;
        this.inferenceResultsMap = inferenceResultsMap;
        this.inferenceFieldInfoMap = inferenceFieldInfoMap;
    }

    protected abstract Map<String, Float> getFields();

    protected abstract String getQuery();

    protected abstract QueryBuilder copy(
        Map<String, InferenceResults> inferenceResultsMap,
        Map<String, Map<String, InferenceFieldInfo>> inferenceFieldInfoMap
    );

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
        out.writeOptional((o1, v1) -> o1.writeMap(v1, (o2, v2) -> o2.writeMap(v2, StreamOutput::writeWriteable)), inferenceFieldInfoMap);
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
            && Objects.equals(inferenceFieldInfoMap, other.inferenceFieldInfoMap);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(originalQuery, inferenceResultsMap, inferenceFieldInfoMap);
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
            String indexName = indexMetadataContext.getFullyQualifiedIndex().getName();
            Map<String, InferenceFieldInfo> indexInferenceFieldInfoMap = inferenceFieldInfoMap.get(indexName);
            if (indexInferenceFieldInfoMap == null) {
                throw new IllegalStateException("No inference field info for index [" + indexName + "]");
            }

            Map<String, Float> inferenceFieldsToQuery = indexInferenceFieldInfoMap.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getWeight()));

            Map<String, Float> nonInferenceFieldsToQuery = getFields();
            if (useDefaultFields() && nonInferenceFieldsToQuery.isEmpty()) {
                nonInferenceFieldsToQuery = getDefaultFields(indexMetadataContext.getIndexSettings().getSettings());
            }
            nonInferenceFieldsToQuery.keySet().removeAll(inferenceFieldsToQuery.keySet());

            return queryFields(inferenceFieldsToQuery, nonInferenceFieldsToQuery, indexMetadataContext);
        }

        ResolvedIndices resolvedIndices = queryRewriteContext.getResolvedIndices();
        if (resolvedIndices != null) {
            // We are preforming a coordinator node rewrite
            if (this.inferenceResultsMap != null && this.inferenceFieldInfoMap != null) {
                return this;
            }

            // Validate early to prevent partial failures
            coordinatorNodeValidate(resolvedIndices);

            Map<String, Map<String, InferenceFieldInfo>> inferenceFieldInfoMap = new HashMap<>();
            Set<String> inferenceIds = getInferenceIdsForFields(
                resolvedIndices.getConcreteLocalIndicesMetadata().values(),
                getFields(),
                resolveWildcards(),
                useDefaultFields(),
                inferenceFieldInfoMap
            );

            // TODO: Check for supported CCS mode here (once we support CCS)

            // NOTE: This logic only works when ccs_minimize_roundtrips=true. It assumes that the remote cluster will perform a new
            // coordinator node rewrite, which will re-intercept the query and determine if a semantic text field is being queried.
            // When ccs_minimize_roundtrips=false and only a remote cluster is querying a semantic text field, it will result in the remote
            // data node receiving the naked original query, which will in turn result in an error about an unsupported field type.
            // Should we always wrap the query in a InterceptedQueryBuilder so that we can handle this case more gracefully?
            if (inferenceIds.isEmpty()) {
                // Not querying a semantic text field
                return originalQuery;
            }

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

            // If the query is null, there's nothing to generate embeddings for. This can happen if pre-computed embeddings are provided
            // by the user.
            String query = getQuery();
            Map<String, InferenceResults> inferenceResultsMap = new ConcurrentHashMap<>();
            if (query != null) {
                for (String inferenceId : inferenceIds) {
                    SemanticQueryBuilder.registerInferenceAsyncAction(queryRewriteContext, inferenceResultsMap, query, inferenceId);
                }
            }

            return copy(inferenceResultsMap, inferenceFieldInfoMap);
        }

        return this;
    }

    private static Set<String> getInferenceIdsForFields(
        Collection<IndexMetadata> indexMetadataCollection,
        Map<String, Float> fields,
        boolean resolveWildcards,
        boolean useDefaultFields,
        Map<String, Map<String, InferenceFieldInfo>> inferenceFieldInfoMap
    ) {
        Set<String> inferenceIds = new HashSet<>();
        for (IndexMetadata indexMetadata : indexMetadataCollection) {
            // To support CCS, we will need to add a prefix to the index name to differentiate local cluster indices
            // from remote cluster indices
            // TODO: Add that now in preparation for CCS support?
            final String indexName = indexMetadata.getIndex().getName();
            final Map<String, Float> indexQueryFields = (useDefaultFields && fields.isEmpty())
                ? getDefaultFields(indexMetadata.getSettings())
                : fields;

            Map<String, InferenceFieldMetadata> indexInferenceFields = indexMetadata.getInferenceFields();
            Map<String, InferenceFieldInfo> indexInferenceFieldInfoMap = new HashMap<>();
            for (Map.Entry<String, Float> entry : indexQueryFields.entrySet()) {
                String indexQueryField = entry.getKey();
                Float weight = entry.getValue();

                if (indexInferenceFields.containsKey(indexQueryField)) {
                    // No wildcards in field name
                    InferenceFieldMetadata inferenceFieldMetadata = indexInferenceFields.get(indexQueryField);
                    inferenceIds.add(inferenceFieldMetadata.getSearchInferenceId());
                    addToInnerInferenceFieldsInfoMap(
                        indexInferenceFieldInfoMap,
                        inferenceFieldMetadata.getName(),
                        inferenceFieldMetadata.getSearchInferenceId(),
                        weight
                    );
                    continue;
                }
                if (resolveWildcards) {
                    if (Regex.isMatchAllPattern(indexQueryField)) {
                        indexInferenceFields.values().forEach(ifm -> {
                            inferenceIds.add(ifm.getSearchInferenceId());
                            addToInnerInferenceFieldsInfoMap(indexInferenceFieldInfoMap, ifm.getName(), ifm.getSearchInferenceId(), weight);
                        });
                    } else if (Regex.isSimpleMatchPattern(indexQueryField)) {
                        indexInferenceFields.values()
                            .stream()
                            .filter(ifm -> Regex.simpleMatch(indexQueryField, ifm.getName()))
                            .forEach(ifm -> {
                                inferenceIds.add(ifm.getSearchInferenceId());
                                addToInnerInferenceFieldsInfoMap(
                                    indexInferenceFieldInfoMap,
                                    ifm.getName(),
                                    ifm.getSearchInferenceId(),
                                    weight
                                );
                            });
                    }
                }
            }

            if (indexInferenceFieldInfoMap.isEmpty() == false) {
                inferenceFieldInfoMap.put(indexName, indexInferenceFieldInfoMap);
            }
        }

        return inferenceIds;
    }

    private static Map<String, Float> getDefaultFields(Settings settings) {
        List<String> defaultFieldsList = settings.getAsList(DEFAULT_FIELD_SETTING.getKey(), DEFAULT_FIELD_SETTING.getDefault(settings));
        return QueryParserHelper.parseFieldsAndWeights(defaultFieldsList);
    }

    private static void addToInnerInferenceFieldsInfoMap(
        Map<String, InferenceFieldInfo> innerInferenceFieldInfoMap,
        String field,
        String inferenceId,
        Float weight
    ) {
        innerInferenceFieldInfoMap.compute(
            field,
            (k, v) -> v == null ? new InferenceFieldInfo(inferenceId, weight) : v.updateWeight(weight)
        );
    }

    protected static class InferenceFieldInfo implements Writeable {
        private final String inferenceId;
        private Float weight;

        protected InferenceFieldInfo(String inferenceId, Float weight) {
            this.inferenceId = inferenceId;
            this.weight = weight;
        }

        protected InferenceFieldInfo(StreamInput in) throws IOException {
            this(in.readString(), in.readFloat());
        }

        protected InferenceFieldInfo updateWeight(Float weight) {
            this.weight = this.weight * weight;
            return this;
        }

        public String getInferenceId() {
            return inferenceId;
        }

        public Float getWeight() {
            return weight;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(inferenceId);
            out.writeFloat(weight);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InferenceFieldInfo that = (InferenceFieldInfo) o;
            return Float.compare(weight, that.weight) == 0 && Objects.equals(inferenceId, that.inferenceId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(inferenceId, weight);
        }
    }
}
