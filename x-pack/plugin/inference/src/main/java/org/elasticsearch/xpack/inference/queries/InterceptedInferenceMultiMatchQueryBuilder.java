/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.inference.InferenceResults;

import java.io.IOException;
import java.util.Map;

public class InterceptedInferenceMultiMatchQueryBuilder extends InterceptedInferenceQueryBuilder<MultiMatchQueryBuilder> {
    public static final String NAME = "intercepted_inference_multi_match";
    public static final NodeFeature SEMANTIC_TEXT_SUPPORTS_MULTI_MATCH_QUERY = new NodeFeature(
        "search.semantic_text_supports_multi_match_query"
    );

    public InterceptedInferenceMultiMatchQueryBuilder(MultiMatchQueryBuilder originalQuery) {
        super(originalQuery);
    }

    public InterceptedInferenceMultiMatchQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    private InterceptedInferenceMultiMatchQueryBuilder(
        InterceptedInferenceQueryBuilder<MultiMatchQueryBuilder> other,
        Map<String, InferenceResults> inferenceResultsMap
    ) {
        super(other, inferenceResultsMap);
    }

    @Override
    protected Map<String, Float> getFields() {
        return originalQuery.fields();
    }

    @Override
    protected String getQuery() {
        return (String) originalQuery.value();
    }

    @Override
    protected QueryBuilder doRewriteBwC(QueryRewriteContext queryRewriteContext) {
        return this;
    }

    @Override
    protected QueryBuilder copy(Map<String, InferenceResults> inferenceResultsMap) {
        return new InterceptedInferenceMultiMatchQueryBuilder(this, inferenceResultsMap);
    }

    @Override
    protected QueryBuilder queryFields(
        Map<String, Float> inferenceFields,
        Map<String, Float> nonInferenceFields,
        QueryRewriteContext indexMetadataContext
    ) {
        validateQueryTypeSupported(originalQuery.type());

        // Handle case where no inference fields are present
        if (inferenceFields.isEmpty()) {
            return nonInferenceFields.isEmpty() ? new MatchNoneQueryBuilder() : originalQuery;
        }

        // Single semantic field scenario
        if (inferenceFields.size() == 1 && nonInferenceFields.isEmpty()) {
            Map.Entry<String, Float> field = inferenceFields.entrySet().iterator().next();
            SemanticQueryBuilder semanticQuery = new SemanticQueryBuilder(field.getKey(), getQuery(), null, inferenceResultsMap);

            float fieldBoost = field.getValue() != null ? field.getValue() : 1.0f;
            float finalBoost = fieldBoost * originalQuery.boost();

            return semanticQuery.boost(finalBoost).queryName(originalQuery.queryName());
        }

        // TODO: Handle multiple semantic fields and mixed scenarios
        throw new UnsupportedOperationException("Multiple semantic fields and mixed scenarios not yet implemented");
    }

    @Override
    protected boolean resolveWildcards() {
        return false;
    }

    @Override
    protected boolean useDefaultFields() {
        return false;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    private void validateQueryTypeSupported(MultiMatchQueryBuilder.Type queryType) {
        switch (queryType) {
            case CROSS_FIELDS:
                throw new IllegalArgumentException(
                    "multi_match query with type [cross_fields] is not supported for semantic_text fields. "
                        + "Use [best_fields] or [most_fields] instead."
                );
            case PHRASE:
                throw new IllegalArgumentException(
                    "multi_match query with type [phrase] is not supported for semantic_text fields. " + "Use [best_fields] instead."
                );
            case PHRASE_PREFIX:
                throw new IllegalArgumentException(
                    "multi_match query with type [phrase_prefix] is not supported for semantic_text fields. " + "Use [best_fields] instead."
                );
            case BOOL_PREFIX:
                throw new IllegalArgumentException(
                    "multi_match query with type [bool_prefix] is not supported for semantic_text fields. "
                        + "Use [best_fields] or [most_fields] instead."
                );
        }
    }
}
