/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SemanticQueryInterceptionUtils {

    private SemanticQueryInterceptionUtils() {}

    public static InferenceIndexInformationForField resolveIndicesForField(String fieldName, ResolvedIndices resolvedIndices) {
        if (resolvedIndices != null) {
            Collection<IndexMetadata> indexMetadataCollection = resolvedIndices.getConcreteLocalIndicesMetadata().values();
            List<String> inferenceIndices = new ArrayList<>();
            List<String> nonInferenceIndices = new ArrayList<>();
            for (IndexMetadata indexMetadata : indexMetadataCollection) {
                String indexName = indexMetadata.getIndex().getName();
                InferenceFieldMetadata inferenceFieldMetadata = indexMetadata.getInferenceFields().get(fieldName);
                if (inferenceFieldMetadata != null) {
                    inferenceIndices.add(indexName);
                } else {
                    nonInferenceIndices.add(indexName);
                }
            }

            return new InferenceIndexInformationForField(inferenceIndices, nonInferenceIndices);
        }
        return null;
    }

    public static QueryBuilder createSubQueryForIndices(List<String> indices, QueryBuilder queryBuilder) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(queryBuilder);
        boolQueryBuilder.filter(new TermsQueryBuilder(IndexFieldMapper.NAME, indices));
        return boolQueryBuilder;
    }

    public record InferenceIndexInformationForField(List<String> inferenceIndices, List<String> nonInferenceIndices) {}
}
