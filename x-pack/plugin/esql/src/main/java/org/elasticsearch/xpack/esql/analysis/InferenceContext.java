/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;

import java.util.Map;
import java.util.Set;

public class InferenceContext {
    private final Map<String, IndexMetadata> indexMetadata;
    private Set<String> indices;

    public InferenceContext(Map<String, IndexMetadata> indexMetadata) {
        this.indexMetadata = indexMetadata;
    }

    public void setIndices(Set<String> indices) {
        this.indices = indices;
    }

    public boolean hasMultipleInferenceIds(String fieldName) {
        boolean found = false;

        for (String index : indices) {
            if (indexMetadata.get(index).getInferenceFields().containsKey(fieldName)) {
                if (found) {
                    return true;
                }
                found = true;
            }
        }
        return false;
    }

    /**
     * Assumes that for a semantic_text field all indices use the same inference ID.
     * Use @hasMultipleInferenceIds to check whether a semantic_text field has different inferece IDs in the given indices.
     */
    public String semanticTextInferenceId(String fieldName) {
        for (String index : indices) {
            InferenceFieldMetadata inferenceFieldMetadata = indexMetadata.get(index).getInferenceFields().get(fieldName);
            if (inferenceFieldMetadata != null) {
                return inferenceFieldMetadata.getInferenceId();
            }
        }
        return null;
    }
}
