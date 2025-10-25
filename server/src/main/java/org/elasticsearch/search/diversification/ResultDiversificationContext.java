/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.diversification;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.vectors.VectorData;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class ResultDiversificationContext {
    private final String field;
    private final int numCandidates;
    private final VectorData queryVector;
    private Map<Integer, VectorData> fieldVectors;

    protected ResultDiversificationContext(
        String field,
        int numCandidates,
        @Nullable VectorData queryVector,
        @Nullable Map<Integer, VectorData> fieldVectors
    ) {
        this.field = field;
        this.numCandidates = numCandidates;
        this.queryVector = queryVector;
        this.fieldVectors = fieldVectors == null ? new HashMap<>() : fieldVectors;
    }

    public String getField() {
        return field;
    }

    public int getNumCandidates() {
        return numCandidates;
    }

    public void setFieldVectors(Map<Integer, VectorData> fieldVectors) {
        this.fieldVectors = fieldVectors;
    }

    public VectorData getQueryVector() {
        return queryVector;
    }

    public VectorData getFieldVector(int docId) {
        return fieldVectors.getOrDefault(docId, null);
    }

    public Set<Map.Entry<Integer, VectorData>> getFieldVectorsEntrySet() {
        return fieldVectors.entrySet();
    }
}
