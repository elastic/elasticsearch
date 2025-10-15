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
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.search.vectors.VectorData;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class ResultDiversificationContext {
    private final String field;
    private final int numCandidates;
    private final DenseVectorFieldMapper fieldMapper;
    private final IndexVersion indexVersion;
    private final VectorData queryVector;
    private Map<Integer, VectorData> fieldVectors;

    // Field _must_ be a dense_vector type
    protected ResultDiversificationContext(
        String field,
        int numCandidates,
        VectorData queryVector,
        DenseVectorFieldMapper fieldMapper,
        IndexVersion indexVersion,
        @Nullable Map<Integer, VectorData> fieldVectors
    ) {
        this.field = field;
        this.numCandidates = numCandidates;
        this.fieldMapper = fieldMapper;
        this.indexVersion = indexVersion;
        this.queryVector = queryVector;
        this.fieldVectors = fieldVectors == null ? new HashMap<>() : fieldVectors;
    }

    public String getField() {
        return field;
    }

    public int getNumCandidates() {
        return numCandidates;
    }

    public DenseVectorFieldMapper getFieldMapper() {
        return fieldMapper;
    }

    public DenseVectorFieldMapper.ElementType getElementType() {
        return fieldMapper.fieldType().getElementType();
    }

    public IndexVersion getIndexVersion() {
        return indexVersion;
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
