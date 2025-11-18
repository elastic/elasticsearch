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

import java.util.Map;
import java.util.Set;

public abstract class ResultDiversificationContext {
    private final String field;
    private final int size;
    private final VectorData queryVector;
    private Map<Integer, VectorData> fieldVectors = null;

    protected ResultDiversificationContext(String field, int size, @Nullable VectorData queryVector) {
        this.field = field;
        this.size = size;
        this.queryVector = queryVector;
    }

    public String getField() {
        return field;
    }

    public int getSize() {
        return size;
    }

    /**
     * Sets the field vectors for this context.
     * Note that the key should be the `RankDoc` rank in the total result set
     * @param fieldVectors the vectors to set
     */
    public void setFieldVectors(Map<Integer, VectorData> fieldVectors) {
        this.fieldVectors = fieldVectors;
    }

    public VectorData getQueryVector() {
        return queryVector;
    }

    public VectorData getFieldVector(int rank) {
        return fieldVectors.getOrDefault(rank, null);
    }

    public Set<Map.Entry<Integer, VectorData>> getFieldVectorsEntrySet() {
        return fieldVectors.entrySet();
    }
}
