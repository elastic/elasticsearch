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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public abstract class ResultDiversificationContext {
    private final String field;
    private final int size;
    private final Supplier<VectorData> queryVector;
    private Map<Integer, List<VectorData>> fieldVectors = null;

    private boolean retrievedQueryVector = false;
    private VectorData realizedQueryVector = null;

    protected ResultDiversificationContext(String field, int size, @Nullable Supplier<VectorData> queryVector) {
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

    public int setFieldVectors(FieldVectorSupplier fieldVectorSupplier) {
        this.fieldVectors = fieldVectorSupplier.getFieldVectors();
        return this.fieldVectors.size();
    }

    public VectorData getQueryVector() {
        if (retrievedQueryVector) {
            return realizedQueryVector;
        }
        realizedQueryVector = queryVector == null ? null : queryVector.get();
        retrievedQueryVector = true;
        return realizedQueryVector;
    }

    public List<VectorData> getFieldVectorData(int rank) {
        return fieldVectors.getOrDefault(rank, null);
    }

    public Set<Map.Entry<Integer, List<VectorData>>> getFieldVectorsEntrySet() {
        return fieldVectors.entrySet();
    }
}
