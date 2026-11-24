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
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

public abstract class ResultDiversificationContext {
    private final String field;
    private final int size;
    private Supplier<VectorData> queryVector;
    private Map<Integer, VectorData> fieldVectors = null;
    private Runnable cancellationCheck = () -> {};

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

    /**
     * Sets the field vectors for this context.
     * Note that the key should be the `RankDoc` rank in the total result set
     * @param fieldVectors the vectors to set
     */
    public void setFieldVectors(Map<Integer, VectorData> fieldVectors) {
        this.fieldVectors = fieldVectors;
    }

    public VectorData getQueryVector() {
        return queryVector == null ? null : queryVector.get();
    }

    public void setQueryVector(Supplier<VectorData> queryVector) {
        this.queryVector = queryVector;
    }

    public VectorData getFieldVector(int rank) {
        return fieldVectors.getOrDefault(rank, null);
    }

    public Set<Map.Entry<Integer, VectorData>> getFieldVectorsEntrySet() {
        return fieldVectors.entrySet();
    }

    /**
     * Sets a check that throws if the search was cancelled. Diversification can compare many vectors,
     * so implementations call {@link #ensureNotCancelled()} periodically to stop early.
     */
    public void setCancellationCheck(Runnable cancellationCheck) {
        this.cancellationCheck = Objects.requireNonNull(cancellationCheck);
    }

    public void ensureNotCancelled() {
        cancellationCheck.run();
    }
}
