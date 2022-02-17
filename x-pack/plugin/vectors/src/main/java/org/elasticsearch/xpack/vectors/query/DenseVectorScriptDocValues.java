/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.ScriptDocValues;

public class DenseVectorScriptDocValues extends ScriptDocValues<BytesRef> {

    public static final String MISSING_VECTOR_FIELD_MESSAGE = "A document doesn't have a value for a vector field!";

    private final int dims;
    protected final DenseVectorSupplier dvSupplier;

    public DenseVectorScriptDocValues(DenseVectorSupplier supplier, int dims) {
        super(supplier);
        this.dvSupplier = supplier;
        this.dims = dims;
    }

    public int dims() {
        return dims;
    }

    private DenseVector getCheckedVector() {
        DenseVector vector = dvSupplier.getInternal();
        if (vector == null) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }
        return vector;
    }

    /**
     * Get dense vector's value as an array of floats
     */
    public float[] getVectorValue() {
        return getCheckedVector().getVector();
    }

    /**
     * Get dense vector's magnitude
     */
    public float getMagnitude() {
        return getCheckedVector().getMagnitude();
    }

    public double dotProduct(float[] queryVector) {
        return getCheckedVector().dotProduct(queryVector);
    }

    public double l1Norm(float[] queryVector) {
        return getCheckedVector().l1Norm(queryVector);
    }

    public double l2Norm(float[] queryVector) {
        return getCheckedVector().l2Norm(queryVector);
    }

    @Override
    public BytesRef get(int index) {
        throw new UnsupportedOperationException(
            "accessing a vector field's value through 'get' or 'value' is not supported, use 'vectorValue' or 'magnitude' instead."
        );
    }

    @Override
    public int size() {
        return dvSupplier.getInternal() == null ? 0 : 1;
    }

    public interface DenseVectorSupplier extends Supplier<BytesRef> {
        @Override
        default BytesRef getInternal(int index) {
            throw new UnsupportedOperationException();
        }

        DenseVector getInternal();
    }
}
