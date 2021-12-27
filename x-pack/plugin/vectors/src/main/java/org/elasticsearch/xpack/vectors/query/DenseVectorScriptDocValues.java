/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.ScriptDocValues;

public abstract class DenseVectorScriptDocValues extends ScriptDocValues<BytesRef> {

    public interface DenseVectorSupplier<T> extends Supplier<BytesRef> {

        @Override
        default BytesRef getInternal(int index) {
            throw new UnsupportedOperationException();
        }

        T getInternal();
    }

    public static final String MISSING_VECTOR_FIELD_MESSAGE = "A document doesn't have a value for a vector field!";

    private final int dims;

    public DenseVectorScriptDocValues(DenseVectorSupplier<?> supplier, int dims) {
        super(supplier);
        this.dims = dims;
    }

    public int dims() {
        return dims;
    }

    /**
     * Get dense vector's value as an array of floats
     */
    public abstract float[] getVectorValue();

    /**
     * Get dense vector's magnitude
     */
    public abstract float getMagnitude();

    public abstract double dotProduct(float[] queryVector);

    public abstract double l1Norm(float[] queryVector);

    public abstract double l2Norm(float[] queryVector);

    @Override
    public BytesRef get(int index) {
        throw new UnsupportedOperationException(
            "accessing a vector field's value through 'get' or 'value' is not supported!" + "Use 'vectorValue' or 'magnitude' instead!'"
        );
    }

    public static DenseVectorScriptDocValues empty(DenseVectorSupplier<?> supplier, int dims) {
        return new DenseVectorScriptDocValues(supplier, dims) {
            @Override
            public float[] getVectorValue() {
                throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
            }

            @Override
            public float getMagnitude() {
                throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
            }

            @Override
            public double dotProduct(float[] queryVector) {
                throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
            }

            @Override
            public double l1Norm(float[] queryVector) {
                throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
            }

            @Override
            public double l2Norm(float[] queryVector) {
                throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
            }

            @Override
            public int size() {
                return supplier.size();
            }
        };
    }
}
