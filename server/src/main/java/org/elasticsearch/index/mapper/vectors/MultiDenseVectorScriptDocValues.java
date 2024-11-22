/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.field.vectors.MultiDenseVector;

import java.util.Iterator;

public class MultiDenseVectorScriptDocValues extends ScriptDocValues<BytesRef> {

    public static final String MISSING_VECTOR_FIELD_MESSAGE = "A document doesn't have a value for a multi-vector field!";

    private final int dims;
    protected final MultiDenseVectorSupplier dvSupplier;

    public MultiDenseVectorScriptDocValues(MultiDenseVectorSupplier supplier, int dims) {
        super(supplier);
        this.dvSupplier = supplier;
        this.dims = dims;
    }

    public int dims() {
        return dims;
    }

    private MultiDenseVector getCheckedVector() {
        MultiDenseVector vector = dvSupplier.getInternal();
        if (vector == null) {
            throw new IllegalArgumentException(MISSING_VECTOR_FIELD_MESSAGE);
        }
        return vector;
    }

    /**
     * Get multi-dense vector's value as an array of floats
     */
    public Iterator<float[]> getVectorValues() {
        return getCheckedVector().getVectors();
    }

    /**
     * Get dense vector's magnitude
     */
    public float[] getMagnitudes() {
        return getCheckedVector().getMagnitudes();
    }

    @Override
    public BytesRef get(int index) {
        throw new UnsupportedOperationException(
            "accessing a multi-vector field's value through 'get' or 'value' is not supported, use 'vectorValues' or 'magnitudes' instead."
        );
    }

    @Override
    public int size() {
        MultiDenseVector mdv = dvSupplier.getInternal();
        if (mdv != null) {
            return mdv.size();
        }
        return 0;
    }

    public interface MultiDenseVectorSupplier extends Supplier<BytesRef> {
        @Override
        default BytesRef getInternal(int index) {
            throw new UnsupportedOperationException();
        }

        MultiDenseVector getInternal();
    }
}
