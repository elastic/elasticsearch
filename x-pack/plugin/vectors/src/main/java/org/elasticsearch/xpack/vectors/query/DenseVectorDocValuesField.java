/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.script.field.DocValuesField;

import java.io.IOException;
import java.util.Iterator;

public abstract class DenseVectorDocValuesField implements DocValuesField<Double>, DenseVectorSupplier {
    protected final String name;

    public DenseVectorDocValuesField(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int size() {
        return isEmpty() ? 0 : 1;
    }

    @Override
    public BytesRef getInternal(int index) {
        throw new UnsupportedOperationException();
    }

    /**
     * Get the DenseVector for a document for an empty dense vector
     */
    public abstract DenseVector get();

    public abstract DenseVector get(DenseVector defaultValue);

    public abstract DenseVectorScriptDocValues getScriptDocValues();

    public static DenseVectorDocValuesField empty(String name, int dims) {
        return new DenseVectorDocValuesField(name) {
            private final DenseVector vector = DenseVector.EMPTY;

            @Override
            public void setNextDocId(int docId) throws IOException {
                // do nothing
            }

            @Override
            public DenseVectorScriptDocValues getScriptDocValues() {
                return new DenseVectorScriptDocValues(this, dims);
            }

            @Override
            public boolean isEmpty() {
                return true;
            }

            @Override
            public Iterator<Double> iterator() {
                return vector.iterator();
            }

            @Override
            public DenseVector getInternal() {
                return null;
            }

            @Override
            public DenseVector get() {
                return vector;
            }

            @Override
            public DenseVector get(DenseVector defaultValue) {
                return defaultValue;
            }
        };
    }
}
