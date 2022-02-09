/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.script.field.DocValuesField;

public abstract class DenseVectorDocValuesField implements DocValuesField<Float>, DenseVectorScriptDocValues.DenseVectorSupplier {
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
     * Get the DenseVector for a document if one exists, DenseVector.EMPTY otherwise
     */
    public abstract DenseVector get();

    public abstract DenseVector get(DenseVector defaultValue);

    public abstract DenseVectorScriptDocValues getScriptDocValues();
}
