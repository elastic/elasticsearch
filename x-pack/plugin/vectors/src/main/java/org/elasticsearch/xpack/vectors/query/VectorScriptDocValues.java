/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.ScriptDocValues;

import java.io.IOException;

/**
 * VectorScriptDocValues represents docValues for dense and sparse vector fields
 */
public abstract class VectorScriptDocValues extends ScriptDocValues<BytesRef> {

    private final BinaryDocValues in;
    private BytesRef value;

    VectorScriptDocValues(BinaryDocValues in) {
        this.in = in;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        if (in.advanceExact(docId)) {
            value = in.binaryValue();
        } else {
            value = null;
        }
    }

    // package private access only for {@link ScoreScriptUtils}
    BytesRef getEncodedValue() {
        return value;
    }

    @Override
    public BytesRef get(int index) {
        throw new UnsupportedOperationException("accessing a vector field's value through 'get' or 'value' is not supported");
    }

    @Override
    public int size() {
        if (value == null) {
            return 0;
        } else {
            return 1;
        }
    }

    // not final, as it needs to be extended by Mockito for tests
    public static class DenseVectorScriptDocValues extends VectorScriptDocValues {
        public DenseVectorScriptDocValues(BinaryDocValues in) {
            super(in);
        }
    }

    // not final, as it needs to be extended by Mockito for tests
    public static class SparseVectorScriptDocValues extends VectorScriptDocValues {
        public SparseVectorScriptDocValues(BinaryDocValues in) {
            super(in);
        }
    }

}
