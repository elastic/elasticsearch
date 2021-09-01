/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */


package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.Field;
import org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoder;

import java.io.IOException;

/**
 * VectorScriptDocValues represents docValues for dense and sparse vector fields
 */
public abstract class VectorScriptDocValues extends ScriptDocValues<BytesRef> {

    private final BinaryDocValues in;
    final Version indexVersion;
    BytesRef value;

    VectorScriptDocValues(BinaryDocValues in, Version indexVersion) {
        this.in = in;
        this.indexVersion = indexVersion;
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
        private final int dims;
        private final float[] vector;

        public DenseVectorScriptDocValues(BinaryDocValues in, Version indexVersion, int dims) {
            super(in, indexVersion);
            this.dims = dims;
            this.vector = new float[dims];
        }

        @Override
        public BytesRef get(int index) {
            throw new UnsupportedOperationException("accessing a vector field's value through 'get' or 'value' is not supported!" +
                "Use 'vectorValue' or 'magnitude' instead!'");
        }

        // package private access only for {@link ScoreScriptUtils}
        int dims() {
            return dims;
        }

        /**
         * Get dense vector's value as an array of floats
         */
        public float[] getVectorValue() {
            VectorEncoderDecoder.decodeDenseVector(value, vector);
            return vector;
        }

        /**
         * Get dense vector's magnitude
         */
        public float getMagnitude() {
            return VectorEncoderDecoder.getMagnitude(indexVersion, value);
        }

        @Override
        public BytesRef getNonPrimitiveValue() {
            return value;
        }

        @Override
        public Field<BytesRef> toField(String fieldName) {
            throw new IllegalStateException("not implemented");
        }
    }

    // not final, as it needs to be extended by Mockito for tests
    public static class SparseVectorScriptDocValues extends VectorScriptDocValues {
        public SparseVectorScriptDocValues(BinaryDocValues in, Version indexVersion) {
            super(in, indexVersion);
        }

        // package private access only for {@link ScoreScriptUtils}
        Version indexVersion() {
            return indexVersion;
        }

        @Override
        public Field<BytesRef> toField(String fieldName) {
            throw new IllegalStateException("not implemented");
        }
    }

}
