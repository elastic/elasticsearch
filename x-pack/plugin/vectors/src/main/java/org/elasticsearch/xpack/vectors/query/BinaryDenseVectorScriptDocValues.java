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
import org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoder;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BinaryDenseVectorScriptDocValues extends DenseVectorScriptDocValues {

    public static class BinaryDenseVectorSupplier implements DenseVectorSupplier<BytesRef> {

        private final BinaryDocValues in;
        private BytesRef value;

        public BinaryDenseVectorSupplier(BinaryDocValues in) {
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

        @Override
        public BytesRef getInternal(int index) {
            throw new UnsupportedOperationException();
        }

        public BytesRef getInternal() {
            return value;
        }

        @Override
        public int size() {
            if (value == null) {
                return 0;
            } else {
                return 1;
            }
        }
    }

    private final BinaryDenseVectorSupplier bdvSupplier;
    private final Version indexVersion;
    private final float[] vector;

    BinaryDenseVectorScriptDocValues(BinaryDenseVectorSupplier supplier, Version indexVersion, int dims) {
        super(supplier, dims);
        this.bdvSupplier = supplier;
        this.indexVersion = indexVersion;
        this.vector = new float[dims];
    }

    @Override
    public int size() {
        return supplier.size();
    }

    @Override
    public float[] getVectorValue() {
        VectorEncoderDecoder.decodeDenseVector(bdvSupplier.getInternal(), vector);
        return vector;
    }

    @Override
    public float getMagnitude() {
        return VectorEncoderDecoder.getMagnitude(indexVersion, bdvSupplier.getInternal());
    }

    @Override
    public double dotProduct(float[] queryVector) {
        BytesRef value = bdvSupplier.getInternal();
        ByteBuffer byteBuffer = ByteBuffer.wrap(value.bytes, value.offset, value.length);

        double dotProduct = 0;
        for (float queryValue : queryVector) {
            dotProduct += queryValue * byteBuffer.getFloat();
        }
        return (float) dotProduct;
    }

    @Override
    public double l1Norm(float[] queryVector) {
        BytesRef value = bdvSupplier.getInternal();
        ByteBuffer byteBuffer = ByteBuffer.wrap(value.bytes, value.offset, value.length);

        double l1norm = 0;
        for (float queryValue : queryVector) {
            l1norm += Math.abs(queryValue - byteBuffer.getFloat());
        }
        return l1norm;
    }

    @Override
    public double l2Norm(float[] queryVector) {
        BytesRef value = bdvSupplier.getInternal();
        ByteBuffer byteBuffer = ByteBuffer.wrap(value.bytes, value.offset, value.length);
        double l2norm = 0;
        for (float queryValue : queryVector) {
            double diff = queryValue - byteBuffer.getFloat();
            l2norm += diff * diff;
        }
        return Math.sqrt(l2norm);
    }
}
