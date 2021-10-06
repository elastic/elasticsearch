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

    private final BinaryDocValues in;
    private final Version indexVersion;
    private final float[] vector;
    private BytesRef value;

    BinaryDenseVectorScriptDocValues(BinaryDocValues in, Version indexVersion, int dims) {
        super(dims);
        this.in = in;
        this.indexVersion = indexVersion;
        this.vector = new float[dims];
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
    public float[] getVectorValue() {
        VectorEncoderDecoder.decodeDenseVector(value, vector);
        return vector;
    }

    @Override
    public float getMagnitude() {
        return VectorEncoderDecoder.getMagnitude(indexVersion, value);
    }

    @Override
    public int size() {
        if (value == null) {
            return 0;
        } else {
            return 1;
        }
    }

    @Override
    public BytesRef getNonPrimitiveValue() {
        return value;
    }

    @Override
    public double dotProduct(float[] queryVector) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(value.bytes, value.offset, value.length);

        double dotProduct = 0;
        for (float queryValue : queryVector) {
            dotProduct += queryValue * byteBuffer.getFloat();
        }
        return (float) dotProduct;
    }

    @Override
    public double l1Norm(float[] queryVector) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(value.bytes, value.offset, value.length);

        double l1norm = 0;
        for (float queryValue : queryVector) {
            l1norm += Math.abs(queryValue - byteBuffer.getFloat());
        }
        return l1norm;
    }

    @Override
    public double l2Norm(float[] queryVector) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(value.bytes, value.offset, value.length);
        double l2norm = 0;
        for (float queryValue : queryVector) {
            double diff = queryValue - byteBuffer.getFloat();
            l2norm += diff * diff;
        }
        return Math.sqrt(l2norm);
    }
}
