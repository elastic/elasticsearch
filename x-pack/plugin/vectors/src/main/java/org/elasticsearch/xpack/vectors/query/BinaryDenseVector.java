/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoder;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

public class BinaryDenseVector implements DenseVector {
    protected BytesRef value;
    protected final float[] vector;
    protected final Version indexVersion;

    public BinaryDenseVector(BytesRef value, int dims, Version indexVersion) {
        this.value = value;
        this.indexVersion = indexVersion;
        this.vector = new float[dims];
    }

    @Override
    public float[] getVector() {
        VectorEncoderDecoder.decodeDenseVector(value, vector);
        return vector;
    }

    @Override
    public float getMagnitude() {
        return VectorEncoderDecoder.getMagnitude(indexVersion, value);
    }

    @Override
    public double dotProduct(QueryVector queryVector) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(value.bytes, value.offset, value.length);

        double dotProduct = 0;
        /*for (double queryValue : queryVector) {
            dotProduct += queryValue * byteBuffer.getFloat();
        }*/
        for (int i = 0; i < queryVector.size(); i++) {
            dotProduct += queryVector.get(i) * byteBuffer.getFloat();
        }
        return dotProduct;
    }

    @Override
    public double l1Norm(QueryVector queryVector) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(value.bytes, value.offset, value.length);

        double l1norm = 0;
        /*
        for (double queryValue : queryVector) {
            l1norm += Math.abs(queryValue - byteBuffer.getFloat());
        }
         */
        for (int i = 0; i < queryVector.size(); i++) {
            l1norm += Math.abs(queryVector.get(i) - byteBuffer.getFloat());
        }
        return l1norm;
    }

    @Override
    public double l2Norm(QueryVector queryVector) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(value.bytes, value.offset, value.length);
        double l2norm = 0;
        for (double queryValue : queryVector) {
            double diff = queryValue - byteBuffer.getFloat();
            l2norm += diff * diff;
        }
        /*
        for (int i = 0; i < queryVector.size(); i++) {
            double diff = queryVector.get(i) - byteBuffer.getFloat();
            l2norm += diff * diff;
        }*/
        return Math.sqrt(l2norm);
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public int getDims() {
        return vector.length;
    }

    @Override
    public PrimitiveIterator.OfDouble iterator() {
        return new PrimitiveIterator.OfDouble() {
            int index = 0;
            final ByteBuffer byteBuffer = ByteBuffer.wrap(value.bytes, value.offset, value.length);

            @Override
            public double nextDouble() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                index++;
                return byteBuffer.getFloat();
            }

            @Override
            public boolean hasNext() {
                return index < vector.length;
            }
        };
    }
}
