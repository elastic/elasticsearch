/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;

import java.io.IOException;
import java.io.UncheckedIOException;

public class OffHeapFloatVectorValues extends FloatVectorValues {

    private final IndexInput vectors;
    private final float[] vector;
    private final int numVectors;
    private final long vectorLength;
    private final RandomAccessInput randomDocs;

    public OffHeapFloatVectorValues(IndexInput vectors, int numVectors, int dimensions, IndexInput randomDocs) throws IOException {
        this.vectors = vectors;
        this.vector = new float[dimensions];
        this.numVectors = numVectors;
        this.vectorLength = (long) dimensions * Float.BYTES;
        this.randomDocs = randomDocs != null ? randomDocs.randomAccessSlice(0, randomDocs.length()) : null;
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
        vectors.seek(ord * vectorLength);
        vectors.readFloats(vector, 0, vector.length);
        return vector;
    }

    public void writeVector(int ord, IndexOutput output, IndexOutput docOutput) throws IOException {
        vectors.seek(ord * vectorLength);
        output.copyBytes(vectors, vectorLength);
        docOutput.writeInt(ord);
    }

    @Override
    public FloatVectorValues copy() {
        return this;
    }

    @Override
    public int dimension() {
        return vector.length;
    }

    @Override
    public int size() {
        return numVectors;
    }

    @Override
    public int ordToDoc(int ord) {
        if (randomDocs == null) {
            return ord;
        }
        try {
            return randomDocs.readInt((long) ord * Integer.BYTES);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
