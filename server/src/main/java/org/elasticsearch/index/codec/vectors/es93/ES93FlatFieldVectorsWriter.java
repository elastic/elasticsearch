/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.simdvec.OffHeapVectorInput;
import org.elasticsearch.simdvec.OffHeapVectorStore;

import java.io.Closeable;
import java.io.IOException;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;

/**
 * {@link FlatFieldVectorsWriter} implementations that accumulate vectors in an off-heap paged native
 * store ({@link OffHeapVectorStore}).
 *
 * <p>The writer overrides {@link #asKnnVectorValues} to return directly a {@code KnnVectorValues} instance
 * which implements {@link HasIndexSlice}, returning a {@link OffHeapVectorInput} wrapping the native
 * {@link OffHeapVectorStore} when {@link HasIndexSlice#getSlice()} is called.
 *
 * <p>{@link #getVectors} returns a view that materialises a fresh heap array on every access, so each
 * traversal of it copies the whole field. It is therefore only suitable for formats that traverse it once
 * at write-out, and never for one that mutates the arrays it gets back (mutations are lost).
 *
 * <p>Use the static factory {@link #create(FieldInfo)} as an
 * {@code IOFunction<FieldInfo, FlatFieldVectorsWriter<?>>} passed to the 3-arg
 * {@code Lucene99FlatVectorsWriter} constructor.
 */
abstract sealed class ES93FlatFieldVectorsWriter<T> extends FlatFieldVectorsWriter<T> implements Closeable permits
    ES93FlatFieldVectorsWriter.Float, ES93FlatFieldVectorsWriter.Byte {

    /** Releases the off-heap store. */
    @Override
    public abstract void close() throws IOException;

    /**
     * Factory for use as {@code IOFunction<FieldInfo, FlatFieldVectorsWriter<?>>} in the 3-arg
     * {@code Lucene99FlatVectorsWriter} constructor. Dispatches on the field's vector encoding.
     */
    static ES93FlatFieldVectorsWriter<?> create(FieldInfo fieldInfo) {
        return switch (fieldInfo.getVectorEncoding()) {
            case FLOAT32 -> new Float(fieldInfo);
            case BYTE -> new Byte(fieldInfo);
        };
    }

    /**
     * Off-heap writer for FLOAT32 vectors.
     */
    static final class Float extends ES93FlatFieldVectorsWriter<float[]> {

        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Float.class);

        private final OffHeapVectorStore<float[]> store;
        private final DocsWithFieldSet docsWithField;
        private final OffHeapVectorInput input;
        private final String fieldName;
        private int lastDocID = -1;
        private boolean finished;

        Float(FieldInfo fieldInfo) {
            this.store = OffHeapVectorStore.forFloats(fieldInfo.getVectorDimension());
            this.fieldName = fieldInfo.name;
            this.docsWithField = new DocsWithFieldSet();
            this.input = store.asIndexInput();
        }

        @Override
        public void addValue(int docID, float[] vectorValue) throws IOException {
            if (finished) {
                throw new IllegalStateException("already finished, cannot add more values");
            }
            if (docID == lastDocID) {
                throw new IllegalArgumentException(
                    "VectorValuesField \"" + fieldName + "\" appears more than once in this document (only one value is allowed per field)"
                );
            }
            assert docID > lastDocID;
            docsWithField.add(docID);
            store.append(vectorValue);
            lastDocID = docID;
        }

        @Override
        public float[] copyValue(float[] value) {
            return Arrays.copyOf(value, value.length);
        }

        /**
         * Returns a lazy materializing view over the off-heap store.
         * Called once per vector by {@code Lucene99FlatVectorsWriter.writeField} during flush.
         */
        @Override
        public List<float[]> getVectors() {
            return new AbstractList<>() {
                @Override
                public float[] get(int index) {
                    return store.get(index);
                }

                @Override
                public int size() {
                    return store.size();
                }
            };
        }

        /**
         * Returns {@link HasIndexSlice} vector values backed by the off-heap store, so that
         * {@link ES93GenericFlatVectorScorer} routes HNSW graph-build scoring to the native supplier.
         */
        @Override
        public KnnVectorValues asKnnVectorValues(VectorEncoding encoding, int dim) {
            return new OffHeapFloatVectorValues(store, input);
        }

        @Override
        public DocsWithFieldSet getDocsWithFieldSet() {
            return docsWithField;
        }

        @Override
        public void finish() throws IOException {
            if (finished) {
                return;
            }
            finished = true;
        }

        @Override
        public void close() throws IOException {
            store.close();
        }

        @Override
        public boolean isFinished() {
            return finished;
        }

        @Override
        public long ramBytesUsed() {
            // Native (off-heap) bytes are still process memory; include them so that the
            // Lucene base-test ramBytesUsed contract is satisfied and callers get an
            // accurate picture of total memory consumed by this field writer.
            return SHALLOW_SIZE + docsWithField.ramBytesUsed() + store.nativeBytes();
        }
    }

    /**
     * Off-heap writer for BYTE vectors.
     */
    static final class Byte extends ES93FlatFieldVectorsWriter<byte[]> {

        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Byte.class);

        private final OffHeapVectorStore<byte[]> store;
        private final DocsWithFieldSet docsWithField;
        private final OffHeapVectorInput input;
        private final String fieldName;
        private int lastDocID = -1;
        private boolean finished;

        Byte(FieldInfo fieldInfo) {
            this.store = OffHeapVectorStore.forBytes(fieldInfo.getVectorDimension());
            this.fieldName = fieldInfo.name;
            this.docsWithField = new DocsWithFieldSet();
            this.input = store.asIndexInput();
        }

        @Override
        public void addValue(int docID, byte[] vectorValue) throws IOException {
            if (finished) {
                throw new IllegalStateException("already finished, cannot add more values");
            }
            if (docID == lastDocID) {
                throw new IllegalArgumentException(
                    "VectorValuesField \"" + fieldName + "\" appears more than once in this document (only one value is allowed per field)"
                );
            }
            assert docID > lastDocID;
            docsWithField.add(docID);
            store.append(vectorValue);
            lastDocID = docID;
        }

        @Override
        public byte[] copyValue(byte[] value) {
            return Arrays.copyOf(value, value.length);
        }

        @Override
        public List<byte[]> getVectors() {
            return new AbstractList<>() {
                @Override
                public byte[] get(int index) {
                    return store.get(index);
                }

                @Override
                public int size() {
                    return store.size();
                }
            };
        }

        @Override
        public KnnVectorValues asKnnVectorValues(VectorEncoding encoding, int dim) {
            return new OffHeapByteVectorValues(store, input);
        }

        @Override
        public DocsWithFieldSet getDocsWithFieldSet() {
            return docsWithField;
        }

        @Override
        public void finish() throws IOException {
            if (finished) {
                return;
            }
            finished = true;
        }

        @Override
        public void close() throws IOException {
            store.close();
        }

        @Override
        public boolean isFinished() {
            return finished;
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + docsWithField.ramBytesUsed() + store.nativeBytes();
        }
    }

    private static final class OffHeapFloatVectorValues extends FloatVectorValues implements HasIndexSlice {

        private final OffHeapVectorStore<float[]> store;
        private final OffHeapVectorInput input;

        OffHeapFloatVectorValues(OffHeapVectorStore<float[]> store, OffHeapVectorInput input) {
            this.store = store;
            this.input = input;
        }

        @Override
        public IndexInput getSlice() {
            return input;
        }

        @Override
        public float[] vectorValue(int ord) throws IOException {
            return store.get(ord);
        }

        @Override
        public FloatVectorValues copy() throws IOException {
            return new OffHeapFloatVectorValues(store, input.clone());
        }

        @Override
        public int dimension() {
            return store.dimension();
        }

        @Override
        public int size() {
            return store.size();
        }
    }

    private static final class OffHeapByteVectorValues extends ByteVectorValues implements HasIndexSlice {

        private final OffHeapVectorStore<byte[]> store;
        private final OffHeapVectorInput input;

        OffHeapByteVectorValues(OffHeapVectorStore<byte[]> store, OffHeapVectorInput input) {
            this.store = store;
            this.input = input;
        }

        @Override
        public IndexInput getSlice() {
            return input;
        }

        @Override
        public byte[] vectorValue(int ord) throws IOException {
            return store.get(ord);
        }

        @Override
        public ByteVectorValues copy() throws IOException {
            return new OffHeapByteVectorValues(store, input.clone());
        }

        @Override
        public int dimension() {
            return store.dimension();
        }

        @Override
        public int size() {
            return store.size();
        }
    }
}
