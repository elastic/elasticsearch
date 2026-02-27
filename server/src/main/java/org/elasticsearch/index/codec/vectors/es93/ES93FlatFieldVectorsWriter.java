/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */

package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.simdvec.OffHeapByteVectorStore;
import org.elasticsearch.simdvec.OffHeapFloatVectorStore;
import org.elasticsearch.simdvec.WrappedNativeByteVectors;
import org.elasticsearch.simdvec.WrappedNativeFloatVectors;

import java.io.IOException;
import java.util.List;

/**
 * Per-field vector writer that stores vectors off-heap via
 * {@link OffHeapFloatVectorStore} / {@link OffHeapByteVectorStore}.
 * This keeps vector data out of the Java heap, reducing GC pressure
 * during indexing.
 */
abstract class ES93FlatFieldVectorsWriter<T> extends FlatFieldVectorsWriter<T> {

    private static final long SHALLOW_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ES93FlatFieldVectorsWriter.class);

    final FieldInfo fieldInfo;
    final int dim;
    final DocsWithFieldSet docsWithField;
    private boolean finished;
    private int lastDocID = -1;

    static ES93FlatFieldVectorsWriter<?> create(FieldInfo fieldInfo) {
        return switch (fieldInfo.getVectorEncoding()) {
            case BYTE -> new ByteWriter(fieldInfo);
            case FLOAT32 -> new FloatWriter(fieldInfo);
        };
    }

    ES93FlatFieldVectorsWriter(FieldInfo fieldInfo) {
        super();
        this.fieldInfo = fieldInfo;
        this.dim = fieldInfo.getVectorDimension();
        this.docsWithField = new DocsWithFieldSet();
    }

    protected abstract void storeVector(T vectorValue);

    protected abstract T getStoredVector(int i);

    protected abstract int storedVectorCount();

    protected abstract void closeStore();

    void normalizeByMagnitudes(float[] magnitudes, int offset, int length) {
        throw new UnsupportedOperationException("only supported for float vector writers");
    }

    @Override
    public void addValue(int docID, T vectorValue) throws IOException {
        if (finished) {
            throw new IllegalStateException("already finished, cannot add more values");
        }
        if (docID == lastDocID) {
            throw new IllegalArgumentException(
                "VectorValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)"
            );
        }
        assert docID > lastDocID;
        storeVector(vectorValue);
        docsWithField.add(docID);
        lastDocID = docID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<T> getVectors() {
        return (List<T>) switch (fieldInfo.getVectorEncoding()) {
            case BYTE -> {
                var store = ((ByteWriter) this).store;
                yield new WrappedNativeByteVectors(store);
            }
            case FLOAT32 -> {
                var store = ((FloatWriter) this).store;
                yield new WrappedNativeFloatVectors(store);
            }
        };
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
        this.finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_RAM_BYTES_USED;
        int count = storedVectorCount();
        if (count == 0) return size;
        return size + docsWithField.ramBytesUsed() + (long) count * RamUsageEstimator.NUM_BYTES_OBJECT_REF + (long) count * dim * fieldInfo
            .getVectorEncoding().byteSize;
    }

    static final class FloatWriter extends ES93FlatFieldVectorsWriter<float[]> {
        final OffHeapFloatVectorStore store;

        FloatWriter(FieldInfo fieldInfo) {
            super(fieldInfo);
            this.store = new OffHeapFloatVectorStore(fieldInfo.getVectorDimension());
        }

        @Override
        protected void storeVector(float[] v) {
            store.addVector(v);
        }

        @Override
        protected float[] getStoredVector(int i) {
            return store.getVector(i);
        }

        @Override
        protected int storedVectorCount() {
            return store.size();
        }

        @Override
        protected void closeStore() {
            store.close();
        }

        @Override
        void normalizeByMagnitudes(float[] magnitudes, int offset, int length) {
            store.normalizeByMagnitudes(magnitudes, offset, length);
        }

        @Override
        public float[] copyValue(float[] vectorValue) {
            throw new UnsupportedOperationException();
        }
    }

    static final class ByteWriter extends ES93FlatFieldVectorsWriter<byte[]> {
        final OffHeapByteVectorStore store;

        ByteWriter(FieldInfo fieldInfo) {
            super(fieldInfo);
            this.store = new OffHeapByteVectorStore(fieldInfo.getVectorDimension());
        }

        @Override
        protected void storeVector(byte[] v) {
            store.addVector(v);
        }

        @Override
        protected byte[] getStoredVector(int i) {
            return store.getVector(i);
        }

        @Override
        protected int storedVectorCount() {
            return store.size();
        }

        @Override
        protected void closeStore() {
            store.close();
        }

        @Override
        public byte[] copyValue(byte[] vectorValue) {
            throw new UnsupportedOperationException();
        }
    }

}
