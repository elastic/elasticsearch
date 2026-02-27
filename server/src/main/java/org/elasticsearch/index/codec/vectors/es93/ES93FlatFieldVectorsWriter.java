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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Per-field vector writer that manages its own in-memory vector storage.
 * This mirrors Lucene's {@code Lucene99FlatVectorsWriter.FieldWriter} but is
 * owned by Elasticsearch, enabling future replacement of the storage backend
 * (e.g. with a contiguous MemorySegment for bulk native operations).
 */
abstract class ES93FlatFieldVectorsWriter<T> extends FlatFieldVectorsWriter<T> {

    private static final long SHALLOW_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ES93FlatFieldVectorsWriter.class);

    final FieldInfo fieldInfo;
    final int dim;
    final DocsWithFieldSet docsWithField;
    final List<T> vectors;
    private boolean finished;
    private int lastDocID = -1;

    static ES93FlatFieldVectorsWriter<?> create(FieldInfo fieldInfo) {
        return switch (fieldInfo.getVectorEncoding()) {
            case BYTE -> new ES93FlatFieldVectorsWriter<byte[]>(fieldInfo) {
                @Override
                public byte[] copyValue(byte[] value) {
                    return ArrayUtil.copyOfSubArray(value, 0, dim);
                }
            };
            case FLOAT32 -> new ES93FlatFieldVectorsWriter<float[]>(fieldInfo) {
                @Override
                public float[] copyValue(float[] value) {
                    return ArrayUtil.copyOfSubArray(value, 0, dim);
                }
            };
        };
    }

    ES93FlatFieldVectorsWriter(FieldInfo fieldInfo) {
        super();
        this.fieldInfo = fieldInfo;
        this.dim = fieldInfo.getVectorDimension();
        this.docsWithField = new DocsWithFieldSet();
        this.vectors = new ArrayList<>();
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
        T copy = copyValue(vectorValue);
        docsWithField.add(docID);
        vectors.add(copy);
        lastDocID = docID;
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_RAM_BYTES_USED;
        if (vectors.isEmpty()) return size;
        return size + docsWithField.ramBytesUsed() + (long) vectors.size() * (RamUsageEstimator.NUM_BYTES_OBJECT_REF
            + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER) + (long) vectors.size() * fieldInfo.getVectorDimension() * fieldInfo
                .getVectorEncoding().byteSize;
    }

    @Override
    public List<T> getVectors() {
        return vectors;
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
}
