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
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsWriter;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Wraps {@link Lucene99FlatVectorsWriter}, delegating merge/finish/close but
 * owning the indexing path (addField + flush) with ES-managed field writers.
 * <p>
 * VarHandles are used to access the delegate's private {@code meta} and
 * {@code vectorData} IndexOutputs so that our flush writes to the same files
 * that finish/close will finalise.
 */
class ES93FlatVectorsWriter extends FlatVectorsWriter {

    private static final long SHALLOW_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ES93FlatVectorsWriter.class);

    private static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

    private static final VarHandle META_HANDLE;
    private static final VarHandle VECTOR_DATA_HANDLE;

    static {
        try {
            var lookup = MethodHandles.privateLookupIn(Lucene99FlatVectorsWriter.class, MethodHandles.lookup());
            META_HANDLE = lookup.findVarHandle(Lucene99FlatVectorsWriter.class, "meta", IndexOutput.class);
            VECTOR_DATA_HANDLE = lookup.findVarHandle(Lucene99FlatVectorsWriter.class, "vectorData", IndexOutput.class);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Failed to access Lucene99FlatVectorsWriter fields", e);
        }
    }

    private final Lucene99FlatVectorsWriter delegate;
    private final List<ES93FlatFieldVectorsWriter<?>> fields = new ArrayList<>();

    ES93FlatVectorsWriter(SegmentWriteState state, FlatVectorsScorer scorer) throws IOException {
        super(scorer);
        this.delegate = new Lucene99FlatVectorsWriter(state, scorer);
    }

    private IndexOutput meta() {
        return (IndexOutput) META_HANDLE.get(delegate);
    }

    private IndexOutput vectorData() {
        return (IndexOutput) VECTOR_DATA_HANDLE.get(delegate);
    }

    @Override
    public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
        var fieldWriter = ES93FlatFieldVectorsWriter.create(fieldInfo);
        fields.add(fieldWriter);
        return fieldWriter;
    }

    @Override
    public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
        IndexOutput meta = meta();
        IndexOutput vectorData = vectorData();
        for (ES93FlatFieldVectorsWriter<?> field : fields) {
            if (sortMap == null) {
                writeField(field, maxDoc, meta, vectorData);
            } else {
                writeSortingField(field, maxDoc, sortMap, meta, vectorData);
            }
            field.finish();
        }
    }

    private static void writeField(ES93FlatFieldVectorsWriter<?> fieldData, int maxDoc, IndexOutput meta, IndexOutput vectorData)
        throws IOException {
        long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
        switch (fieldData.fieldInfo.getVectorEncoding()) {
            case BYTE -> writeByteVectors(fieldData, vectorData);
            case FLOAT32 -> writeFloat32Vectors(fieldData, vectorData);
        }
        long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
        writeMeta(fieldData.fieldInfo, maxDoc, vectorDataOffset, vectorDataLength, fieldData.docsWithField, meta, vectorData);
    }

    private static void writeFloat32Vectors(ES93FlatFieldVectorsWriter<?> fieldData, IndexOutput vectorData) throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(fieldData.dim * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (Object v : fieldData.vectors) {
            buffer.asFloatBuffer().put((float[]) v);
            vectorData.writeBytes(buffer.array(), buffer.array().length);
        }
    }

    private static void writeByteVectors(ES93FlatFieldVectorsWriter<?> fieldData, IndexOutput vectorData) throws IOException {
        for (Object v : fieldData.vectors) {
            byte[] vector = (byte[]) v;
            vectorData.writeBytes(vector, vector.length);
        }
    }

    private static void writeSortingField(
        ES93FlatFieldVectorsWriter<?> fieldData,
        int maxDoc,
        Sorter.DocMap sortMap,
        IndexOutput meta,
        IndexOutput vectorData
    ) throws IOException {
        final int[] ordMap = new int[fieldData.docsWithField.cardinality()];
        DocsWithFieldSet newDocsWithField = new DocsWithFieldSet();
        mapOldOrdToNewOrd(fieldData.docsWithField, sortMap, null, ordMap, newDocsWithField);

        long vectorDataOffset = switch (fieldData.fieldInfo.getVectorEncoding()) {
            case BYTE -> writeSortedByteVectors(fieldData, ordMap, vectorData);
            case FLOAT32 -> writeSortedFloat32Vectors(fieldData, ordMap, vectorData);
        };
        long vectorDataLength = vectorData.getFilePointer() - vectorDataOffset;
        writeMeta(fieldData.fieldInfo, maxDoc, vectorDataOffset, vectorDataLength, newDocsWithField, meta, vectorData);
    }

    private static long writeSortedFloat32Vectors(ES93FlatFieldVectorsWriter<?> fieldData, int[] ordMap, IndexOutput vectorData)
        throws IOException {
        long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
        final ByteBuffer buffer = ByteBuffer.allocate(fieldData.dim * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int ordinal : ordMap) {
            float[] vector = (float[]) fieldData.vectors.get(ordinal);
            buffer.asFloatBuffer().put(vector);
            vectorData.writeBytes(buffer.array(), buffer.array().length);
        }
        return vectorDataOffset;
    }

    private static long writeSortedByteVectors(ES93FlatFieldVectorsWriter<?> fieldData, int[] ordMap, IndexOutput vectorData)
        throws IOException {
        long vectorDataOffset = vectorData.alignFilePointer(Float.BYTES);
        for (int ordinal : ordMap) {
            byte[] vector = (byte[]) fieldData.vectors.get(ordinal);
            vectorData.writeBytes(vector, vector.length);
        }
        return vectorDataOffset;
    }

    private static void writeMeta(
        FieldInfo field,
        int maxDoc,
        long vectorDataOffset,
        long vectorDataLength,
        DocsWithFieldSet docsWithField,
        IndexOutput meta,
        IndexOutput vectorData
    ) throws IOException {
        meta.writeInt(field.number);
        meta.writeInt(field.getVectorEncoding().ordinal());
        meta.writeInt(field.getVectorSimilarityFunction().ordinal());
        meta.writeVLong(vectorDataOffset);
        meta.writeVLong(vectorDataLength);
        meta.writeVInt(field.getVectorDimension());

        int count = docsWithField.cardinality();
        meta.writeInt(count);
        OrdToDocDISIReaderConfiguration.writeStoredMeta(DIRECT_MONOTONIC_BLOCK_SHIFT, meta, vectorData, count, maxDoc, docsWithField);
    }

    @Override
    public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        delegate.mergeOneField(fieldInfo, mergeState);
    }

    @Override
    public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        return delegate.mergeOneFieldToIndex(fieldInfo, mergeState);
    }

    @Override
    public void finish() throws IOException {
        delegate.finish();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public long ramBytesUsed() {
        long total = SHALLOW_RAM_BYTES_USED;
        for (ES93FlatFieldVectorsWriter<?> field : fields) {
            total += field.ramBytesUsed();
        }
        return total;
    }
}
