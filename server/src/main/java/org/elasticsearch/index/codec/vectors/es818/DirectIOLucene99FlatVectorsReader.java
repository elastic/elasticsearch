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
package org.elasticsearch.index.codec.vectors.es818;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene95.OffHeapByteVectorValues;
import org.apache.lucene.codecs.lucene95.OffHeapFloatVectorValues;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.elasticsearch.index.codec.vectors.reflect.OffHeapStats;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readSimilarityFunction;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readVectorEncoding;

/** Copied from Lucene99FlatVectorsReader in Lucene 10.2, then modified to support DirectIOIndexInputSupplier */
@SuppressForbidden(reason = "Copied from lucene")
public class DirectIOLucene99FlatVectorsReader extends FlatVectorsReader implements OffHeapStats {

    private static final boolean USE_DIRECT_IO = Boolean.parseBoolean(System.getProperty("vector.rescoring.directio", "true"));

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(DirectIOLucene99FlatVectorsReader.class);

    private final IntObjectHashMap<FieldEntry> fields = new IntObjectHashMap<>();
    private final IndexInput vectorData;
    private final FieldInfos fieldInfos;

    @SuppressWarnings("this-escape")
    public DirectIOLucene99FlatVectorsReader(SegmentReadState state, FlatVectorsScorer scorer) throws IOException {
        super(scorer);
        int versionMeta = readMetadata(state);
        this.fieldInfos = state.fieldInfos;
        boolean success = false;
        try {
            vectorData = openDataInput(
                state,
                versionMeta,
                DirectIOLucene99FlatVectorsFormat.VECTOR_DATA_EXTENSION,
                DirectIOLucene99FlatVectorsFormat.VECTOR_DATA_CODEC_NAME,
                // Flat formats are used to randomly access vectors from their node ID that is stored
                // in the HNSW graph.
                state.context.withReadAdvice(ReadAdvice.RANDOM)
            );
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    public static boolean shouldUseDirectIO(SegmentReadState state) {
        return USE_DIRECT_IO && FilterDirectory.unwrap(state.directory) instanceof DirectIOIndexInputSupplier;
    }

    private int readMetadata(SegmentReadState state) throws IOException {
        String metaFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            DirectIOLucene99FlatVectorsFormat.META_EXTENSION
        );
        int versionMeta = -1;
        try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName)) {
            Throwable priorE = null;
            try {
                versionMeta = CodecUtil.checkIndexHeader(
                    meta,
                    DirectIOLucene99FlatVectorsFormat.META_CODEC_NAME,
                    DirectIOLucene99FlatVectorsFormat.VERSION_START,
                    DirectIOLucene99FlatVectorsFormat.VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                readFields(meta, state.fieldInfos);
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(meta, priorE);
            }
        }
        return versionMeta;
    }

    private static IndexInput openDataInput(
        SegmentReadState state,
        int versionMeta,
        String fileExtension,
        String codecName,
        IOContext context
    ) throws IOException {
        String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, fileExtension);
        // use direct IO for accessing raw vector data for searches
        IndexInput in = USE_DIRECT_IO && FilterDirectory.unwrap(state.directory) instanceof DirectIOIndexInputSupplier did
            ? did.openInputDirect(fileName, context)
            : state.directory.openInput(fileName, context);
        boolean success = false;
        try {
            int versionVectorData = CodecUtil.checkIndexHeader(
                in,
                codecName,
                DirectIOLucene99FlatVectorsFormat.VERSION_START,
                DirectIOLucene99FlatVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            if (versionMeta != versionVectorData) {
                throw new CorruptIndexException(
                    "Format versions mismatch: meta=" + versionMeta + ", " + codecName + "=" + versionVectorData,
                    in
                );
            }
            CodecUtil.retrieveChecksum(in);
            success = true;
            return in;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(in);
            }
        }
    }

    private void readFields(ChecksumIndexInput meta, FieldInfos infos) throws IOException {
        for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
            FieldInfo info = infos.fieldInfo(fieldNumber);
            if (info == null) {
                throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
            }
            FieldEntry fieldEntry = FieldEntry.create(meta, info);
            fields.put(info.number, fieldEntry);
        }
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + fields.ramBytesUsed();
    }

    @Override
    public void checkIntegrity() throws IOException {
        CodecUtil.checksumEntireFile(vectorData);
    }

    @Override
    public FlatVectorsReader getMergeInstance() {
        try {
            // Update the read advice since vectors are guaranteed to be accessed sequentially for merge
            this.vectorData.updateReadAdvice(ReadAdvice.SEQUENTIAL);
            return this;
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    private FieldEntry getFieldEntry(String field, VectorEncoding expectedEncoding) {
        final FieldInfo info = fieldInfos.fieldInfo(field);
        final FieldEntry fieldEntry;
        if (info == null || (fieldEntry = fields.get(info.number)) == null) {
            throw new IllegalArgumentException("field=\"" + field + "\" not found");
        }
        if (fieldEntry.vectorEncoding != expectedEncoding) {
            throw new IllegalArgumentException(
                "field=\"" + field + "\" is encoded as: " + fieldEntry.vectorEncoding + " expected: " + expectedEncoding
            );
        }
        return fieldEntry;
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        final FieldEntry fieldEntry = getFieldEntry(field, VectorEncoding.FLOAT32);
        return OffHeapFloatVectorValues.load(
            fieldEntry.similarityFunction,
            vectorScorer,
            fieldEntry.ordToDoc,
            fieldEntry.vectorEncoding,
            fieldEntry.dimension,
            fieldEntry.vectorDataOffset,
            fieldEntry.vectorDataLength,
            vectorData
        );
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
        final FieldEntry fieldEntry = getFieldEntry(field, VectorEncoding.BYTE);
        return OffHeapByteVectorValues.load(
            fieldEntry.similarityFunction,
            vectorScorer,
            fieldEntry.ordToDoc,
            fieldEntry.vectorEncoding,
            fieldEntry.dimension,
            fieldEntry.vectorDataOffset,
            fieldEntry.vectorDataLength,
            vectorData
        );
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
        final FieldEntry fieldEntry = getFieldEntry(field, VectorEncoding.FLOAT32);
        return vectorScorer.getRandomVectorScorer(
            fieldEntry.similarityFunction,
            OffHeapFloatVectorValues.load(
                fieldEntry.similarityFunction,
                vectorScorer,
                fieldEntry.ordToDoc,
                fieldEntry.vectorEncoding,
                fieldEntry.dimension,
                fieldEntry.vectorDataOffset,
                fieldEntry.vectorDataLength,
                vectorData
            ),
            target
        );
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
        final FieldEntry fieldEntry = getFieldEntry(field, VectorEncoding.BYTE);
        return vectorScorer.getRandomVectorScorer(
            fieldEntry.similarityFunction,
            OffHeapByteVectorValues.load(
                fieldEntry.similarityFunction,
                vectorScorer,
                fieldEntry.ordToDoc,
                fieldEntry.vectorEncoding,
                fieldEntry.dimension,
                fieldEntry.vectorDataOffset,
                fieldEntry.vectorDataLength,
                vectorData
            ),
            target
        );
    }

    @Override
    public void finishMerge() throws IOException {
        // This makes sure that the access pattern hint is reverted back since HNSW implementation
        // needs it
        this.vectorData.updateReadAdvice(ReadAdvice.RANDOM);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(vectorData);
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
        return Map.of();  // no off-heap
    }

    private record FieldEntry(
        VectorSimilarityFunction similarityFunction,
        VectorEncoding vectorEncoding,
        long vectorDataOffset,
        long vectorDataLength,
        int dimension,
        int size,
        OrdToDocDISIReaderConfiguration ordToDoc,
        FieldInfo info
    ) {

        FieldEntry {
            if (similarityFunction != info.getVectorSimilarityFunction()) {
                throw new IllegalStateException(
                    "Inconsistent vector similarity function for field=\""
                        + info.name
                        + "\"; "
                        + similarityFunction
                        + " != "
                        + info.getVectorSimilarityFunction()
                );
            }
            int infoVectorDimension = info.getVectorDimension();
            if (infoVectorDimension != dimension) {
                throw new IllegalStateException(
                    "Inconsistent vector dimension for field=\"" + info.name + "\"; " + infoVectorDimension + " != " + dimension
                );
            }

            int byteSize = switch (info.getVectorEncoding()) {
                case BYTE -> Byte.BYTES;
                case FLOAT32 -> Float.BYTES;
            };
            long vectorBytes = Math.multiplyExact((long) infoVectorDimension, byteSize);
            long numBytes = Math.multiplyExact(vectorBytes, size);
            if (numBytes != vectorDataLength) {
                throw new IllegalStateException(
                    "Vector data length "
                        + vectorDataLength
                        + " not matching size="
                        + size
                        + " * dim="
                        + dimension
                        + " * byteSize="
                        + byteSize
                        + " = "
                        + numBytes
                );
            }
        }

        static FieldEntry create(IndexInput input, FieldInfo info) throws IOException {
            final VectorEncoding vectorEncoding = readVectorEncoding(input);
            final VectorSimilarityFunction similarityFunction = readSimilarityFunction(input);
            final var vectorDataOffset = input.readVLong();
            final var vectorDataLength = input.readVLong();
            final var dimension = input.readVInt();
            final var size = input.readInt();
            final var ordToDoc = OrdToDocDISIReaderConfiguration.fromStoredMeta(input, size);
            return new FieldEntry(similarityFunction, vectorEncoding, vectorDataOffset, vectorDataLength, dimension, size, ordToDoc, info);
        }
    }
}
