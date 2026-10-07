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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
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
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.vectors.BFloat16;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readSimilarityFunction;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readVectorEncoding;
import static org.elasticsearch.index.codec.vectors.VectorScoringUtils.scoreAndCollectAll;

public final class ES93BFloat16FlatVectorsReader extends FlatVectorsReader {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ES93BFloat16FlatVectorsReader.class);

    private final IntObjectHashMap<FieldEntry> fields;
    private final FlatVectorsScorer vectorScorer;
    private final IndexInput vectorData;
    private final FieldInfos fieldInfos;
    private final IOContext dataContext;
    private final Directory directory;
    private final String vectorDataFN;
    // the search reader, which holds the mapping its merge instances share
    private final ES93BFloat16FlatVectorsReader original;
    // As for stored fields, getMergeInstance may be called outside merges, so several merge instances may be held at once:
    // the merge state is guarded by the original's lock.
    // On the original: the merge mapping and how many merge instances hold it.
    private IndexInput mergeVectorData;
    private int mergeInstances;
    // On a merge instance: whether it released the mapping.
    private boolean finished;

    public ES93BFloat16FlatVectorsReader(SegmentReadState state, FlatVectorsScorer scorer) throws IOException {
        this.fields = new IntObjectHashMap<>();
        int versionMeta = readMetadata(state);
        this.fieldInfos = state.fieldInfos;
        this.vectorScorer = scorer;
        this.directory = state.directory;
        this.vectorDataFN = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            ES93BFloat16FlatVectorsFormat.VECTOR_DATA_EXTENSION
        );
        this.original = this;
        // how these are read is up to whoever wraps this format
        dataContext = state.context.union(FileTypeHint.DATA, FileDataHint.KNN_VECTORS);
        try {
            vectorData = openDataInput(
                state,
                versionMeta,
                ES93BFloat16FlatVectorsFormat.VECTOR_DATA_EXTENSION,
                ES93BFloat16FlatVectorsFormat.VECTOR_DATA_CODEC_NAME,
                dataContext
            );
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(this);
            throw t;
        }
    }

    /** Reads the same fields as {@code original}, through the mapping a merge opened for itself. */
    private ES93BFloat16FlatVectorsReader(ES93BFloat16FlatVectorsReader original, IndexInput vectorData) {
        this.fields = original.fields;
        this.vectorScorer = original.vectorScorer;
        this.vectorData = vectorData;
        this.fieldInfos = original.fieldInfos;
        this.dataContext = original.dataContext;
        this.directory = original.directory;
        this.vectorDataFN = original.vectorDataFN;
        this.original = original;
    }

    private int readMetadata(SegmentReadState state) throws IOException {
        String metaFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            ES93BFloat16FlatVectorsFormat.META_EXTENSION
        );
        int versionMeta = -1;
        try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName)) {
            Throwable priorE = null;
            try {
                versionMeta = CodecUtil.checkIndexHeader(
                    meta,
                    ES93BFloat16FlatVectorsFormat.META_CODEC_NAME,
                    ES93BFloat16FlatVectorsFormat.VERSION_START,
                    ES93BFloat16FlatVectorsFormat.VERSION_CURRENT,
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
        IndexInput in = state.directory.openInput(fileName, context);
        try {
            int versionVectorData = CodecUtil.checkIndexHeader(
                in,
                codecName,
                ES93BFloat16FlatVectorsFormat.VERSION_START,
                ES93BFloat16FlatVectorsFormat.VERSION_CURRENT,
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
            return in;
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(in);
            throw t;
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
        return ES93BFloat16FlatVectorsReader.SHALLOW_SIZE + fields.ramBytesUsed();
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
        final FieldEntry entry = getFieldEntryOrThrow(fieldInfo.name);
        return Map.of(ES93BFloat16FlatVectorsFormat.VECTOR_DATA_EXTENSION, entry.vectorDataLength());
    }

    @Override
    public void checkIntegrity() throws IOException {
        CodecUtil.checksumEntireFile(vectorData);
    }

    /**
     * Merges read the vectors sequentially and searches at random, and advice applies to a whole mapping, so merges share a
     * mapping of their own, each through a clone.
     */
    @Override
    public FlatVectorsReader getMergeInstance() throws IOException {
        if (mergeNeedsItsOwnMapping() == false) {
            return this;
        }
        IndexInput data = original.mergeVectorData();
        try {
            return new ES93BFloat16FlatVectorsReader(original, data.clone());
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(original.release());
            throw t;
        }
    }

    /** Only when searches read the file at random and this reader was not opened by a merge. */
    private boolean mergeNeedsItsOwnMapping() {
        return dataContext.context() != IOContext.Context.MERGE && dataContext.hints().contains(DataAccessHint.RANDOM);
    }

    /** The mapping merges read, opened by the first merge instance and released by {@link #finishMerge()}. */
    private synchronized IndexInput mergeVectorData() throws IOException {
        assert original == this;
        if (mergeVectorData == null) {
            try {
                mergeVectorData = directory.openInput(vectorDataFN, mergeContext());
            } catch (FileNotFoundException | NoSuchFileException e) {
                // an open reader outlives its files, so fall back to the mapping it already holds
                mergeVectorData = vectorData;
            }
        }
        mergeInstances++;
        return mergeVectorData;
    }

    /** The caller's context as a merge with sequential access. */
    private IOContext mergeContext() {
        IOContext.FileOpenHint[] hints = Stream.concat(
            dataContext.hints().stream().filter(hint -> hint instanceof DataAccessHint == false),
            Stream.of(DataAccessHint.SEQUENTIAL)
        ).toArray(IOContext.FileOpenHint[]::new);
        return IOContext.merge().withHints(hints);
    }

    @Override
    public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        scoreAndCollectAll(knnCollector, acceptDocs, getFloatVectorValues(field).scorer(target));
    }

    private FieldEntry getFieldEntryOrThrow(String field) {
        final FieldInfo info = fieldInfos.fieldInfo(field);
        final FieldEntry entry;
        if (info == null || (entry = fields.get(info.number)) == null) {
            throw new IllegalArgumentException("field=\"" + field + "\" not found");
        }
        return entry;
    }

    private FieldEntry getFieldEntry(String field, VectorEncoding expectedEncoding) {
        final FieldEntry fieldEntry = getFieldEntryOrThrow(field);
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
        return OffHeapBFloat16VectorValues.load(
            fieldEntry.similarityFunction,
            vectorScorer,
            fieldEntry.ordToDoc,
            fieldEntry.vectorEncoding,
            fieldEntry.dimension,
            fieldEntry.size,
            fieldEntry.vectorDataOffset,
            fieldEntry.vectorDataLength,
            vectorData
        );
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
        throw new IllegalStateException(field + " only supports float vectors");
    }

    @Override
    public FlatVectorsScorer getFlatVectorScorer(String field) throws IOException {
        return vectorScorer;
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
        final FieldEntry fieldEntry = getFieldEntry(field, VectorEncoding.FLOAT32);
        return vectorScorer.getRandomVectorScorer(
            fieldEntry.similarityFunction,
            OffHeapBFloat16VectorValues.load(
                fieldEntry.similarityFunction,
                vectorScorer,
                fieldEntry.ordToDoc,
                fieldEntry.vectorEncoding,
                fieldEntry.dimension,
                fieldEntry.size,
                fieldEntry.vectorDataOffset,
                fieldEntry.vectorDataLength,
                vectorData
            ),
            target
        );
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
        throw new UnsupportedOperationException(field + " only supports float vectors");
    }

    /** Releases this merge instance's hold on the merge mapping, once; a no-op on the search reader. */
    @Override
    public void finishMerge() throws IOException {
        if (original != this) {
            IOUtils.close(original.releaseMergeVectorData(this));
        }
    }

    /** Releases the hold of {@code mergeInstance}, once; returns the mapping to close, if any. */
    private synchronized IndexInput releaseMergeVectorData(ES93BFloat16FlatVectorsReader mergeInstance) {
        assert original == this && mergeInstance.original == this;
        if (mergeInstance.finished) {
            return null;
        }
        mergeInstance.finished = true;
        return release();
    }

    /** Releases one hold; once none is left, returns the mapping to close outside the lock. */
    private synchronized IndexInput release() {
        assert original == this && mergeInstances > 0;
        if (--mergeInstances > 0) {
            return null;
        }
        IndexInput toClose = mergeVectorData == vectorData ? null : mergeVectorData;
        mergeVectorData = null;
        return toClose;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(vectorData, takeMergeVectorData());
    }

    /** Takes the merge mapping, so a merge finishing later does not close it again. */
    private synchronized IndexInput takeMergeVectorData() {
        if (original != this || mergeVectorData == vectorData) {
            return null;
        }
        IndexInput toClose = mergeVectorData;
        mergeVectorData = null;
        return toClose;
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
            if (vectorEncoding == VectorEncoding.BYTE) {
                throw new IllegalStateException(
                    "Incorrect vector encoding for field=\"" + info.name + "\"; " + vectorEncoding + " not supported"
                );
            }

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

            int byteSize = BFloat16.BYTES;
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
