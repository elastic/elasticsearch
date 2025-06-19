/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
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
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.OrdinalTranslatedKnnCollector;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS;

/**
 * Reader for GPU-accelerated vectors. This reader is used to read the GPU vectors from the index.
 */
public class GPUVectorsReader extends KnnVectorsReader {

    private final IndexInput gpuIdx;
    private final SegmentReadState state;
    private final FieldInfos fieldInfos;
    protected final IntObjectHashMap<FieldEntry> fields;
    private final FlatVectorsReader rawVectorsReader;

    @SuppressWarnings("this-escape")
    public GPUVectorsReader(SegmentReadState state, FlatVectorsReader rawVectorsReader) throws IOException {
        this.state = state;
        this.fieldInfos = state.fieldInfos;
        this.rawVectorsReader = rawVectorsReader;
        this.fields = new IntObjectHashMap<>();
        String meta = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, GPUVectorsFormat.GPU_META_EXTENSION);

        int versionMeta = -1;
        boolean success = false;
        try (ChecksumIndexInput gpuMeta = state.directory.openChecksumInput(meta)) {
            Throwable priorE = null;
            try {
                versionMeta = CodecUtil.checkIndexHeader(
                    gpuMeta,
                    GPUVectorsFormat.NAME,
                    GPUVectorsFormat.VERSION_START,
                    GPUVectorsFormat.VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                readFields(gpuMeta);
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(gpuMeta, priorE);
            }
            gpuIdx = openDataInput(state, versionMeta, GPUVectorsFormat.GPU_IDX_EXTENSION, GPUVectorsFormat.NAME, state.context);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    private static IndexInput openDataInput(
        SegmentReadState state,
        int versionMeta,
        String fileExtension,
        String codecName,
        IOContext context
    ) throws IOException {
        final String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, fileExtension);
        final IndexInput in = state.directory.openInput(fileName, context);
        boolean success = false;
        try {
            final int versionVectorData = CodecUtil.checkIndexHeader(
                in,
                codecName,
                GPUVectorsFormat.VERSION_START,
                GPUVectorsFormat.VERSION_CURRENT,
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

    private void readFields(ChecksumIndexInput meta) throws IOException {
        for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
            final FieldInfo info = fieldInfos.fieldInfo(fieldNumber);
            if (info == null) {
                throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
            }
            fields.put(info.number, readField(meta, info));
        }
    }

    private FieldEntry readField(IndexInput input, FieldInfo info) throws IOException {
        final VectorEncoding vectorEncoding = readVectorEncoding(input);
        final VectorSimilarityFunction similarityFunction = readSimilarityFunction(input);
        final long dataOffset = input.readLong();
        final long dataLength = input.readLong();

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
        return new FieldEntry(similarityFunction, vectorEncoding, dataOffset, dataLength);
    }

    private static VectorSimilarityFunction readSimilarityFunction(DataInput input) throws IOException {
        final int i = input.readInt();
        if (i < 0 || i >= SIMILARITY_FUNCTIONS.size()) {
            throw new IllegalArgumentException("invalid distance function: " + i);
        }
        return SIMILARITY_FUNCTIONS.get(i);
    }

    private static VectorEncoding readVectorEncoding(DataInput input) throws IOException {
        final int encodingId = input.readInt();
        if (encodingId < 0 || encodingId >= VectorEncoding.values().length) {
            throw new CorruptIndexException("Invalid vector encoding id: " + encodingId, input);
        }
        return VectorEncoding.values()[encodingId];
    }

    @Override
    public final void checkIntegrity() throws IOException {
        rawVectorsReader.checkIntegrity();
        CodecUtil.checksumEntireFile(gpuIdx);
    }

    @Override
    public final FloatVectorValues getFloatVectorValues(String field) throws IOException {
        return rawVectorsReader.getFloatVectorValues(field);
    }

    @Override
    public final ByteVectorValues getByteVectorValues(String field) throws IOException {
        return rawVectorsReader.getByteVectorValues(field);
    }

    @Override
    public final void search(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
        // TODO: Implement GPU-accelerated search
        collectAllMatchingDocs(knnCollector, acceptDocs, rawVectorsReader.getRandomVectorScorer(field, target));
    }

    private void collectAllMatchingDocs(KnnCollector knnCollector, Bits acceptDocs, RandomVectorScorer scorer) throws IOException {
        OrdinalTranslatedKnnCollector collector = new OrdinalTranslatedKnnCollector(knnCollector, scorer::ordToDoc);
        Bits acceptedOrds = scorer.getAcceptOrds(acceptDocs);
        for (int i = 0; i < scorer.maxOrd(); i++) {
            if (acceptedOrds == null || acceptedOrds.get(i)) {
                collector.collect(i, scorer.score(i));
                collector.incVisitedCount(1);
            }
        }
        assert collector.earlyTerminated() == false;
    }

    @Override
    public final void search(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
        collectAllMatchingDocs(knnCollector, acceptDocs, rawVectorsReader.getRandomVectorScorer(field, target));
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(rawVectorsReader, gpuIdx);
    }

    protected record FieldEntry(
        VectorSimilarityFunction similarityFunction,
        VectorEncoding vectorEncoding,
        long dataOffset,
        long dataLength
    ) {
        IndexInput dataSlice(IndexInput dataFile) throws IOException {
            return dataFile.slice("gpu-data", dataOffset, dataLength);
        }
    }
}
