/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

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
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.hnsw.NeighborQueue;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.search.vectors.IVFKnnSearchStrategy;

import java.io.IOException;
import java.util.Map;
import java.util.function.IntPredicate;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS;
import static org.elasticsearch.index.codec.vectors.IVFVectorsFormat.DYNAMIC_NPROBE;

/**
 * Reader for IVF vectors. This reader is used to read the IVF vectors from the index.
 */
public abstract class IVFVectorsReader extends KnnVectorsReader {

    private final IndexInput ivfCentroids, ivfClusters;
    private final SegmentReadState state;
    private final FieldInfos fieldInfos;
    protected final IntObjectHashMap<FieldEntry> fields;
    private final FlatVectorsReader rawVectorsReader;

    protected IVFVectorsReader(SegmentReadState state, FlatVectorsReader rawVectorsReader) throws IOException {
        this.state = state;
        this.fieldInfos = state.fieldInfos;
        this.rawVectorsReader = rawVectorsReader;
        this.fields = new IntObjectHashMap<>();
        String meta = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, IVFVectorsFormat.IVF_META_EXTENSION);

        int versionMeta = -1;
        boolean success = false;
        try (ChecksumIndexInput ivfMeta = state.directory.openChecksumInput(meta)) {
            Throwable priorE = null;
            try {
                versionMeta = CodecUtil.checkIndexHeader(
                    ivfMeta,
                    IVFVectorsFormat.NAME,
                    IVFVectorsFormat.VERSION_START,
                    IVFVectorsFormat.VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                readFields(ivfMeta);
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(ivfMeta, priorE);
            }
            ivfCentroids = openDataInput(state, versionMeta, IVFVectorsFormat.CENTROID_EXTENSION, IVFVectorsFormat.NAME, state.context);
            ivfClusters = openDataInput(state, versionMeta, IVFVectorsFormat.CLUSTER_EXTENSION, IVFVectorsFormat.NAME, state.context);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    abstract CentroidQueryScorer getCentroidScorer(
        FieldInfo fieldInfo,
        int numCentroids,
        IndexInput centroids,
        float[] target,
        IndexInput clusters
    ) throws IOException;

    protected abstract FloatVectorValues getCentroids(IndexInput indexInput, int numCentroids, FieldInfo info) throws IOException;

    public FloatVectorValues getCentroids(FieldInfo fieldInfo) throws IOException {
        FieldEntry entry = fields.get(fieldInfo.number);
        if (entry == null) {
            return null;
        }
        return getCentroids(entry.centroidSlice(ivfCentroids), entry.postingListOffsets.length, fieldInfo);
    }

    int centroidSize(String fieldName, int centroidOrdinal) throws IOException {
        FieldInfo fieldInfo = state.fieldInfos.fieldInfo(fieldName);
        FieldEntry entry = fields.get(fieldInfo.number);
        ivfClusters.seek(entry.postingListOffsets[centroidOrdinal]);
        return ivfClusters.readVInt();
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
                IVFVectorsFormat.VERSION_START,
                IVFVectorsFormat.VERSION_CURRENT,
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
        final long centroidOffset = input.readLong();
        final long centroidLength = input.readLong();
        final int numPostingLists = input.readVInt();
        final long[] postingListOffsets = new long[numPostingLists];
        for (int i = 0; i < numPostingLists; i++) {
            postingListOffsets[i] = input.readLong();
        }
        final float[] globalCentroid = new float[info.getVectorDimension()];
        float globalCentroidDp = 0;
        if (numPostingLists > 0) {
            input.readFloats(globalCentroid, 0, globalCentroid.length);
            globalCentroidDp = Float.intBitsToFloat(input.readInt());
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
        return new FieldEntry(
            similarityFunction,
            vectorEncoding,
            centroidOffset,
            centroidLength,
            postingListOffsets,
            globalCentroid,
            globalCentroidDp
        );
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
        CodecUtil.checksumEntireFile(ivfCentroids);
        CodecUtil.checksumEntireFile(ivfClusters);
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
        final FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32) == false) {
            rawVectorsReader.search(field, target, knnCollector, acceptDocs);
            return;
        }
        if (fieldInfo.getVectorDimension() != target.length) {
            throw new IllegalArgumentException(
                "vector query dimension: " + target.length + " differs from field dimension: " + fieldInfo.getVectorDimension()
            );
        }
        float percentFiltered = 1f;
        if (acceptDocs instanceof BitSet bitSet) {
            percentFiltered = Math.max(0f, Math.min(1f, (float) bitSet.approximateCardinality() / bitSet.length()));
        }
        int numVectors = rawVectorsReader.getFloatVectorValues(field).size();
        BitSet visitedDocs = new FixedBitSet(state.segmentInfo.maxDoc() + 1);
        IntPredicate needsScoring = docId -> {
            if (acceptDocs != null && acceptDocs.get(docId) == false) {
                return false;
            }
            return visitedDocs.getAndSet(docId) == false;
        };
        int nProbe = DYNAMIC_NPROBE;
        if (knnCollector.getSearchStrategy() instanceof IVFKnnSearchStrategy ivfSearchStrategy) {
            nProbe = ivfSearchStrategy.getNProbe();
        }

        FieldEntry entry = fields.get(fieldInfo.number);
        CentroidQueryScorer centroidQueryScorer = getCentroidScorer(
            fieldInfo,
            entry.postingListOffsets.length,
            entry.centroidSlice(ivfCentroids),
            target,
            ivfClusters
        );
        if (nProbe == DYNAMIC_NPROBE) {
            // empirically based, and a good dynamic to get decent recall while scaling a la "efSearch"
            // scaling by the number of centroids vs. the nearest neighbors requested
            // not perfect, but a comparative heuristic.
            // we might want to utilize the total vector count as well, but this is a good start
            nProbe = (int) Math.round(Math.log10(centroidQueryScorer.size()) * Math.sqrt(knnCollector.k()));
            // clip to be between 1 and the number of centroids
            nProbe = Math.max(Math.min(nProbe, centroidQueryScorer.size()), 1);
        }
        final NeighborQueue centroidQueue = scorePostingLists(fieldInfo, knnCollector, centroidQueryScorer, nProbe);
        PostingVisitor scorer = getPostingVisitor(fieldInfo, ivfClusters, target, needsScoring);
        int centroidsVisited = 0;
        long expectedDocs = 0;
        long actualDocs = 0;
        // initially we visit only the "centroids to search"
        while (centroidQueue.size() > 0 && centroidsVisited < nProbe && actualDocs < knnCollector.k()) {
            ++centroidsVisited;
            // todo do we actually need to know the score???
            int centroidOrdinal = centroidQueue.pop();
            // todo do we need direct access to the raw centroid???, this is used for quantizing, maybe hydrating and quantizing
            // is enough?
            expectedDocs += scorer.resetPostingsScorer(centroidOrdinal, centroidQueryScorer.centroid(centroidOrdinal));
            actualDocs += scorer.visit(knnCollector);
        }
        if (acceptDocs != null) {
            float unfilteredRatioVisited = (float) expectedDocs / numVectors;
            int filteredVectors = (int) Math.ceil(numVectors * percentFiltered);
            float expectedScored = Math.min(2 * filteredVectors * unfilteredRatioVisited, expectedDocs / 2f);
            while (centroidQueue.size() > 0 && (actualDocs < expectedScored || actualDocs < knnCollector.k())) {
                int centroidOrdinal = centroidQueue.pop();
                scorer.resetPostingsScorer(centroidOrdinal, centroidQueryScorer.centroid(centroidOrdinal));
                actualDocs += scorer.visit(knnCollector);
            }
        }
    }

    @Override
    public final void search(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
        final FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
        final ByteVectorValues values = rawVectorsReader.getByteVectorValues(field);
        for (int i = 0; i < values.size(); i++) {
            final float score = fieldInfo.getVectorSimilarityFunction().compare(target, values.vectorValue(i));
            knnCollector.collect(values.ordToDoc(i), score);
            if (knnCollector.earlyTerminated()) {
                return;
            }
        }
    }

    abstract NeighborQueue scorePostingLists(
        FieldInfo fieldInfo,
        KnnCollector knnCollector,
        CentroidQueryScorer centroidQueryScorer,
        int nProbe
    ) throws IOException;

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
        var raw = rawVectorsReader.getOffHeapByteSize(fieldInfo);
        FieldEntry fe = fields.get(fieldInfo.number);
        if (fe == null) {
            assert fieldInfo.getVectorEncoding() == VectorEncoding.BYTE;
            return raw;
        }
        return raw;  // for now just return the size of raw

        // TODO: determine desired off off-heap requirements
        // var centroids = Map.of(EXTENSION, fe.xxxLength());
        // var clusters = Map.of(EXTENSION, fe.yyyLength());
        // return KnnVectorsReader.mergeOffHeapByteSizeMaps(raw, centroids, clusters);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(rawVectorsReader, ivfCentroids, ivfClusters);
    }

    protected record FieldEntry(
        VectorSimilarityFunction similarityFunction,
        VectorEncoding vectorEncoding,
        long centroidOffset,
        long centroidLength,
        long[] postingListOffsets,
        float[] globalCentroid,
        float globalCentroidDp
    ) {
        IndexInput centroidSlice(IndexInput centroidFile) throws IOException {
            return centroidFile.slice("centroids", centroidOffset, centroidLength);
        }
    }

    abstract PostingVisitor getPostingVisitor(FieldInfo fieldInfo, IndexInput postingsLists, float[] target, IntPredicate needsScoring)
        throws IOException;

    interface CentroidQueryScorer {
        int size();

        float[] centroid(int centroidOrdinal) throws IOException;

        float score(int centroidOrdinal) throws IOException;
    }

    interface PostingVisitor {
        // TODO maybe we can not specifically pass the centroid...

        /** returns the number of documents in the posting list */
        int resetPostingsScorer(int centroidOrdinal, float[] centroid) throws IOException;

        /** returns the number of scored documents */
        int visit(KnnCollector collector) throws IOException;
    }
}
