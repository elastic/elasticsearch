/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors.codec;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FlatVectorsReader;
import org.apache.lucene.codecs.HnswGraphProvider;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.ScalarQuantizer;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.OrdinalTranslatedKnnCollector;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.packed.DirectMonotonicReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Reads vectors from the index segments along with index data structures supporting KNN search.
 */
@SuppressForbidden(reason = "copy from Lucene")
public final class ESLucene99HnswVectorsReader extends KnnVectorsReader implements ESQuantizedVectorsReader, HnswGraphProvider {

    static final String META_EXTENSION = "vem";
    static final String VECTOR_INDEX_EXTENSION = "vex";
    static final String META_CODEC_NAME = "Lucene99HnswVectorsFormatMeta";
    static final String VECTOR_INDEX_CODEC_NAME = "Lucene99HnswVectorsFormatIndex";

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ESLucene99HnswVectorsReader.class);

    private final FieldInfos fieldInfos;
    private final Map<String, FieldEntry> fields = new HashMap<>();
    private final IndexInput vectorIndex;
    private final FlatVectorsReader flatVectorsReader;

    ESLucene99HnswVectorsReader(SegmentReadState state, FlatVectorsReader flatVectorsReader) throws IOException {
        this.flatVectorsReader = flatVectorsReader;
        boolean success = false;
        this.fieldInfos = state.fieldInfos;
        String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, META_EXTENSION);
        int versionMeta = -1;
        try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName, state.context)) {
            Throwable priorE = null;
            try {
                versionMeta = CodecUtil.checkIndexHeader(
                    meta,
                    META_CODEC_NAME,
                    Lucene99HnswVectorsFormat.VERSION_START,
                    Lucene99HnswVectorsFormat.VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                readFields(meta, state.fieldInfos);
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(meta, priorE);
            }
            vectorIndex = openDataInput(state, versionMeta, VECTOR_INDEX_EXTENSION, VECTOR_INDEX_CODEC_NAME);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    private static IndexInput openDataInput(SegmentReadState state, int versionMeta, String fileExtension, String codecName)
        throws IOException {
        String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, fileExtension);
        IndexInput in = state.directory.openInput(fileName, state.context);
        boolean success = false;
        try {
            int versionVectorData = CodecUtil.checkIndexHeader(
                in,
                codecName,
                Lucene99HnswVectorsFormat.VERSION_START,
                Lucene99HnswVectorsFormat.VERSION_CURRENT,
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
            FieldEntry fieldEntry = readField(meta);
            validateFieldEntry(info, fieldEntry);
            fields.put(info.name, fieldEntry);
        }
    }

    private void validateFieldEntry(FieldInfo info, FieldEntry fieldEntry) {
        int dimension = info.getVectorDimension();
        if (dimension != fieldEntry.dimension) {
            throw new IllegalStateException(
                "Inconsistent vector dimension for field=\"" + info.name + "\"; " + dimension + " != " + fieldEntry.dimension
            );
        }
    }

    private VectorSimilarityFunction readSimilarityFunction(DataInput input) throws IOException {
        int similarityFunctionId = input.readInt();
        if (similarityFunctionId < 0 || similarityFunctionId >= VectorSimilarityFunction.values().length) {
            throw new CorruptIndexException("Invalid similarity function id: " + similarityFunctionId, input);
        }
        return VectorSimilarityFunction.values()[similarityFunctionId];
    }

    private VectorEncoding readVectorEncoding(DataInput input) throws IOException {
        int encodingId = input.readInt();
        if (encodingId < 0 || encodingId >= VectorEncoding.values().length) {
            throw new CorruptIndexException("Invalid vector encoding id: " + encodingId, input);
        }
        return VectorEncoding.values()[encodingId];
    }

    private FieldEntry readField(IndexInput input) throws IOException {
        VectorEncoding vectorEncoding = readVectorEncoding(input);
        VectorSimilarityFunction similarityFunction = readSimilarityFunction(input);
        return new FieldEntry(input, vectorEncoding, similarityFunction);
    }

    @Override
    public long ramBytesUsed() {
        return ESLucene99HnswVectorsReader.SHALLOW_SIZE + RamUsageEstimator.sizeOfMap(
            fields,
            RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class)
        ) + flatVectorsReader.ramBytesUsed();
    }

    @Override
    public void checkIntegrity() throws IOException {
        flatVectorsReader.checkIntegrity();
        CodecUtil.checksumEntireFile(vectorIndex);
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        return flatVectorsReader.getFloatVectorValues(field);
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
        return flatVectorsReader.getByteVectorValues(field);
    }

    @Override
    public void search(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
        FieldEntry fieldEntry = fields.get(field);

        if (fieldEntry.size() == 0 || knnCollector.k() == 0 || fieldEntry.vectorEncoding != VectorEncoding.FLOAT32) {
            return;
        }
        final RandomVectorScorer scorer = flatVectorsReader.getRandomVectorScorer(field, target);
        final KnnCollector collector = new OrdinalTranslatedKnnCollector(knnCollector, scorer::ordToDoc);
        final Bits acceptedOrds = scorer.getAcceptOrds(acceptDocs);
        if (knnCollector.k() < scorer.maxOrd()) {
            HnswGraphSearcher.search(scorer, collector, getGraph(fieldEntry), acceptedOrds);
        } else {
            // if k is larger than the number of vectors, we can just iterate over all vectors
            // and collect them
            for (int i = 0; i < scorer.maxOrd(); i++) {
                if (acceptedOrds == null || acceptedOrds.get(i)) {
                    knnCollector.incVisitedCount(1);
                    knnCollector.collect(scorer.ordToDoc(i), scorer.score(i));
                }
            }
        }
    }

    @Override
    public void search(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
        FieldEntry fieldEntry = fields.get(field);

        if (fieldEntry.size() == 0 || knnCollector.k() == 0 || fieldEntry.vectorEncoding != VectorEncoding.BYTE) {
            return;
        }
        final RandomVectorScorer scorer = flatVectorsReader.getRandomVectorScorer(field, target);
        final KnnCollector collector = new OrdinalTranslatedKnnCollector(knnCollector, scorer::ordToDoc);
        final Bits acceptedOrds = scorer.getAcceptOrds(acceptDocs);
        if (knnCollector.k() < scorer.maxOrd()) {
            HnswGraphSearcher.search(scorer, collector, getGraph(fieldEntry), acceptedOrds);
        } else {
            // if k is larger than the number of vectors, we can just iterate over all vectors
            // and collect them
            for (int i = 0; i < scorer.maxOrd(); i++) {
                if (acceptedOrds == null || acceptedOrds.get(i)) {
                    knnCollector.incVisitedCount(1);
                    knnCollector.collect(scorer.ordToDoc(i), scorer.score(i));
                }
            }
        }
    }

    @Override
    public HnswGraph getGraph(String field) throws IOException {
        FieldInfo info = fieldInfos.fieldInfo(field);
        if (info == null) {
            throw new IllegalArgumentException("No such field '" + field + "'");
        }
        FieldEntry entry = fields.get(field);
        if (entry != null && entry.vectorIndexLength > 0) {
            return getGraph(entry);
        } else {
            return HnswGraph.EMPTY;
        }
    }

    private HnswGraph getGraph(FieldEntry entry) throws IOException {
        return new OffHeapHnswGraph(entry, vectorIndex);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(flatVectorsReader, vectorIndex);
    }

    @Override
    public ESQuantizedByteVectorValues getQuantizedVectorValues(String field) throws IOException {
        if (flatVectorsReader instanceof ESQuantizedVectorsReader) {
            return ((ESQuantizedVectorsReader) flatVectorsReader).getQuantizedVectorValues(field);
        }
        return null;
    }

    @Override
    public ScalarQuantizer getQuantizationState(String field) {
        if (flatVectorsReader instanceof ESQuantizedVectorsReader) {
            return ((ESQuantizedVectorsReader) flatVectorsReader).getQuantizationState(field);
        }
        return null;
    }

    static class FieldEntry implements Accountable {
        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class);
        final VectorSimilarityFunction similarityFunction;
        final VectorEncoding vectorEncoding;
        final long vectorIndexOffset;
        final long vectorIndexLength;
        final int M;
        final int numLevels;
        final int dimension;
        final int size;
        final int[][] nodesByLevel;
        // for each level the start offsets in vectorIndex file from where to read neighbours
        final DirectMonotonicReader.Meta offsetsMeta;
        final long offsetsOffset;
        final int offsetsBlockShift;
        final long offsetsLength;

        FieldEntry(IndexInput input, VectorEncoding vectorEncoding, VectorSimilarityFunction similarityFunction) throws IOException {
            this.similarityFunction = similarityFunction;
            this.vectorEncoding = vectorEncoding;
            vectorIndexOffset = input.readVLong();
            vectorIndexLength = input.readVLong();
            dimension = input.readVInt();
            size = input.readInt();
            // read nodes by level
            M = input.readVInt();
            numLevels = input.readVInt();
            nodesByLevel = new int[numLevels][];
            long numberOfOffsets = 0;
            for (int level = 0; level < numLevels; level++) {
                if (level > 0) {
                    int numNodesOnLevel = input.readVInt();
                    numberOfOffsets += numNodesOnLevel;
                    nodesByLevel[level] = new int[numNodesOnLevel];
                    nodesByLevel[level][0] = input.readVInt();
                    for (int i = 1; i < numNodesOnLevel; i++) {
                        nodesByLevel[level][i] = nodesByLevel[level][i - 1] + input.readVInt();
                    }
                } else {
                    numberOfOffsets += size;
                }
            }
            if (numberOfOffsets > 0) {
                offsetsOffset = input.readLong();
                offsetsBlockShift = input.readVInt();
                offsetsMeta = DirectMonotonicReader.loadMeta(input, numberOfOffsets, offsetsBlockShift);
                offsetsLength = input.readLong();
            } else {
                offsetsOffset = 0;
                offsetsBlockShift = 0;
                offsetsMeta = null;
                offsetsLength = 0;
            }
        }

        int size() {
            return size;
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + Arrays.stream(nodesByLevel).mapToLong(nodes -> RamUsageEstimator.sizeOf(nodes)).sum() + RamUsageEstimator
                .sizeOf(offsetsMeta);
        }
    }

    /** Read the nearest-neighbors graph from the index input */
    private static final class OffHeapHnswGraph extends HnswGraph {

        final IndexInput dataIn;
        final int[][] nodesByLevel;
        final int numLevels;
        final int entryNode;
        final int size;
        int arcCount;
        int arcUpTo;
        int arc;
        private final DirectMonotonicReader graphLevelNodeOffsets;
        private final long[] graphLevelNodeIndexOffsets;
        // Allocated to be M*2 to track the current neighbors being explored
        private final int[] currentNeighborsBuffer;

        OffHeapHnswGraph(FieldEntry entry, IndexInput vectorIndex) throws IOException {
            this.dataIn = vectorIndex.slice("graph-data", entry.vectorIndexOffset, entry.vectorIndexLength);
            this.nodesByLevel = entry.nodesByLevel;
            this.numLevels = entry.numLevels;
            this.entryNode = numLevels > 1 ? nodesByLevel[numLevels - 1][0] : 0;
            this.size = entry.size();
            final RandomAccessInput addressesData = vectorIndex.randomAccessSlice(entry.offsetsOffset, entry.offsetsLength);
            this.graphLevelNodeOffsets = DirectMonotonicReader.getInstance(entry.offsetsMeta, addressesData);
            this.currentNeighborsBuffer = new int[entry.M * 2];
            graphLevelNodeIndexOffsets = new long[numLevels];
            graphLevelNodeIndexOffsets[0] = 0;
            for (int i = 1; i < numLevels; i++) {
                // nodesByLevel is `null` for the zeroth level as we know its size
                int nodeCount = nodesByLevel[i - 1] == null ? size : nodesByLevel[i - 1].length;
                graphLevelNodeIndexOffsets[i] = graphLevelNodeIndexOffsets[i - 1] + nodeCount;
            }
        }

        @Override
        public void seek(int level, int targetOrd) throws IOException {
            int targetIndex = level == 0 ? targetOrd : Arrays.binarySearch(nodesByLevel[level], 0, nodesByLevel[level].length, targetOrd);
            assert targetIndex >= 0;
            // unsafe; no bounds checking
            dataIn.seek(graphLevelNodeOffsets.get(targetIndex + graphLevelNodeIndexOffsets[level]));
            arcCount = dataIn.readVInt();
            if (arcCount > 0) {
                currentNeighborsBuffer[0] = dataIn.readVInt();
                for (int i = 1; i < arcCount; i++) {
                    currentNeighborsBuffer[i] = currentNeighborsBuffer[i - 1] + dataIn.readVInt();
                }
            }
            arc = -1;
            arcUpTo = 0;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public int nextNeighbor() throws IOException {
            if (arcUpTo >= arcCount) {
                return NO_MORE_DOCS;
            }
            arc = currentNeighborsBuffer[arcUpTo];
            ++arcUpTo;
            return arc;
        }

        @Override
        public int numLevels() throws IOException {
            return numLevels;
        }

        @Override
        public int entryNode() throws IOException {
            return entryNode;
        }

        @Override
        public NodesIterator getNodesOnLevel(int level) {
            if (level == 0) {
                return new ArrayNodesIterator(size());
            } else {
                return new ArrayNodesIterator(nodesByLevel[level], nodesByLevel[level].length);
            }
        }
    }
}
