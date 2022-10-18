/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.NeighborQueue;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Reads vectors from the index segments along with index data structures supporting KNN search.
 *
 * NOTE: this class was temporarily copied from Lucene to fix a bug in Lucene94HnswVectorsReader.
 * Its only modification is to use a long instead of integer in the {@code validateFieldEntry} method.
 */
@SuppressForbidden(reason = "class is copied from Lucene")
public final class XLucene94HnswVectorsReader extends KnnVectorsReader {

    private final FieldInfos fieldInfos;
    private final Map<String, FieldEntry> fields = new HashMap<>();
    private final IndexInput vectorData;
    private final IndexInput vectorIndex;

    XLucene94HnswVectorsReader(SegmentReadState state) throws IOException {
        this.fieldInfos = state.fieldInfos;
        int versionMeta = readMetadata(state);
        boolean success = false;
        try {
            vectorData = openDataInput(
                state,
                versionMeta,
                XLucene94HnswVectorsFormat.VECTOR_DATA_EXTENSION,
                XLucene94HnswVectorsFormat.VECTOR_DATA_CODEC_NAME
            );
            vectorIndex = openDataInput(
                state,
                versionMeta,
                XLucene94HnswVectorsFormat.VECTOR_INDEX_EXTENSION,
                XLucene94HnswVectorsFormat.VECTOR_INDEX_CODEC_NAME
            );
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    private int readMetadata(SegmentReadState state) throws IOException {
        String metaFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            XLucene94HnswVectorsFormat.META_EXTENSION
        );
        int versionMeta = -1;
        try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName, state.context)) {
            Throwable priorE = null;
            try {
                versionMeta = CodecUtil.checkIndexHeader(
                    meta,
                    XLucene94HnswVectorsFormat.META_CODEC_NAME,
                    XLucene94HnswVectorsFormat.VERSION_START,
                    XLucene94HnswVectorsFormat.VERSION_CURRENT,
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

    private static IndexInput openDataInput(SegmentReadState state, int versionMeta, String fileExtension, String codecName)
        throws IOException {
        String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, fileExtension);
        IndexInput in = state.directory.openInput(fileName, state.context);
        boolean success = false;
        try {
            int versionVectorData = CodecUtil.checkIndexHeader(
                in,
                codecName,
                XLucene94HnswVectorsFormat.VERSION_START,
                XLucene94HnswVectorsFormat.VERSION_CURRENT,
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

        int byteSize;
        switch (info.getVectorEncoding()) {
            case BYTE:
                byteSize = Byte.BYTES;
                break;
            default:
            case FLOAT32:
                byteSize = Float.BYTES;
                break;
        }
        long numBytes = (long) fieldEntry.size * dimension * byteSize;
        if (numBytes != fieldEntry.vectorDataLength) {
            throw new IllegalStateException(
                "Vector data length "
                    + fieldEntry.vectorDataLength
                    + " not matching size="
                    + fieldEntry.size
                    + " * dim="
                    + dimension
                    + " * byteSize="
                    + byteSize
                    + " = "
                    + numBytes
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
        long totalBytes = RamUsageEstimator.shallowSizeOfInstance(XLucene94HnswVectorsFormat.class);
        totalBytes += RamUsageEstimator.sizeOfMap(fields, RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class));
        return totalBytes;
    }

    @Override
    public void checkIntegrity() throws IOException {
        CodecUtil.checksumEntireFile(vectorData);
        CodecUtil.checksumEntireFile(vectorIndex);
    }

    @Override
    public VectorValues getVectorValues(String field) throws IOException {
        FieldEntry fieldEntry = fields.get(field);
        VectorValues values = XOffHeapVectorValues.load(fieldEntry, vectorData);
        if (fieldEntry.vectorEncoding == VectorEncoding.BYTE) {
            return new XExpandingVectorValues(values);
        } else {
            return values;
        }
    }

    @Override
    public TopDocs search(String field, float[] target, int k, Bits acceptDocs, int visitedLimit) throws IOException {
        FieldEntry fieldEntry = fields.get(field);

        if (fieldEntry.size() == 0) {
            return new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
        }

        // bound k by total number of vectors to prevent oversizing data structures
        k = Math.min(k, fieldEntry.size());
        XOffHeapVectorValues vectorValues = XOffHeapVectorValues.load(fieldEntry, vectorData);

        NeighborQueue results = HnswGraphSearcher.search(
            target,
            k,
            vectorValues,
            fieldEntry.vectorEncoding,
            fieldEntry.similarityFunction,
            getGraph(fieldEntry),
            vectorValues.getAcceptOrds(acceptDocs),
            visitedLimit
        );

        int i = 0;
        ScoreDoc[] scoreDocs = new ScoreDoc[Math.min(results.size(), k)];
        while (results.size() > 0) {
            int node = results.topNode();
            float score = results.topScore();
            results.pop();
            scoreDocs[scoreDocs.length - ++i] = new ScoreDoc(vectorValues.ordToDoc(node), score);
        }

        TotalHits.Relation relation = results.incomplete() ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO : TotalHits.Relation.EQUAL_TO;
        return new TopDocs(new TotalHits(results.visitedCount(), relation), scoreDocs);
    }

    /** Get knn graph values; used for testing */
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
        IndexInput bytesSlice = vectorIndex.slice("graph-data", entry.vectorIndexOffset, entry.vectorIndexLength);
        return new OffHeapHnswGraph(entry, bytesSlice);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(vectorData, vectorIndex);
    }

    static class FieldEntry {

        final VectorSimilarityFunction similarityFunction;
        final VectorEncoding vectorEncoding;
        final long vectorDataOffset;
        final long vectorDataLength;
        final long vectorIndexOffset;
        final long vectorIndexLength;
        final int M;
        final int numLevels;
        final int dimension;
        final int size;
        final int[][] nodesByLevel;
        // for each level the start offsets in vectorIndex file from where to read neighbours
        final long[] graphOffsetsByLevel;

        // the following four variables used to read docIds encoded by IndexDISI
        // special values of docsWithFieldOffset are -1 and -2
        // -1 : dense
        // -2 : empty
        // other: sparse
        final long docsWithFieldOffset;
        final long docsWithFieldLength;
        final short jumpTableEntryCount;
        final byte denseRankPower;

        // the following four variables used to read ordToDoc encoded by DirectMonotonicWriter
        // note that only spare case needs to store ordToDoc
        final long addressesOffset;
        final int blockShift;
        final DirectMonotonicReader.Meta meta;
        final long addressesLength;

        FieldEntry(IndexInput input, VectorEncoding vectorEncoding, VectorSimilarityFunction similarityFunction) throws IOException {
            this.similarityFunction = similarityFunction;
            this.vectorEncoding = vectorEncoding;
            vectorDataOffset = input.readVLong();
            vectorDataLength = input.readVLong();
            vectorIndexOffset = input.readVLong();
            vectorIndexLength = input.readVLong();
            dimension = input.readInt();
            size = input.readInt();

            docsWithFieldOffset = input.readLong();
            docsWithFieldLength = input.readLong();
            jumpTableEntryCount = input.readShort();
            denseRankPower = input.readByte();

            // dense or empty
            if (docsWithFieldOffset == -1 || docsWithFieldOffset == -2) {
                addressesOffset = 0;
                blockShift = 0;
                meta = null;
                addressesLength = 0;
            } else {
                // sparse
                addressesOffset = input.readLong();
                blockShift = input.readVInt();
                meta = DirectMonotonicReader.loadMeta(input, size, blockShift);
                addressesLength = input.readLong();
            }

            // read nodes by level
            M = input.readInt();
            numLevels = input.readInt();
            nodesByLevel = new int[numLevels][];
            for (int level = 0; level < numLevels; level++) {
                int numNodesOnLevel = input.readInt();
                if (level == 0) {
                    // we don't store nodes for level 0th, as this level contains all nodes
                    assert numNodesOnLevel == size;
                    nodesByLevel[0] = null;
                } else {
                    nodesByLevel[level] = new int[numNodesOnLevel];
                    for (int i = 0; i < numNodesOnLevel; i++) {
                        nodesByLevel[level][i] = input.readInt();
                    }
                }
            }

            // calculate for each level the start offsets in vectorIndex file from where to read
            // neighbours
            graphOffsetsByLevel = new long[numLevels];
            for (int level = 0; level < numLevels; level++) {
                if (level == 0) {
                    graphOffsetsByLevel[level] = 0;
                } else if (level == 1) {
                    int numNodesOnLevel0 = size;
                    graphOffsetsByLevel[level] = (1 + (M * 2)) * Integer.BYTES * numNodesOnLevel0;
                } else {
                    int numNodesOnPrevLevel = nodesByLevel[level - 1].length;
                    graphOffsetsByLevel[level] = graphOffsetsByLevel[level - 1] + (1 + M) * Integer.BYTES * numNodesOnPrevLevel;
                }
            }
        }

        int size() {
            return size;
        }
    }

    /** Read the nearest-neighbors graph from the index input */
    private static final class OffHeapHnswGraph extends HnswGraph {

        final IndexInput dataIn;
        final int[][] nodesByLevel;
        final long[] graphOffsetsByLevel;
        final int numLevels;
        final int entryNode;
        final int size;
        final long bytesForConns;
        final long bytesForConns0;

        int arcCount;
        int arcUpTo;
        int arc;

        OffHeapHnswGraph(FieldEntry entry, IndexInput dataIn) {
            this.dataIn = dataIn;
            this.nodesByLevel = entry.nodesByLevel;
            this.numLevels = entry.numLevels;
            this.entryNode = numLevels > 1 ? nodesByLevel[numLevels - 1][0] : 0;
            this.size = entry.size();
            this.graphOffsetsByLevel = entry.graphOffsetsByLevel;
            this.bytesForConns = ((long) entry.M + 1) * Integer.BYTES;
            this.bytesForConns0 = ((long) (entry.M * 2) + 1) * Integer.BYTES;
        }

        @Override
        public void seek(int level, int targetOrd) throws IOException {
            int targetIndex = level == 0 ? targetOrd : Arrays.binarySearch(nodesByLevel[level], 0, nodesByLevel[level].length, targetOrd);
            assert targetIndex >= 0;
            long graphDataOffset = graphOffsetsByLevel[level] + targetIndex * (level == 0 ? bytesForConns0 : bytesForConns);
            // unsafe; no bounds checking
            dataIn.seek(graphDataOffset);
            arcCount = dataIn.readInt();
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
            ++arcUpTo;
            arc = dataIn.readInt();
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
                return new NodesIterator(size());
            } else {
                return new NodesIterator(nodesByLevel[level], nodesByLevel[level].length);
            }
        }
    }
}
