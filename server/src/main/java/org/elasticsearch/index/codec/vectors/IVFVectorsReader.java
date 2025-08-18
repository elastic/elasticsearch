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
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.search.vectors.IVFKnnSearchStrategy;

import java.io.IOException;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS;
import static org.elasticsearch.index.codec.vectors.IVFVectorsFormat.DYNAMIC_VISIT_RATIO;

/**
 * Reader for IVF vectors. This reader is used to read the IVF vectors from the index.
 */
public abstract class IVFVectorsReader extends KnnVectorsReader {

    private final IndexInput ivfCentroids, ivfClusters;
    private final SegmentReadState state;
    private final FieldInfos fieldInfos;
    protected final IntObjectHashMap<FieldEntry> fields;
    private final FlatVectorsReader rawVectorsReader;

    @SuppressWarnings("this-escape")
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

    abstract CentroidIterator getCentroidIterator(FieldInfo fieldInfo, int numCentroids, IndexInput centroids, float[] target)
        throws IOException;

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
        final int numCentroids = input.readInt();
        final long centroidOffset = input.readLong();
        final long centroidLength = input.readLong();
        final float[] globalCentroid = new float[info.getVectorDimension()];
        long postingListOffset = -1;
        long postingListLength = -1;
        float globalCentroidDp = 0;
        if (centroidLength > 0) {
            postingListOffset = input.readLong();
            postingListLength = input.readLong();
            input.readFloats(globalCentroid, 0, globalCentroid.length);
            globalCentroidDp = Float.intBitsToFloat(input.readInt());
        }
        return new FieldEntry(
            similarityFunction,
            vectorEncoding,
            numCentroids,
            centroidOffset,
            centroidLength,
            postingListOffset,
            postingListLength,
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
        float visitRatio = DYNAMIC_VISIT_RATIO;
        // Search strategy may be null if this is being called from checkIndex (e.g. from a test)
        if (knnCollector.getSearchStrategy() instanceof IVFKnnSearchStrategy ivfSearchStrategy) {
            visitRatio = ivfSearchStrategy.getVisitRatio();
        }

        FieldEntry entry = fields.get(fieldInfo.number);
        if (visitRatio == DYNAMIC_VISIT_RATIO) {
            // empirically based, and a good dynamic to get decent recall while scaling a la "efSearch"
            // scaling by the number of vectors vs. the nearest neighbors requested
            // not perfect, but a comparative heuristic.
            // TODO: we might want to consider the density of the centroids as experiments shows that for fewer vectors per centroid,
            // the least vectors we need to score to get a good recall.
            float estimated = Math.round(Math.log10(numVectors) * Math.log10(numVectors) * (knnCollector.k()));
            // clip so we visit at least one vector
            visitRatio = estimated / numVectors;
        }
        // we account for soar vectors here. We can potentially visit a vector twice so we multiply by 2 here.
        long maxVectorVisited = (long) (2.0 * visitRatio * numVectors);
        CentroidIterator centroidIterator = getCentroidIterator(fieldInfo, entry.numCentroids, entry.centroidSlice(ivfCentroids), target);
        PostingVisitor scorer = getPostingVisitor(fieldInfo, entry.postingListSlice(ivfClusters), target, acceptDocs);

        long expectedDocs = 0;
        long actualDocs = 0;
        // initially we visit only the "centroids to search"
        // Note, numCollected is doing the bare minimum here.
        // TODO do we need to handle nested doc counts similarly to how we handle
        // filtering? E.g. keep exploring until we hit an expected number of parent documents vs. child vectors?
        while (centroidIterator.hasNext()
            && (maxVectorVisited > actualDocs || knnCollector.minCompetitiveSimilarity() == Float.NEGATIVE_INFINITY)) {
            // todo do we actually need to know the score???
            long offset = centroidIterator.nextPostingListOffset();
            // todo do we need direct access to the raw centroid???, this is used for quantizing, maybe hydrating and quantizing
            // is enough?
            expectedDocs += scorer.resetPostingsScorer(offset);
            actualDocs += scorer.visit(knnCollector);
        }
        if (acceptDocs != null) {
            float unfilteredRatioVisited = (float) expectedDocs / numVectors;
            int filteredVectors = (int) Math.ceil(numVectors * percentFiltered);
            float expectedScored = Math.min(2 * filteredVectors * unfilteredRatioVisited, expectedDocs / 2f);
            while (centroidIterator.hasNext() && (actualDocs < expectedScored || actualDocs < knnCollector.k())) {
                long offset = centroidIterator.nextPostingListOffset();
                scorer.resetPostingsScorer(offset);
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

    @Override
    public void close() throws IOException {
        IOUtils.close(rawVectorsReader, ivfCentroids, ivfClusters);
    }

    protected record FieldEntry(
        VectorSimilarityFunction similarityFunction,
        VectorEncoding vectorEncoding,
        int numCentroids,
        long centroidOffset,
        long centroidLength,
        long postingListOffset,
        long postingListLength,
        float[] globalCentroid,
        float globalCentroidDp
    ) {
        IndexInput centroidSlice(IndexInput centroidFile) throws IOException {
            return centroidFile.slice("centroids", centroidOffset, centroidLength);
        }

        IndexInput postingListSlice(IndexInput postingListFile) throws IOException {
            return postingListFile.slice("postingLists", postingListOffset, postingListLength);
        }
    }

    abstract PostingVisitor getPostingVisitor(FieldInfo fieldInfo, IndexInput postingsLists, float[] target, Bits needsScoring)
        throws IOException;

    interface CentroidIterator {
        boolean hasNext();

        long nextPostingListOffset() throws IOException;
    }

    interface PostingVisitor {
        // TODO maybe we can not specifically pass the centroid...

        /** returns the number of documents in the posting list */
        int resetPostingsScorer(long offset) throws IOException;

        /** returns the number of scored documents */
        int visit(KnnCollector collector) throws IOException;
    }
}
