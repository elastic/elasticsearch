/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

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
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.vectors.GenericFlatVectorReaders;
import org.elasticsearch.search.vectors.ESAcceptDocs;
import org.elasticsearch.search.vectors.IVFKnnSearchStrategy;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS;

/**
 * Reader for IVF vectors. This reader is used to read the IVF vectors from the index.
 */
public abstract class IVFVectorsReader<E extends IVFVectorsReader.FieldEntry> extends KnnVectorsReader {

    // Two-Signal Model constants for dynamic visit ratio computation.
    // Computes a visit ratio from the num_candidates/k ratio and k magnitude.
    private static final double V_MIN = 0.003;
    private static final double V_MAX = 0.04;
    private static final double LOG1P_R_MAX = Math.log1p(10.0);
    private static final double LOG1P_K_MAX = Math.log1p(10_000.0);
    private static final double RATIO_WEIGHT = 0.85;
    private static final double K_WEIGHT = 0.15;

    // Segment-size cap constants.
    // Empirical power-law curve calibrated on GIST-1M, Wiki-Cohere-1M, and MSMarco-130M datasets.
    // Caps the visit ratio for large segments where fewer clusters need visiting to achieve the target recall.
    // Produces ~10% cap for small segments (100K), ~4.5% at 1M, and ~2-3% for large segments (5-10M).
    private static final double CAP_COEFFICIENT = 0.045;
    private static final int CAP_REF_SIZE = 1_000_000;
    private static final double CAP_EXPONENT = 0.35;
    static final float DEFAULT_TARGET_RECALL = 0.9f;

    protected final IndexInput ivfCentroids, ivfClusters;
    private final SegmentReadState state;
    private final FieldInfos fieldInfos;
    protected final IntObjectHashMap<E> fields;
    private final GenericFlatVectorReaders genericReaders;
    private final String centroidExtension;
    private final String clusterExtension;
    private final int versionDirectIo;
    private final float dynamicVisitRatio;
    protected int versionMeta = -1;

    @SuppressWarnings("this-escape")
    protected IVFVectorsReader(
        SegmentReadState state,
        GenericFlatVectorReaders.LoadFlatVectorsReader loadReader,
        String codecName,
        String centroidExtension,
        String clusterExtension,
        String metaExtension,
        int versionStart,
        int versionCurrent,
        int versionDirectIo,
        float dynamicVisitRatio
    ) throws IOException {
        this.state = state;
        this.fieldInfos = state.fieldInfos;
        this.fields = new IntObjectHashMap<>();
        this.genericReaders = new GenericFlatVectorReaders();
        this.centroidExtension = centroidExtension;
        this.clusterExtension = clusterExtension;
        this.versionDirectIo = versionDirectIo;
        this.dynamicVisitRatio = dynamicVisitRatio;
        String meta = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);

        int versionMeta = -1;
        try (ChecksumIndexInput ivfMeta = state.directory.openChecksumInput(meta)) {
            Throwable priorE = null;
            try {
                versionMeta = CodecUtil.checkIndexHeader(
                    ivfMeta,
                    codecName,
                    versionStart,
                    versionCurrent,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                this.versionMeta = versionMeta;
                readFields(ivfMeta, versionMeta, genericReaders, loadReader);
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(ivfMeta, priorE);
            }
            ivfCentroids = openDataInput(state, versionMeta, centroidExtension, codecName, versionStart, versionCurrent, state.context);
            ivfClusters = openDataInput(state, versionMeta, clusterExtension, codecName, versionStart, versionCurrent, state.context);
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(this);
            throw t;
        }
    }

    public abstract CentroidIterator getCentroidIterator(
        FieldInfo fieldInfo,
        int numCentroids,
        IndexInput centroids,
        float[] target,
        IndexInput postingListSlice,
        AcceptDocs acceptDocs,
        float approximateCost,
        FloatVectorValues values,
        float visitRatio
    ) throws IOException;

    protected static IndexInput openDataInput(
        SegmentReadState state,
        int versionMeta,
        String fileExtension,
        String codecName,
        int versionStart,
        int versionCurrent,
        IOContext context
    ) throws IOException {
        final String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, fileExtension);
        final IndexInput in = state.directory.openInput(fileName, context);
        try {
            final int versionVectorData = CodecUtil.checkIndexHeader(
                in,
                codecName,
                versionStart,
                versionCurrent,
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

    private void readFields(
        ChecksumIndexInput meta,
        int versionMeta,
        GenericFlatVectorReaders genericFields,
        GenericFlatVectorReaders.LoadFlatVectorsReader loadReader
    ) throws IOException {
        for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
            final FieldInfo info = fieldInfos.fieldInfo(fieldNumber);
            if (info == null) {
                throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
            }

            E fieldEntry = readField(meta, info, versionMeta);
            genericFields.loadField(fieldNumber, fieldEntry, loadReader);

            fields.put(info.number, fieldEntry);
        }
    }

    private E readField(IndexInput input, FieldInfo info, int versionMeta) throws IOException {
        final String rawVectorFormat = input.readString();
        final boolean useDirectIOReads = versionMeta >= versionDirectIo && input.readByte() == 1;
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
        return doReadField(
            input,
            rawVectorFormat,
            useDirectIOReads,
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

    protected abstract E doReadField(
        IndexInput input,
        String rawVectorFormat,
        boolean useDirectIOReads,
        VectorSimilarityFunction similarityFunction,
        VectorEncoding vectorEncoding,
        int numCentroids,
        long centroidOffset,
        long centroidLength,
        long postingListOffset,
        long postingListLength,
        float[] globalCentroid,
        float globalCentroidDp
    ) throws IOException;

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
        for (var reader : genericReaders.allReaders()) {
            reader.checkIntegrity();
        }
        CodecUtil.checksumEntireFile(ivfCentroids);
        CodecUtil.checksumEntireFile(ivfClusters);
    }

    protected FlatVectorsReader getReaderForField(String field) {
        FieldInfo info = fieldInfos.fieldInfo(field);
        if (info == null) throw new IllegalArgumentException("Could not find field [" + field + "]");
        return genericReaders.getReaderForField(info.number);
    }

    @Override
    public final FloatVectorValues getFloatVectorValues(String field) throws IOException {
        return getReaderForField(field).getFloatVectorValues(field);
    }

    @Override
    public final ByteVectorValues getByteVectorValues(String field) throws IOException {
        return getReaderForField(field).getByteVectorValues(field);
    }

    @Override
    public final void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        final FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32) == false) {
            getReaderForField(field).search(field, target, knnCollector, acceptDocs);
            return;
        }
        if (fieldInfo.getVectorDimension() != target.length) {
            throw new IllegalArgumentException(
                "vector query dimension: " + target.length + " differs from field dimension: " + fieldInfo.getVectorDimension()
            );
        }

        final ESAcceptDocs esAcceptDocs;
        if (acceptDocs instanceof ESAcceptDocs) {
            esAcceptDocs = (ESAcceptDocs) acceptDocs;
        } else {
            esAcceptDocs = null;
        }

        FloatVectorValues values = getReaderForField(field).getFloatVectorValues(field);
        int numVectors = values.size();
        final float approximateCost;
        if (esAcceptDocs == ESAcceptDocs.ESAcceptDocsAll.INSTANCE) {
            approximateCost = numVectors;
        } else {
            approximateCost = esAcceptDocs == null ? acceptDocs.cost() : esAcceptDocs.approximateCost();
        }
        float percentFiltered = Math.max(0f, Math.min(1f, approximateCost / numVectors));
        int k = knnCollector.k();
        int numCands = k;
        float visitRatio = dynamicVisitRatio;
        // Search strategy may be null if this is being called from checkIndex (e.g. from a test)
        if (knnCollector.getSearchStrategy() instanceof IVFKnnSearchStrategy ivfSearchStrategy) {
            visitRatio = ivfSearchStrategy.getVisitRatio();
            numCands = ivfSearchStrategy.getNumCands();
            k = ivfSearchStrategy.getK();
        }

        FieldEntry entry = fields.get(fieldInfo.number);
        if (visitRatio == dynamicVisitRatio) {
            visitRatio = Math.min(computeDynamicVisitRatio(numCands, k), computeSegmentSizeCap(numVectors));
        }
        // we account for soar vectors here. We can potentially visit a vector twice so we multiply by 2 here.
        long maxVectorVisited = (long) (2.0 * visitRatio * numVectors);
        IndexInput postListSlice = entry.postingListSlice(ivfClusters);
        CentroidIterator centroidPrefetchingIterator = getCentroidIterator(
            fieldInfo,
            entry.numCentroids,
            entry.centroidSlice(ivfCentroids),
            target,
            postListSlice,
            acceptDocs,
            approximateCost,
            values,
            visitRatio
        );
        Bits acceptDocsBits = acceptDocs.bits();
        PostingVisitor scorer = getPostingVisitor(fieldInfo, postListSlice, target, acceptDocsBits, entry.centroidSlice(ivfCentroids));
        long expectedDocs = 0;
        long actualDocs = 0;
        // initially we visit only the "centroids to search"
        // Note, numCollected is doing the bare minimum here.
        // TODO do we need to handle nested doc counts similarly to how we handle
        // filtering? E.g. keep exploring until we hit an expected number of parent documents vs. child vectors?
        while (centroidPrefetchingIterator.hasNext()
            && (maxVectorVisited > expectedDocs || knnCollector.minCompetitiveSimilarity() == Float.NEGATIVE_INFINITY)) {
            PostingMetadata postingMetadata = centroidPrefetchingIterator.nextPosting();
            expectedDocs += scorer.resetPostingsScorer(postingMetadata);
            actualDocs += scorer.visit(knnCollector);
            if (knnCollector.getSearchStrategy() != null) {
                knnCollector.getSearchStrategy().nextVectorsBlock();
            }
        }
        if (acceptDocsBits != null) {
            // TODO Adjust the value here when using centroid filtering
            float unfilteredRatioVisited = (float) expectedDocs / numVectors;
            int filteredVectors = (int) Math.ceil(numVectors * percentFiltered);
            float expectedScored = Math.min(2 * filteredVectors * unfilteredRatioVisited, expectedDocs / 2f);
            while (centroidPrefetchingIterator.hasNext() && (actualDocs < expectedScored || actualDocs < knnCollector.k())) {
                PostingMetadata postingMetadata = centroidPrefetchingIterator.nextPosting();
                scorer.resetPostingsScorer(postingMetadata);
                actualDocs += scorer.visit(knnCollector);
                if (knnCollector.getSearchStrategy() != null) {
                    knnCollector.getSearchStrategy().nextVectorsBlock();
                }
            }
        }
    }

    /**
     * Computes the dynamic visit ratio using the Two-Signal model.
     * The formula blends the num_candidates/k ratio signal with the k magnitude signal.
     */
    static float computeDynamicVisitRatio(int numCands, int k) {
        double r = (double) numCands / Math.max(k, 1);
        double z = RATIO_WEIGHT * logScale(r - 1.0, LOG1P_R_MAX) + K_WEIGHT * logScale(k, LOG1P_K_MAX);
        return (float) (V_MIN + (V_MAX - V_MIN) * z);
    }

    private static double logScale(double value, double log1pMax) {
        return Math.max(0.0, Math.min(1.0, Math.log1p(value) / log1pMax));
    }

    /**
     * Computes a segment-size-aware cap on the visit ratio.
     * Larger segments have better-formed IVF clusters and need a lower visit ratio to achieve the target recall.
     * The power-law curve is calibrated on multi-dataset experiments (GIST-1M, Wiki-Cohere, MSMarco-130M).
     * <p>
     * Formula: cap = {@link #CAP_COEFFICIENT} * ({@link #CAP_REF_SIZE} / numVectors)^{@link #CAP_EXPONENT}
     *              * (0.1 / (1 - targetRecall))
     *
     * @param numVectors number of vectors in the segment
     * @return the upper-bound visit ratio for this segment size
     */
    static float computeSegmentSizeCap(int numVectors) {
        if (numVectors <= 0) {
            return (float) V_MAX;
        }
        double sizeScale = Math.pow((double) CAP_REF_SIZE / numVectors, CAP_EXPONENT);
        double recallScale = 0.1 / (1.0 - DEFAULT_TARGET_RECALL);
        return (float) Math.min(1.0, CAP_COEFFICIENT * sizeScale * recallScale);
    }

    @Override
    public final void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        final FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
        final ByteVectorValues values = getReaderForField(field).getByteVectorValues(field);
        for (int i = 0; i < values.size(); i++) {
            final float score = fieldInfo.getVectorSimilarityFunction().compare(target, values.vectorValue(i));
            knnCollector.collect(values.ordToDoc(i), score);
            if (knnCollector.earlyTerminated()) {
                return;
            }
        }
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
        var raw = getReaderForField(fieldInfo.name).getOffHeapByteSize(fieldInfo);
        FieldEntry fe = fields.get(fieldInfo.number);
        if (fe == null) {
            assert fieldInfo.getVectorEncoding() == VectorEncoding.BYTE;
            return raw;
        }

        var centroidsClusters = Map.of(centroidExtension, fe.centroidLength, clusterExtension, fe.postingListLength);
        return KnnVectorsReader.mergeOffHeapByteSizeMaps(raw, centroidsClusters);
    }

    @Override
    public void close() throws IOException {
        List<Closeable> closeables = new ArrayList<>(genericReaders.allReaders());
        Collections.addAll(closeables, ivfCentroids, ivfClusters);
        IOUtils.close(closeables);
    }

    protected static class FieldEntry implements GenericFlatVectorReaders.Field {
        protected final String rawVectorFormatName;
        protected final boolean useDirectIOReads;
        protected final VectorSimilarityFunction similarityFunction;
        protected final VectorEncoding vectorEncoding;
        protected final int numCentroids;
        protected final long centroidOffset;
        protected final long centroidLength;
        protected final long postingListOffset;
        protected final long postingListLength;
        protected final float[] globalCentroid;
        protected final float globalCentroidDp;
        protected final int bulkSize;

        public FieldEntry(
            String rawVectorFormatName,
            boolean useDirectIOReads,
            VectorSimilarityFunction similarityFunction,
            VectorEncoding vectorEncoding,
            int numCentroids,
            long centroidOffset,
            long centroidLength,
            long postingListOffset,
            long postingListLength,
            float[] globalCentroid,
            float globalCentroidDp,
            int bulkSize
        ) {
            this.rawVectorFormatName = rawVectorFormatName;
            this.useDirectIOReads = useDirectIOReads;
            this.similarityFunction = similarityFunction;
            this.vectorEncoding = vectorEncoding;
            this.numCentroids = numCentroids;
            this.centroidOffset = centroidOffset;
            this.centroidLength = centroidLength;
            this.postingListOffset = postingListOffset;
            this.postingListLength = postingListLength;
            this.globalCentroid = globalCentroid;
            this.globalCentroidDp = globalCentroidDp;
            this.bulkSize = bulkSize;
        }

        @Override
        public String rawVectorFormatName() {
            return rawVectorFormatName;
        }

        @Override
        public boolean useDirectIOReads() {
            return useDirectIOReads;
        }

        public int numCentroids() {
            return numCentroids;
        }

        public float[] globalCentroid() {
            return globalCentroid;
        }

        public float globalCentroidDp() {
            return globalCentroidDp;
        }

        public VectorSimilarityFunction similarityFunction() {
            return similarityFunction;
        }

        public IndexInput centroidSlice(IndexInput centroidFile) throws IOException {
            return centroidFile.slice("centroids", centroidOffset, centroidLength);
        }

        public IndexInput postingListSlice(IndexInput postingListFile) throws IOException {
            return postingListFile.slice("postingLists", postingListOffset, postingListLength);
        }

        public int getBulkSize() {
            return bulkSize;
        }
    }

    public abstract PostingVisitor getPostingVisitor(
        FieldInfo fieldInfo,
        IndexInput postingsLists,
        float[] target,
        Bits needsScoring,
        IndexInput centroidSlice
    ) throws IOException;

    public interface PostingVisitor {
        /** returns the number of documents in the posting list */
        int resetPostingsScorer(PostingMetadata metadata) throws IOException;

        /** returns the number of scored documents */
        int visit(KnnCollector collector) throws IOException;
    }

}
