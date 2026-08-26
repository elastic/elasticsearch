/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;
import org.elasticsearch.common.CheckedIntFunction;
import org.elasticsearch.index.codec.vectors.GenericFlatVectorReaders;
import org.elasticsearch.index.codec.vectors.ash.AshPostingsVisitor;
import org.elasticsearch.index.codec.vectors.ash.AshProjectionMatrix;
import org.elasticsearch.index.codec.vectors.cluster.ClusteringVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIndexFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIterator;
import org.elasticsearch.index.codec.vectors.diskbbq.FlatCentroidIndex;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader;
import org.elasticsearch.index.codec.vectors.diskbbq.PrefetchingCentroidIterator;
import org.elasticsearch.lucene.store.MemorySegmentAccessInputAccess;
import org.elasticsearch.search.vectors.ESAcceptDocs;
import org.elasticsearch.simdvec.AsymmetricHashingVectorsScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ASH-specific implementation of {@link IVFVectorsReader}. Scores posting lists using
 * {@link AshPostingsVisitor} with a precomputed projection matrix ({@link AshProjectionMatrix}).
 */
public class ESNextDiskASHVectorsReader extends IVFVectorsReader<ESNextDiskASHVectorsReader.ASHFieldEntry> {

    private final ConcurrentHashMap<String, AshProjectionMatrix> ashMatrixCache;

    /** Default query quantization bits for ASH integer scoring (D2Q4). Set to 0 to use the float path. */
    static final int DEFAULT_ASH_QUERY_BITS_PER_DIM = 4;

    private final int queryBitsPerDim;

    public ESNextDiskASHVectorsReader(SegmentReadState state, GenericFlatVectorReaders.LoadFlatVectorsReader getFormatReader)
        throws IOException {
        this(state, getFormatReader, DEFAULT_ASH_QUERY_BITS_PER_DIM);
    }

    public ESNextDiskASHVectorsReader(
        SegmentReadState state,
        GenericFlatVectorReaders.LoadFlatVectorsReader getFormatReader,
        int queryBitsPerDim
    ) throws IOException {
        super(
            state,
            getFormatReader,
            ESNextDiskASHVectorsFormat.NAME,
            ESNextDiskASHVectorsFormat.CENTROID_EXTENSION,
            ESNextDiskASHVectorsFormat.CLUSTER_EXTENSION,
            ESNextDiskASHVectorsFormat.IVF_META_EXTENSION,
            ESNextDiskASHVectorsFormat.VERSION_START,
            ESNextDiskASHVectorsFormat.VERSION_CURRENT,
            ESNextDiskASHVectorsFormat.VERSION_DIRECT_IO,
            ESNextDiskASHVectorsFormat.DYNAMIC_VISIT_RATIO
        );
        this.ashMatrixCache = new ConcurrentHashMap<>();
        this.queryBitsPerDim = queryBitsPerDim;
    }

    private ESNextDiskASHVectorsReader(ESNextDiskASHVectorsReader other, GenericFlatVectorReaders genericReaders) {
        super(other, genericReaders);
        this.ashMatrixCache = other.ashMatrixCache;
        this.queryBitsPerDim = other.queryBitsPerDim;
    }

    @Override
    protected ESNextDiskASHVectorsReader mergeInstance(GenericFlatVectorReaders genericReaders) {
        return new ESNextDiskASHVectorsReader(this, genericReaders);
    }

    @Override
    protected int getNumberOfVectors(ASHFieldEntry entry, KnnVectorValues values, IndexInput centroidSlice, ESAcceptDocs esAcceptDocs)
        throws IOException {
        int size = values.size();
        assert esAcceptDocs == null
            || entry.numSlices >= 0 && esAcceptDocs.sliceOrd() >= 0
            || entry.numSlices == -1 && esAcceptDocs.sliceOrd() == -1;
        if (entry.numSlices > 0) {
            long fp = centroidSlice.getFilePointer();
            final int bitsRequired = DirectWriter.bitsRequired(entry.maxSliceSize);
            final long sizeLookup = DirectWriter.bytesRequired(entry.numSlices, bitsRequired);
            if (esAcceptDocs != null) {
                int sliceOrd = esAcceptDocs.sliceOrd();
                assert sliceOrd < entry.numSlices : "sliceOrd out of range for centroid slices";
                final LongValues longValues = DirectReader.getInstance(centroidSlice.randomAccessSlice(fp, sizeLookup), bitsRequired);
                size = (int) longValues.get(sliceOrd);
            }
            centroidSlice.seek(fp + sizeLookup);
        }
        return size;
    }

    CentroidIterator getPostingListPrefetchIterator(CentroidIterator centroidIterator, IndexInput postingListSlice) throws IOException {
        return new PrefetchingCentroidIterator(centroidIterator, postingListSlice);
    }

    @Override
    public CentroidIterator getCentroidIterator(
        FieldInfo fieldInfo,
        int numCentroids,
        IndexInput centroids,
        QueryTarget queryTarget,
        IndexInput postingListSlice,
        AcceptDocs acceptDocs,
        float approximateCost,
        KnnVectorValues values,
        float visitRatio
    ) throws IOException {
        float[] targetQuery = switch (queryTarget) {
            case QueryTarget.FloatQuery fq -> fq.vector();
            case QueryTarget.ByteQuery bq -> {
                float[] widened = new float[bq.vector().length];
                for (int i = 0; i < bq.vector().length; i++) {
                    widened[i] = bq.vector()[i];
                }
                yield widened;
            }
        };
        ASHFieldEntry fieldEntry = fields.get(fieldInfo.number);
        var iterator = switch (fieldEntry.centroidIndexFormat()) {
            case FLAT -> new FlatCentroidIndex(
                fieldInfo,
                fieldEntry,
                numCentroids,
                centroids,
                targetQuery,
                acceptDocs,
                approximateCost,
                values,
                visitRatio,
                false // ASH only supports float centroids
            ).getIterator();
        };
        return getPostingListPrefetchIterator(iterator, postingListSlice);
    }

    @Override
    protected ASHFieldEntry doReadField(
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
    ) throws IOException {
        int bulkSize = input.readInt();
        CentroidIndexFormat centroidIndexFormat = CentroidIndexFormat.fromId(input.readInt());
        long preconditionerLength = input.readLong();
        long preconditionerOffset = -1;
        if (preconditionerLength > 0) {
            preconditionerOffset = input.readLong();
        }
        int numSlices = input.readInt();
        int maxSliceSize = 0;
        if (numSlices > 0) {
            maxSliceSize = input.readVInt();
        }
        int ashBitsPerDim = input.readVInt();
        return new ASHFieldEntry(
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
            globalCentroidDp,
            centroidIndexFormat,
            bulkSize,
            preconditionerOffset,
            preconditionerLength,
            numSlices,
            maxSliceSize,
            ashBitsPerDim
        );
    }

    private AshProjectionMatrix getAshProjectionMatrix(FieldInfo fieldInfo) {
        return ashMatrixCache.computeIfAbsent(fieldInfo.name, name -> {
            try {
                final ASHFieldEntry fieldEntry = fields.get(fieldInfo.number);
                long preconditionerOffset = fieldEntry.preconditionerOffset;
                long preconditionerLength = fieldEntry.preconditionerLength;
                if (preconditionerLength <= 0) {
                    throw new IllegalStateException("ASH segment missing projection matrix for field: " + fieldInfo.name);
                }
                IndexInput slice = ivfCentroids.slice("ash-preconditioner", preconditionerOffset, preconditionerLength);
                slice.seek(0);
                var matrix = AshProjectionMatrix.read(slice);
                // Eagerly compute wT so it's ready for concurrent search threads
                matrix.wT();
                return matrix;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public PostingVisitor getPostingVisitor(
        FieldInfo fieldInfo,
        KnnVectorValues values,
        IndexInput indexInput,
        QueryTarget queryTarget,
        Bits needsScoring,
        IndexInput centroidSlice,
        ESAcceptDocs acceptDocs
    ) throws IOException {
        ASHFieldEntry entry = fields.get(fieldInfo.number);
        float[] target = switch (queryTarget) {
            case QueryTarget.FloatQuery fq -> fq.vector();
            case QueryTarget.ByteQuery bq -> {
                float[] widened = new float[bq.vector().length];
                for (int i = 0; i < bq.vector().length; i++) {
                    widened[i] = bq.vector()[i];
                }
                yield widened;
            }
        };

        var ashMatrix = getAshProjectionMatrix(fieldInfo);
        int dimension = fieldInfo.getVectorDimension();
        int numCentroids = entry.numCentroids();
        // Raw float centroids are stored as the last (numCentroids * dimension * Float.BYTES) bytes
        // of the centroid file, written contiguously by FlatCentroidIndexWriter.writeCentroidData().
        long rawCentroidsOffset = centroidSlice.length() - (long) numCentroids * dimension * Float.BYTES;
        IndexInput centroidInput = centroidSlice.clone();
        float[] centroidBuf = new float[dimension];
        CheckedIntFunction<float[], IOException> centroidReader = (int ord) -> {
            centroidInput.seek(rawCentroidsOffset + (long) ord * dimension * Float.BYTES);
            centroidInput.readFloats(centroidBuf, 0, dimension);
            return centroidBuf;
        };
        // Unwrap once so the scorer and visitor share the same IndexInput object.
        // This mirrors how the BBQ MemorySegmentPostingsVisitor passes a single input
        // to both the ES940OSQVectorsScorer and the visitor's own correction reads.
        IndexInput unwrappedInput = FilterIndexInput.unwrapOnlyTest(indexInput);
        unwrappedInput = MemorySegmentAccessInputAccess.unwrap(unwrappedInput);

        int nDims = ashMatrix.wT().length / dimension;
        AsymmetricHashingVectorsScorer scorer = ESVectorUtil.getASHVectorsScorer(
            unwrappedInput,
            nDims,
            entry.ashBitsPerDim(),
            queryBitsPerDim
        );
        return new AshPostingsVisitor(
            ashMatrix.wT(),
            dimension,
            target,
            fieldInfo.getVectorSimilarityFunction(),
            scorer,
            unwrappedInput,
            needsScoring,
            entry.ashBitsPerDim(),
            queryBitsPerDim,
            centroidReader
        );
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public CentroidData<?> readCentroidData(String fieldName) throws IOException {
        FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldName);
        if (fieldInfo == null) {
            return null;
        }
        ASHFieldEntry entry = fields.get(fieldInfo.number);
        if (entry == null || entry.numCentroids() == 0) {
            return null;
        }
        int dimension = fieldInfo.getVectorDimension();
        int numCentroids = entry.numCentroids();
        final KnnVectorValues vectorValues = getFloatVectorValues(fieldInfo.name);
        int numVectors = vectorValues != null ? vectorValues.size() : 0;
        int[] clusterSizes = new int[numCentroids];

        // ASH always uses float centroids
        long rawCentroidsSize = (long) numCentroids * dimension * Float.BYTES;

        IndexInput centroidsSlice = null;
        boolean success = false;
        try (IndexInput centroidSlice = entry.centroidSlice(ivfCentroids); IndexInput postingSlice = entry.postingListSlice(ivfClusters)) {
            long[] postingOffsets = readPostingListOffsets(centroidSlice, numVectors, numCentroids, dimension, Float.BYTES);

            for (int c = 0; c < numCentroids; c++) {
                postingSlice.seek(postingOffsets[c] + Integer.BYTES);
                clusterSizes[c] = postingSlice.readVInt();
            }

            long centroidsOffset = centroidSlice.length() - rawCentroidsSize;
            centroidsSlice = centroidSlice.slice("centroids-raw", centroidsOffset, rawCentroidsSize);
            ClusteringVectorValues centroids = KMeansFloatVectorValues.build(centroidsSlice, null, numCentroids, dimension);
            CentroidData data = new CentroidData(centroids, clusterSizes, entry.globalCentroid(), centroidsSlice);
            success = true;
            return data;
        } finally {
            if (success == false && centroidsSlice != null) {
                centroidsSlice.close();
            }
        }
    }

    private static long[] readPostingListOffsets(
        IndexInput centroidSlice,
        int numVectors,
        int numCentroids,
        int dimension,
        int bytesPerComponent
    ) throws IOException {
        long[] offsets = new long[numCentroids];
        int bitsRequired = DirectWriter.bitsRequired(numCentroids);
        long sizeLookup = DirectWriter.bytesRequired(numVectors, bitsRequired);
        centroidSlice.seek(sizeLookup);
        int numParents = centroidSlice.readVInt();
        long rawCentroidsSize = (long) numCentroids * dimension * bytesPerComponent;
        long offsetTableEntrySize = numParents == 0 ? 2L * Long.BYTES : 2L * Long.BYTES + Integer.BYTES;
        long offsetTableStart = centroidSlice.length() - rawCentroidsSize - offsetTableEntrySize * numCentroids;

        centroidSlice.seek(offsetTableStart);
        for (int i = 0; i < numCentroids; i++) {
            offsets[i] = centroidSlice.readLong();
            centroidSlice.readLong();
            if (numParents > 0) {
                centroidSlice.readInt();
            }
        }
        return offsets;
    }

    /**
     * ASH-specific field entry. Extends {@link FieldEntry} with the ASH bits-per-dimension
     * configuration used for document vector encoding.
     */
    public static class ASHFieldEntry extends FieldEntry {
        private final CentroidIndexFormat centroidIndexFormat;
        final long preconditionerOffset;
        final long preconditionerLength;
        final int numSlices;
        final int maxSliceSize;
        private final int ashBitsPerDim;

        ASHFieldEntry(
            String rawVectorFormat,
            boolean doDirectIOReads,
            VectorSimilarityFunction similarityFunction,
            VectorEncoding vectorEncoding,
            int numCentroids,
            long centroidOffset,
            long centroidLength,
            long postingListOffset,
            long postingListLength,
            float[] globalCentroid,
            float globalCentroidDp,
            CentroidIndexFormat centroidIndexFormat,
            int bulkSize,
            long preconditionerOffset,
            long preconditionerLength,
            int numSlices,
            int maxSliceSize,
            int ashBitsPerDim
        ) {
            super(
                rawVectorFormat,
                doDirectIOReads,
                similarityFunction,
                vectorEncoding,
                numCentroids,
                centroidOffset,
                centroidLength,
                postingListOffset,
                postingListLength,
                globalCentroid,
                globalCentroidDp,
                bulkSize
            );
            this.centroidIndexFormat = centroidIndexFormat;
            this.preconditionerOffset = preconditionerOffset;
            this.preconditionerLength = preconditionerLength;
            this.numSlices = numSlices;
            this.maxSliceSize = maxSliceSize;
            this.ashBitsPerDim = ashBitsPerDim;
        }

        public CentroidIndexFormat centroidIndexFormat() {
            return centroidIndexFormat;
        }

        public long preconditionerOffset() {
            return preconditionerOffset;
        }

        public long preconditionerLength() {
            return preconditionerLength;
        }

        public int ashBitsPerDim() {
            return ashBitsPerDim;
        }

        @Override
        public int numSlices() {
            return numSlices;
        }
    }
}
