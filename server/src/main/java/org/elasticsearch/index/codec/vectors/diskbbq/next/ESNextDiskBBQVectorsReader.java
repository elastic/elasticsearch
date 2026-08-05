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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;
import org.elasticsearch.index.codec.vectors.GenericFlatVectorReaders;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.cluster.ClusteringVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.KMeansByteVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.index.codec.vectors.diskbbq.CalibrationAwareReader;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIndexFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIterator;
import org.elasticsearch.index.codec.vectors.diskbbq.DocIdsWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.FlatCentroidIndex;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader.QueryTarget;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfAutoCalibration;
import org.elasticsearch.index.codec.vectors.diskbbq.PostingMetadata;
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;
import org.elasticsearch.index.codec.vectors.diskbbq.PrefetchingCentroidIterator;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.VectorPreconditioner;
import org.elasticsearch.search.vectors.BulkKnnCollector;
import org.elasticsearch.search.vectors.ESAcceptDocs;
import org.elasticsearch.simdvec.ES940OSQVectorsScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer.DEFAULT_LAMBDA;
import static org.elasticsearch.index.codec.vectors.diskbbq.PostingMetadata.NO_ORDINAL;
import static org.elasticsearch.simdvec.ES940OSQVectorsScorer.BULK_SIZE;

/**
 * Default implementation of {@link IVFVectorsReader}. It scores the posting lists centroids using
 * brute force and then scores the top ones using the posting list.
 */
public class ESNextDiskBBQVectorsReader extends IVFVectorsReader<ESNextDiskBBQVectorsReader.NextFieldEntry>
    implements
        VectorPreconditioner,
        CalibrationAwareReader {

    public ESNextDiskBBQVectorsReader(SegmentReadState state, GenericFlatVectorReaders.LoadFlatVectorsReader getFormatReader)
        throws IOException {
        super(
            state,
            getFormatReader,
            ESNextDiskBBQVectorsFormat.NAME,
            ESNextDiskBBQVectorsFormat.CENTROID_EXTENSION,
            ESNextDiskBBQVectorsFormat.CLUSTER_EXTENSION,
            ESNextDiskBBQVectorsFormat.IVF_META_EXTENSION,
            ESNextDiskBBQVectorsFormat.VERSION_START,
            ESNextDiskBBQVectorsFormat.VERSION_CURRENT,
            ESNextDiskBBQVectorsFormat.VERSION_DIRECT_IO,
            ESNextDiskBBQVectorsFormat.DYNAMIC_VISIT_RATIO
        );
    }

    CentroidIterator getPostingListPrefetchIterator(CentroidIterator centroidIterator, IndexInput postingListSlice) throws IOException {
        // TODO we may want to prefetch more than one postings list, however, we will likely want to place a limit
        // so we don't bother prefetching many lists we won't end up scoring
        return new PrefetchingCentroidIterator(centroidIterator, postingListSlice);
    }

    @Override
    protected int getNumberOfVectors(NextFieldEntry entry, KnnVectorValues values, IndexInput centroidSlice, ESAcceptDocs esAcceptDocs)
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

    @Override
    public float getOversampleFactor(FieldInfo fieldInfo) {
        final NextFieldEntry e = fields.get(fieldInfo.number);
        if (e == null) {
            return IvfAutoCalibration.NO_CALIBRATED_OVERSAMPLE;
        }
        float r = e.rescoreOversample();
        return Float.isFinite(r) ? r : IvfAutoCalibration.NO_CALIBRATED_OVERSAMPLE;
    }

    @Override
    public boolean shouldPrecondition(FieldInfo fieldInfo) {
        final NextFieldEntry e = fields.get(fieldInfo.number);
        return e != null && e.preconditionerLength() > 0;
    }

    @Override
    public QuantEncoding getQuantEncoding(FieldInfo fieldInfo) {
        final NextFieldEntry e = fields.get(fieldInfo.number);
        return e == null ? null : e.quantEncoding();
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
        // Extract float target for FlatCentroidIndex (byte queries are converted at the IVF search entry point)
        // TODO: For byte queries, this widens to float because FlatCentroidIndex quantizes the query against the
        // global centroid (float) via scalarQuantize(float[], ..., float[]). A mixed scalarQuantize(byte[], ..., float[])
        // overload in OptimizedScalarQuantizer + ESVectorUtil SIMD layer would eliminate this widening.
        // Low priority — only widens a single query vector, not a hot path.
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
        ESNextDiskBBQVectorsReader.NextFieldEntry fieldEntry = fields.get(fieldInfo.number);
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
                fieldEntry.byteCentroids()
            ).getIterator();
        };
        return getPostingListPrefetchIterator(iterator, postingListSlice);
    }

    @Override
    protected NextFieldEntry doReadField(
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
        QuantEncoding quantEncoding = QuantEncoding.fromId(input.readInt());
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
        float rescoreOversample = Float.intBitsToFloat(input.readInt());
        // ESNext format extension: byte centroid flag
        boolean byteCentroids = input.readByte() == 1;
        return new NextFieldEntry(
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
            quantEncoding,
            bulkSize,
            preconditionerOffset,
            preconditionerLength,
            numSlices,
            maxSliceSize,
            rescoreOversample,
            byteCentroids
        );
    }

    @Override
    public Preconditioner getPreconditioner(FieldInfo fieldInfo) throws IOException {
        final NextFieldEntry fieldEntry = fields.get(fieldInfo.number);
        // only seems possible in tests
        if (fieldEntry == null) {
            return null;
        }
        long preconditionerOffset = fieldEntry.preconditionerOffset();
        long preconditionerLength = fieldEntry.preconditionerLength();
        if (preconditionerLength > 0) {
            IndexInput ivfPreconditionerSlice = ivfCentroids.slice("preconditioner", preconditionerOffset, preconditionerLength);
            if (ivfPreconditionerSlice != null) {
                ivfPreconditionerSlice.seek(0);
                return Preconditioner.read(ivfPreconditionerSlice);
            }
        }
        return null;
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public CentroidData<?> readCentroidData(String fieldName) throws IOException {
        FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldName);
        if (fieldInfo == null) {
            return null;
        }
        NextFieldEntry entry = fields.get(fieldInfo.number);
        if (entry == null || entry.numCentroids() == 0) {
            return null;
        }
        int dimension = fieldInfo.getVectorDimension();
        int numCentroids = entry.numCentroids();
        final KnnVectorValues vectorValues;
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.BYTE)) {
            vectorValues = getByteVectorValues(fieldInfo.name);
        } else {
            vectorValues = getFloatVectorValues(fieldInfo.name);
        }
        int numVectors = vectorValues != null ? vectorValues.size() : 0;
        int[] clusterSizes = new int[numCentroids];

        // Byte-backed fields store centroids as 1 byte per dimension;
        // float-backed fields store centroids as 4 bytes (float) per dimension.
        boolean byteBacked = entry.byteCentroids();
        int bytesPerComponent = byteBacked ? Byte.BYTES : Float.BYTES;
        long rawCentroidsSize = (long) numCentroids * dimension * bytesPerComponent;

        IndexInput centroidsSlice = null;
        boolean success = false;
        try (IndexInput centroidSlice = entry.centroidSlice(ivfCentroids); IndexInput postingSlice = entry.postingListSlice(ivfClusters)) {
            long[] postingOffsets = readPostingListOffsets(centroidSlice, numVectors, numCentroids, dimension, bytesPerComponent);

            // First pass: read cluster sizes only (from the posting slice).
            for (int c = 0; c < numCentroids; c++) {
                postingSlice.seek(postingOffsets[c] + Integer.BYTES);
                clusterSizes[c] = postingSlice.readVInt();
            }

            // The raw centroids live contiguously at the end of the centroid data; slice that
            // region and hand it to the streaming view. The slice owns its own resources and
            // outlives the parent centroidSlice.
            long centroidsOffset = centroidSlice.length() - rawCentroidsSize;
            centroidsSlice = centroidSlice.slice("centroids-raw", centroidsOffset, rawCentroidsSize);
            ClusteringVectorValues centroids;
            if (byteBacked) {
                centroids = KMeansByteVectorValues.build(centroidsSlice, null, numCentroids, dimension);
            } else {
                centroids = KMeansFloatVectorValues.build(centroidsSlice, null, numCentroids, dimension);
            }
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

    public static class NextFieldEntry extends FieldEntry {
        private final CentroidIndexFormat centroidIndexFormat;
        private final QuantEncoding quantEncoding;
        protected final long preconditionerOffset;
        protected final long preconditionerLength;
        // -1 "not sliced".
        // 0 "sliced but on flush".
        // > 0 "sliced but on merge, is the number of slices".
        final int numSlices;
        final int maxSliceSize;
        private final float rescoreOversample;
        private final boolean byteCentroids;

        NextFieldEntry(
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
            QuantEncoding quantEncoding,
            int bulkSize,
            long preconditionerOffset,
            long preconditionerLength,
            int numSlices,
            int maxSliceSize,
            float rescoreOversample,
            boolean byteCentroids
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
            this.quantEncoding = quantEncoding;
            this.preconditionerOffset = preconditionerOffset;
            this.preconditionerLength = preconditionerLength;
            this.numSlices = numSlices;
            this.maxSliceSize = maxSliceSize;
            this.rescoreOversample = rescoreOversample;
            this.byteCentroids = byteCentroids;
        }

        public CentroidIndexFormat centroidIndexFormat() {
            return centroidIndexFormat;
        }

        public QuantEncoding quantEncoding() {
            return quantEncoding;
        }

        public long preconditionerOffset() {
            return preconditionerOffset;
        }

        public long preconditionerLength() {
            return preconditionerLength;
        }

        public float rescoreOversample() {
            return rescoreOversample;
        }

        public boolean byteCentroids() {
            return byteCentroids;
        }

        @Override
        public int numSlices() {
            return numSlices;
        }
    }

    @Override
    protected long maxVectorsToVisit(NextFieldEntry entry, float visitRatio, int numVectors) {
        return switch (entry.centroidIndexFormat()) {
            case FLAT -> super.maxVectorsToVisit(entry, visitRatio, numVectors);
        };
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
        NextFieldEntry entry = fields.get(fieldInfo.number);
        // Extract float target for QueryQuantizer (byte queries are widened at the search entry point)
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
        if (entry.numSlices > 0) {
            final int bitsRequired = DirectWriter.bitsRequired(entry.maxSliceSize);
            final long sizeLookup = DirectWriter.bytesRequired(entry.numSlices, bitsRequired);
            centroidSlice.skipBytes(sizeLookup);
        }
        final int bitsRequired = DirectWriter.bitsRequired(entry.numCentroids());
        final long sizeLookup = DirectWriter.bytesRequired(values.size(), bitsRequired);
        centroidSlice.skipBytes(sizeLookup);
        QuantEncoding quantEncoding = entry.quantEncoding();
        int numParents = centroidSlice.readVInt();
        if (entry.numSlices > 0) {
            // skip slice offsets
            centroidSlice.skipBytes((long) entry.numSlices * Integer.BYTES);
        }
        final QueryQuantizer queryQuantizer;
        if (numParents > 0) {
            // unused
            int longestPostingList = centroidSlice.readVInt();
            // Parent centroids (second-level cluster centers) are always stored as floats
            // since they are arithmetic means of leaf centroids and don't fit in byte range.
            IndexInput parentsSlice = centroidSlice.slice(
                "parents-slice",
                centroidSlice.getFilePointer(),
                (long) numParents * fieldInfo.getVectorDimension() * Float.BYTES
            );
            queryQuantizer = new QueryQuantizer(quantEncoding, fieldInfo, target, parentsSlice, entry.globalCentroid());
        } else {
            queryQuantizer = new QueryQuantizer(quantEncoding, fieldInfo, target, null, entry.globalCentroid());
        }
        if (entry.numSlices == 0) {
            // Sliced segment without per-slice centroid structure (e.g. byte fields during flush).
            // Uses SlicedMemorySegmentPostingsVisitor which handles the flat posting list format.
            int startDoc;
            int endDoc;
            if (acceptDocs instanceof ESAcceptDocs esAccept && esAccept.sliceAcceptDocs() != null) {
                ESAcceptDocs.SliceAcceptDocs sliceAcceptDocs = esAccept.sliceAcceptDocs();
                startDoc = sliceAcceptDocs.startDoc();
                endDoc = sliceAcceptDocs.endDoc();
            } else {
                startDoc = 0;
                endDoc = values.ordToDoc(values.size() - 1) + 1;
            }
            return new SlicedMemorySegmentPostingsVisitor(
                queryQuantizer,
                quantEncoding,
                indexInput,
                entry,
                fieldInfo,
                needsScoring,
                values,
                startDoc,
                endDoc
            );

        } else {
            return new MemorySegmentPostingsVisitor(queryQuantizer, quantEncoding, indexInput, entry, fieldInfo, needsScoring);
        }
    }

    private record QueryQuantizerResult(OptimizedScalarQuantizer.QuantizationResult queryCorrections, byte[] quantizedTarget) {}

    private static final int QUERY_CACHE_SIZE = 16;

    private static class QueryQuantizer {
        private final LinkedHashMap<Integer, QueryQuantizerResult> cache;
        private final QuantEncoding quantEncoding;
        private final float[] target;
        private final float[] scratch;
        private final int[] quantizationScratch;
        private final OptimizedScalarQuantizer quantizer;
        private final IndexInput parentsSlice;
        private final float[] globalCentroid;
        private final float[] centroidScratch;

        private int currentCentroidOrdinal = -2;
        private int nextCentroidOrdinal = -1;
        private byte[] evictedQuantizedQuery = null;
        private QueryQuantizerResult result = null;

        QueryQuantizer(QuantEncoding quantEncoding, FieldInfo fieldInfo, float[] target, IndexInput parentsSlice, float[] globalCentroid) {
            this.quantEncoding = quantEncoding;
            this.target = target;
            this.scratch = new float[fieldInfo.getVectorDimension()];
            this.centroidScratch = new float[fieldInfo.getVectorDimension()];
            this.quantizationScratch = new int[quantEncoding.discretizedDimensions(fieldInfo.getVectorDimension())];
            this.quantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction(), DEFAULT_LAMBDA, 1);
            this.parentsSlice = parentsSlice;
            this.globalCentroid = globalCentroid;

            this.cache = new LinkedHashMap<>(QUERY_CACHE_SIZE, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<Integer, QueryQuantizerResult> eldest) {
                    if (size() > QUERY_CACHE_SIZE) {
                        evictedQuantizedQuery = eldest.getValue().quantizedTarget();
                        return true;
                    }
                    return false;
                }
            };
        }

        void reset(int centroidOrdinal) {
            this.nextCentroidOrdinal = centroidOrdinal;
        }

        void quantizeQueryIfNecessary() throws IOException {
            if (nextCentroidOrdinal != currentCentroidOrdinal) {
                var quantized = cache.get(nextCentroidOrdinal);
                if (quantized != null) {
                    result = quantized;
                    currentCentroidOrdinal = nextCentroidOrdinal;
                    return;
                }
                // reuse the evicted byte array to reduce allocations
                final byte[] quantizedQuery = Objects.requireNonNullElseGet(
                    evictedQuantizedQuery,
                    () -> new byte[quantEncoding.getQueryPackedLength(target.length)]
                );
                final float[] queryCentroid;
                if (parentsSlice != null) {
                    assert nextCentroidOrdinal >= 0;
                    // Parent centroids are always stored as floats
                    parentsSlice.seek((long) nextCentroidOrdinal * centroidScratch.length * Float.BYTES);
                    parentsSlice.readFloats(centroidScratch, 0, centroidScratch.length);
                    queryCentroid = centroidScratch;
                } else {
                    assert nextCentroidOrdinal == NO_ORDINAL;
                    queryCentroid = globalCentroid;
                }

                OptimizedScalarQuantizer.QuantizationResult queryCorrections = quantizer.scalarQuantize(
                    target,
                    scratch,
                    quantizationScratch,
                    quantEncoding.queryBits(),
                    queryCentroid
                );
                quantEncoding.packQuery(quantizationScratch, quantizedQuery);
                currentCentroidOrdinal = nextCentroidOrdinal;
                result = new QueryQuantizerResult(queryCorrections, quantizedQuery);
                cache.put(nextCentroidOrdinal, result);
            }
        }

        OptimizedScalarQuantizer.QuantizationResult getQueryCorrections() {
            return result.queryCorrections();
        }

        byte[] getQuantizedTarget() {
            return result.quantizedTarget();
        }
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
        // TODO: override if adding new files
        return super.getOffHeapByteSize(fieldInfo);
    }

    private static class SlicedMemorySegmentPostingsVisitor extends MemorySegmentPostingsVisitor {
        final int startDocId;
        final int endDocId;
        final KnnVectorValues vectorValues;

        SlicedMemorySegmentPostingsVisitor(
            QueryQuantizer queryQuantizer,
            QuantEncoding quantEncoding,
            IndexInput indexInput,
            FieldEntry entry,
            FieldInfo fieldInfo,
            Bits acceptDocs,
            KnnVectorValues values,
            int startDocId,
            int endDocId
        ) throws IOException {
            super(queryQuantizer, quantEncoding, indexInput, entry, fieldInfo, acceptDocs);
            this.startDocId = startDocId;
            this.endDocId = endDocId;
            this.vectorValues = values;
        }

        @Override
        public int resetPostingsScorer(PostingMetadata metadata) throws IOException {
            int totalVectors = super.resetPostingsScorer(metadata);
            int totalBlocks = totalVectors / BULK_SIZE;
            KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
            if (iterator.advance(startDocId) >= endDocId) {
                this.vectors = 0;
                return 0;
            }
            int minOrd = iterator.index();
            int docId = iterator.advance(endDocId);
            int maxOrd;
            if (docId == DocIdSetIterator.NO_MORE_DOCS) {
                maxOrd = vectorValues.size();
            } else {
                maxOrd = iterator.index();
            }
            // When searching the full segment (startDocId == 0), the doc range may span
            // more ordinals than a single posting list in multi-centroid segments. In that case
            // we clamp to the posting list bounds rather than asserting.
            if (maxOrd - minOrd > totalVectors) {
                maxOrd = Math.min(maxOrd, minOrd + totalVectors);
            }
            int startBlock = minOrd / BULK_SIZE;
            int endBlock = (maxOrd - 1) / BULK_SIZE;
            if (endBlock == totalBlocks) {
                this.vectors = totalVectors - startBlock * BULK_SIZE;
            } else {
                this.vectors = (1 + endBlock - startBlock) * BULK_SIZE;
            }
            docBase = startBlock * BULK_SIZE;
            slicePos += startBlock * BULK_SIZE * quantizedByteLength;
            return this.vectors;
        }

        @Override
        protected int docToBulkScore(int[] docIds, int[] offsets, Bits acceptDocs, int bulkSize) {
            int docToScore = 0;
            for (int i = 0; i < bulkSize; i++) {
                if (docIds[i] == -1 || (acceptDocs != null && acceptDocs.get(docIds[i]) == false)) {
                    docIds[i] = -1;
                } else {
                    offsets[docToScore] = i;
                    docToScore++;
                }
            }
            return docToScore;
        }

        @Override
        protected void readDocIds(int count) {
            for (int j = 0; j < count; j++) {
                int docId = vectorValues.ordToDoc(docBase++);
                if (docId >= startDocId && docId < endDocId) {
                    docIdsScratch[j] = docId;
                } else {
                    docIdsScratch[j] = -1;
                }
            }
        }
    }

    private static class MemorySegmentPostingsVisitor implements PostingVisitor {
        final long quantizedByteLength;
        final IndexInput indexInput;
        final FieldEntry entry;
        final FieldInfo fieldInfo;
        final Bits acceptDocs;
        private final ES940OSQVectorsScorer osqVectorsScorer;
        final float[] scores = new float[BULK_SIZE];
        final float[] correctionsLower = new float[BULK_SIZE];
        final float[] correctionsUpper = new float[BULK_SIZE];
        final int[] correctionsSum = new int[BULK_SIZE];
        final float[] correctionsAdd = new float[BULK_SIZE];
        final int[] docIdsScratch = new int[BULK_SIZE];
        final int[] offsetsScratch = new int[BULK_SIZE];
        byte docEncoding;
        int docBase = 0;

        int vectors;
        float centroidToParentSqDist;
        float centroidDistance;
        long slicePos;

        private final QueryQuantizer queryQuantizer;
        final DocIdsWriter idsWriter = new DocIdsWriter();
        final VectorSimilarityFunction similarityFunction;
        final long quantizedVectorByteSize;

        MemorySegmentPostingsVisitor(
            QueryQuantizer queryQuantizer,
            QuantEncoding quantEncoding,
            IndexInput indexInput,
            FieldEntry entry,
            FieldInfo fieldInfo,
            Bits acceptDocs
        ) throws IOException {
            this.queryQuantizer = queryQuantizer;
            this.indexInput = indexInput;
            this.similarityFunction = fieldInfo.getVectorSimilarityFunction();
            this.entry = entry;
            this.fieldInfo = fieldInfo;
            this.acceptDocs = acceptDocs;
            quantizedVectorByteSize = quantEncoding.getDocPackedLength(fieldInfo.getVectorDimension());
            quantizedByteLength = quantizedVectorByteSize + (Float.BYTES * 3) + Integer.BYTES;
            osqVectorsScorer = ESVectorUtil.getES940OSQVectorsScorer(
                indexInput,
                quantEncoding.queryBits(),
                quantEncoding.bits(),
                fieldInfo.getVectorDimension(),
                (int) quantizedVectorByteSize,
                BULK_SIZE,
                quantEncoding.bits() == 2 || quantEncoding.bits() == 4
                    ? ES940OSQVectorsScorer.BitEncoding.PACKED
                    : ES940OSQVectorsScorer.BitEncoding.STRIPED
            );
        }

        @Override
        public int resetPostingsScorer(PostingMetadata metadata) throws IOException {
            float score = metadata.documentCentroidScore();
            indexInput.seek(metadata.offset());
            centroidToParentSqDist = Float.intBitsToFloat(indexInput.readInt());
            vectors = indexInput.readVInt();
            docEncoding = indexInput.readByte();
            docBase = 0;
            slicePos = indexInput.getFilePointer();
            // The score is the transformed score used when searching the centroids.
            // we need to convert it back to the raw similarity to be used as part of
            // final corrections
            centroidDistance = switch (similarityFunction) {
                case EUCLIDEAN -> ((1 / score) - 1) - centroidToParentSqDist;
                case COSINE, DOT_PRODUCT -> 2 * score - 1;
                case MAXIMUM_INNER_PRODUCT -> score - 1;
            };
            queryQuantizer.reset(metadata.queryCentroidOrdinal());
            return vectors;
        }

        private float scoreIndividually(int bulkSize) throws IOException {
            float maxScore = Float.NEGATIVE_INFINITY;
            // score individually, first the quantized byte chunk
            for (int j = 0; j < bulkSize; j++) {
                int doc = docIdsScratch[j];
                if (doc != -1) {
                    float qcDist = osqVectorsScorer.quantizeScore(queryQuantizer.getQuantizedTarget());
                    scores[j] = qcDist;
                } else {
                    indexInput.skipBytes(quantizedVectorByteSize);
                }
            }
            // read in all corrections
            indexInput.readFloats(correctionsLower, 0, bulkSize);
            indexInput.readFloats(correctionsUpper, 0, bulkSize);
            for (int j = 0; j < bulkSize; j++) {
                correctionsSum[j] = indexInput.readInt();
            }
            indexInput.readFloats(correctionsAdd, 0, bulkSize);
            // Now apply corrections
            for (int j = 0; j < bulkSize; j++) {
                int doc = docIdsScratch[j];
                if (doc != -1) {
                    scores[j] = osqVectorsScorer.applyCorrectionsIndividually(
                        queryQuantizer.getQueryCorrections().lowerInterval(),
                        queryQuantizer.getQueryCorrections().upperInterval(),
                        queryQuantizer.getQueryCorrections().quantizedComponentSum(),
                        centroidDistance,
                        fieldInfo.getVectorSimilarityFunction(),
                        0,
                        correctionsLower[j],
                        correctionsUpper[j],
                        correctionsSum[j],
                        correctionsAdd[j],
                        scores[j]
                    );
                    if (scores[j] > maxScore) {
                        maxScore = scores[j];
                    }
                }
            }
            return maxScore;
        }

        protected int docToBulkScore(int[] docIds, int[] offsets, Bits acceptDocs, int bulkSize) {
            if (acceptDocs == null) {
                return bulkSize;
            }
            int docToScore = 0;
            for (int i = 0; i < bulkSize; i++) {
                if (docIds[i] == -1 || acceptDocs.get(docIds[i]) == false) {
                    docIds[i] = -1;
                } else {
                    offsets[docToScore] = i;
                    docToScore++;
                }
            }
            return docToScore;
        }

        protected void collectBulk(KnnCollector knnCollector, float[] scores, int bulkSize, int docsToBulkScore, float maxScore) {
            if (knnCollector instanceof BulkKnnCollector bulkCollector) {
                if (docsToBulkScore == bulkSize) {
                    bulkCollector.bulkCollect(docIdsScratch, scores, bulkSize, maxScore);
                    return;
                }
                for (int i = 0; i < docsToBulkScore; i++) {
                    int offset = offsetsScratch[i];
                    docIdsScratch[i] = docIdsScratch[offset];
                    scores[i] = scores[offset];
                }
                bulkCollector.bulkCollect(docIdsScratch, scores, docsToBulkScore, maxScore);
                return;
            }
            for (int i = 0; i < bulkSize; i++) {
                final int doc = docIdsScratch[i];
                if (doc != -1) {
                    knnCollector.collect(doc, scores[i]);
                }
            }
        }

        protected void readDocIds(int count) throws IOException {
            idsWriter.readInts(indexInput, count, docEncoding, docIdsScratch);
            // reconstitute from the deltas
            for (int j = 0; j < count; j++) {
                docBase += docIdsScratch[j];
                docIdsScratch[j] = docBase;
            }
        }

        @Override
        public int visit(KnnCollector knnCollector) throws IOException {
            indexInput.seek(slicePos);
            // block processing
            int scoredDocs = 0;
            int limit = vectors - BULK_SIZE + 1;
            int i = 0;
            // read Docs
            for (; i < limit; i += BULK_SIZE) {
                // read the doc ids
                readDocIds(BULK_SIZE);
                final int docsToBulkScore = docToBulkScore(docIdsScratch, offsetsScratch, acceptDocs, BULK_SIZE);
                if (docsToBulkScore == 0) {
                    indexInput.skipBytes(quantizedByteLength * BULK_SIZE);
                    continue;
                }
                queryQuantizer.quantizeQueryIfNecessary();
                final float maxScore;
                if (docsToBulkScore == 1) {
                    maxScore = scoreIndividually(BULK_SIZE);
                } else if (docsToBulkScore < BULK_SIZE) {
                    maxScore = osqVectorsScorer.scoreBulkOffsets(
                        queryQuantizer.getQuantizedTarget(),
                        queryQuantizer.getQueryCorrections().lowerInterval(),
                        queryQuantizer.getQueryCorrections().upperInterval(),
                        queryQuantizer.getQueryCorrections().quantizedComponentSum(),
                        centroidDistance,
                        fieldInfo.getVectorSimilarityFunction(),
                        0f,
                        offsetsScratch,
                        docsToBulkScore,
                        scores,
                        BULK_SIZE
                    );
                } else {
                    maxScore = osqVectorsScorer.scoreBulk(
                        queryQuantizer.getQuantizedTarget(),
                        queryQuantizer.getQueryCorrections().lowerInterval(),
                        queryQuantizer.getQueryCorrections().upperInterval(),
                        queryQuantizer.getQueryCorrections().quantizedComponentSum(),
                        centroidDistance,
                        fieldInfo.getVectorSimilarityFunction(),
                        0f,
                        scores
                    );
                }
                if (knnCollector.minCompetitiveSimilarity() < maxScore) {
                    collectBulk(knnCollector, scores, BULK_SIZE, docsToBulkScore, maxScore);
                }
                scoredDocs += docsToBulkScore;
            }
            // bulk process tail
            if (i < vectors) {
                int tailSize = vectors - i;
                readDocIds(tailSize);
                final int docsToBulkScore = docToBulkScore(docIdsScratch, offsetsScratch, acceptDocs, tailSize);
                if (docsToBulkScore == 0) {
                    indexInput.skipBytes(quantizedByteLength * tailSize);
                } else {
                    queryQuantizer.quantizeQueryIfNecessary();
                    final float maxScore;
                    if (docsToBulkScore == 1) {
                        maxScore = scoreIndividually(tailSize);
                    } else if (docsToBulkScore < tailSize) {
                        maxScore = osqVectorsScorer.scoreBulkOffsets(
                            queryQuantizer.getQuantizedTarget(),
                            queryQuantizer.getQueryCorrections().lowerInterval(),
                            queryQuantizer.getQueryCorrections().upperInterval(),
                            queryQuantizer.getQueryCorrections().quantizedComponentSum(),
                            centroidDistance,
                            fieldInfo.getVectorSimilarityFunction(),
                            0f,
                            offsetsScratch,
                            docsToBulkScore,
                            scores,
                            tailSize
                        );
                    } else {
                        maxScore = osqVectorsScorer.scoreBulk(
                            queryQuantizer.getQuantizedTarget(),
                            queryQuantizer.getQueryCorrections().lowerInterval(),
                            queryQuantizer.getQueryCorrections().upperInterval(),
                            queryQuantizer.getQueryCorrections().quantizedComponentSum(),
                            centroidDistance,
                            fieldInfo.getVectorSimilarityFunction(),
                            0f,
                            scores,
                            tailSize
                        );
                    }
                    if (knnCollector.minCompetitiveSimilarity() < maxScore) {
                        collectBulk(knnCollector, scores, tailSize, docsToBulkScore, maxScore);
                    }
                    scoredDocs += docsToBulkScore;
                }
            }
            if (scoredDocs > 0) {
                knnCollector.incVisitedCount(scoredDocs);
            }
            return scoredDocs;
        }
    }

}
