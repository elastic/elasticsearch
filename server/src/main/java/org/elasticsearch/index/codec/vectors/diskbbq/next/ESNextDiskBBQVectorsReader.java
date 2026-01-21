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
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;
import org.elasticsearch.index.codec.vectors.GenericFlatVectorReaders;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.cluster.NeighborQueue;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIterator;
import org.elasticsearch.index.codec.vectors.diskbbq.DocIdsWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader;
import org.elasticsearch.index.codec.vectors.diskbbq.PostingMetadata;
import org.elasticsearch.index.codec.vectors.diskbbq.PrefetchingCentroidIterator;
import org.elasticsearch.simdvec.ES92Int7VectorsScorer;
import org.elasticsearch.simdvec.ESNextOSQVectorsScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer.DEFAULT_LAMBDA;
import static org.elasticsearch.simdvec.ESNextOSQVectorsScorer.BULK_SIZE;

/**
 * Default implementation of {@link IVFVectorsReader}. It scores the posting lists centroids using
 * brute force and then scores the top ones using the posting list.
 */
public class ESNextDiskBBQVectorsReader extends IVFVectorsReader {

    public ESNextDiskBBQVectorsReader(SegmentReadState state, GenericFlatVectorReaders.LoadFlatVectorsReader getFormatReader)
        throws IOException {
        super(state, getFormatReader);
    }

    CentroidIterator getPostingListPrefetchIterator(CentroidIterator centroidIterator, IndexInput postingListSlice) throws IOException {
        // TODO we may want to prefetch more than one postings list, however, we will likely want to place a limit
        // so we don't bother prefetching many lists we won't end up scoring
        return new PrefetchingCentroidIterator(centroidIterator, postingListSlice);
    }

    static long directWriterSizeOnDisk(long numValues, int bitsPerValue) {
        // TODO: use method in https://github.com/apache/lucene/pull/15422 when/if merged.
        long bytes = (numValues * bitsPerValue + Byte.SIZE - 1) / 8;
        int paddingBitsNeeded;
        if (bitsPerValue > Integer.SIZE) {
            paddingBitsNeeded = Long.SIZE - bitsPerValue;
        } else if (bitsPerValue > Short.SIZE) {
            paddingBitsNeeded = Integer.SIZE - bitsPerValue;
        } else if (bitsPerValue > Byte.SIZE) {
            paddingBitsNeeded = Short.SIZE - bitsPerValue;
        } else {
            paddingBitsNeeded = 0;
        }
        final int paddingBytesNeeded = (paddingBitsNeeded + Byte.SIZE - 1) / Byte.SIZE;
        return bytes + paddingBytesNeeded;
    }

    @Override
    public CentroidIterator getCentroidIterator(
        FieldInfo fieldInfo,
        int numCentroids,
        IndexInput centroids,
        float[] targetQuery,
        IndexInput postingListSlice,
        AcceptDocs acceptDocs,
        float approximateCost,
        FloatVectorValues values,
        float visitRatio
    ) throws IOException {
        final FieldEntry fieldEntry = fields.get(fieldInfo.number);
        int bulkSize = fieldEntry.getBulkSize();
        float approximateDocsPerCentroid = approximateCost / numCentroids;
        if (approximateDocsPerCentroid <= 1.25) {
            // TODO: we need to make this call to build the iterator, otherwise accept docs breaks all together
            approximateDocsPerCentroid = (float) acceptDocs.cost() / numCentroids;
        }
        final int bitsRequired = DirectWriter.bitsRequired(numCentroids);
        final long sizeLookup = directWriterSizeOnDisk(values.size(), bitsRequired);
        final long fp = centroids.getFilePointer();
        final FixedBitSet acceptCentroids;
        if (approximateDocsPerCentroid > 1.25 || numCentroids == 1) {
            // only apply centroid filtering when we expect some / many centroids will not have
            // any matching document.
            acceptCentroids = null;
        } else {
            acceptCentroids = new FixedBitSet(numCentroids);
            final KnnVectorValues.DocIndexIterator docIndexIterator = values.iterator();
            final DocIdSetIterator iterator = ConjunctionUtils.intersectIterators(List.of(acceptDocs.iterator(), docIndexIterator));
            final LongValues longValues = DirectReader.getInstance(centroids.randomAccessSlice(fp, sizeLookup), bitsRequired);
            int doc = iterator.nextDoc();
            for (; doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                acceptCentroids.set((int) longValues.get(docIndexIterator.index()));
            }
        }
        final OptimizedScalarQuantizer scalarQuantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        final int[] scratch = new int[targetQuery.length];
        final OptimizedScalarQuantizer.QuantizationResult queryParams = scalarQuantizer.scalarQuantize(
            targetQuery,
            new float[targetQuery.length],
            scratch,
            (byte) 7,
            fieldEntry.globalCentroid()
        );
        final byte[] quantized = new byte[targetQuery.length];
        for (int i = 0; i < quantized.length; i++) {
            quantized[i] = (byte) scratch[i];
        }
        final ES92Int7VectorsScorer scorer = ESVectorUtil.getES92Int7VectorsScorer(centroids, fieldInfo.getVectorDimension(), bulkSize);
        centroids.seek(fp + sizeLookup);
        int numParents = centroids.readVInt();

        CentroidIterator centroidIterator;
        if (numParents > 0) {
            // equivalent to (float) centroidsPerParentCluster / 2
            float centroidOversampling = (float) fieldEntry.numCentroids() / (2 * numParents);
            centroidIterator = getCentroidIteratorWithParents(
                fieldInfo,
                centroids,
                numParents,
                numCentroids,
                scorer,
                quantized,
                queryParams,
                fieldEntry.globalCentroidDp(),
                visitRatio * centroidOversampling,
                acceptCentroids,
                bulkSize
            );
        } else {
            centroidIterator = getCentroidIteratorNoParent(
                fieldInfo,
                centroids,
                numCentroids,
                scorer,
                quantized,
                queryParams,
                fieldEntry.globalCentroidDp(),
                acceptCentroids,
                bulkSize
            );
        }
        return getPostingListPrefetchIterator(centroidIterator, postingListSlice);
    }

    @Override
    protected FieldEntry doReadField(
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
        ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding = ESNextDiskBBQVectorsFormat.QuantEncoding.fromId(input.readInt());
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
            quantEncoding,
            bulkSize
        );
    }

    static class NextFieldEntry extends FieldEntry {
        private final ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding;

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
            ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding,
            int bulkSize
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
            this.quantEncoding = quantEncoding;
        }

        public ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding() {
            return quantEncoding;
        }
    }

    private static CentroidIterator getCentroidIteratorNoParent(
        FieldInfo fieldInfo,
        IndexInput centroids,
        int numCentroids,
        ES92Int7VectorsScorer scorer,
        byte[] quantizeQuery,
        OptimizedScalarQuantizer.QuantizationResult queryParams,
        float globalCentroidDp,
        FixedBitSet acceptCentroids,
        int bulkSize
    ) throws IOException {
        final NeighborQueue neighborQueue = new NeighborQueue(numCentroids, true);
        final long centroidQuantizeSize = fieldInfo.getVectorDimension() + 3 * Float.BYTES + Integer.BYTES;
        score(
            neighborQueue,
            numCentroids,
            0,
            scorer,
            centroids,
            centroidQuantizeSize,
            quantizeQuery,
            queryParams,
            globalCentroidDp,
            fieldInfo.getVectorSimilarityFunction(),
            new float[bulkSize],
            acceptCentroids,
            bulkSize
        );
        long offset = centroids.getFilePointer();
        return new CentroidIterator() {
            @Override
            public boolean hasNext() {
                return neighborQueue.size() > 0;
            }

            @Override
            public PostingMetadata nextPosting() throws IOException {
                long centroidOrdinalAndScore = neighborQueue.popRaw();
                int centroidOrd = neighborQueue.decodeNodeId(centroidOrdinalAndScore);
                float score = neighborQueue.decodeScore(centroidOrdinalAndScore);
                centroids.seek(offset + (long) Long.BYTES * 2 * centroidOrd);
                long postingListOffset = centroids.readLong();
                long postingListLength = centroids.readLong();
                return new PostingMetadata(postingListOffset, postingListLength, centroidOrd, score);
            }
        };
    }

    private static CentroidIterator getCentroidIteratorWithParents(
        FieldInfo fieldInfo,
        IndexInput centroids,
        int numParents,
        int numCentroids,
        ES92Int7VectorsScorer scorer,
        byte[] quantizeQuery,
        OptimizedScalarQuantizer.QuantizationResult queryParams,
        float globalCentroidDp,
        float centroidRatio,
        FixedBitSet acceptCentroids,
        int bulkSize
    ) throws IOException {
        // build the three queues we are going to use
        final long rawParentSize = (long) fieldInfo.getVectorDimension() * Float.BYTES;
        final long centroidQuantizeSize = fieldInfo.getVectorDimension() + 3 * Float.BYTES + Integer.BYTES;
        final NeighborQueue parentsQueue = new NeighborQueue(numParents, true);
        final int maxChildrenSize = centroids.readVInt();
        final NeighborQueue currentParentQueue = new NeighborQueue(maxChildrenSize, true);
        final int bufferSize = (int) Math.min(Math.max(centroidRatio * numCentroids, 1), numCentroids);
        final int numCentroidsFiltered = acceptCentroids == null ? numCentroids : acceptCentroids.cardinality();
        if (numCentroidsFiltered == 0) {
            // TODO maybe this makes CentroidIterator polymorphic?
            return new CentroidIterator() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public PostingMetadata nextPosting() {
                    return null;
                }
            };
        }
        final float[] scores = new float[bulkSize];
        final NeighborQueue neighborQueue;
        if (acceptCentroids != null && numCentroidsFiltered <= bufferSize) {
            // we are collecting every non-filter centroid, therefore we do not need to score the
            // parents. We give each of them the same score.
            neighborQueue = new NeighborQueue(numCentroidsFiltered, true);
            for (int i = 0; i < numParents; i++) {
                parentsQueue.add(i, 0.5f);
            }
            centroids.skipBytes((centroidQuantizeSize + rawParentSize) * numParents);
        } else {
            neighborQueue = new NeighborQueue(bufferSize, true);
            // score the parents
            centroids.skipBytes(rawParentSize * numParents);
            score(
                parentsQueue,
                numParents,
                0,
                scorer,
                centroids,
                centroidQuantizeSize,
                quantizeQuery,
                queryParams,
                globalCentroidDp,
                fieldInfo.getVectorSimilarityFunction(),
                scores,
                null,
                bulkSize
            );
        }

        final long offset = centroids.getFilePointer();
        final long childrenOffset = offset + (long) Long.BYTES * numParents;
        // populate the children's queue by reading parents one by one
        while (parentsQueue.size() > 0 && neighborQueue.size() < bufferSize) {
            final int pop = parentsQueue.pop();
            populateOneChildrenGroup(
                currentParentQueue,
                centroids,
                offset + 2L * Integer.BYTES * pop,
                childrenOffset,
                centroidQuantizeSize,
                fieldInfo,
                scorer,
                quantizeQuery,
                queryParams,
                globalCentroidDp,
                scores,
                acceptCentroids,
                bulkSize
            );
            while (currentParentQueue.size() > 0 && neighborQueue.size() < bufferSize) {
                final float score = currentParentQueue.topScore();
                final int children = currentParentQueue.pop();
                neighborQueue.add(children, score);
            }
        }
        final long childrenFileOffsets = childrenOffset + centroidQuantizeSize * numCentroids;
        return new CentroidIterator() {

            @Override
            public boolean hasNext() {
                return neighborQueue.size() > 0;
            }

            @Override
            public PostingMetadata nextPosting() throws IOException {
                long centroidOrdinalAndScore = nextCentroid();
                int centroidOrdinal = neighborQueue.decodeNodeId(centroidOrdinalAndScore);
                float score = neighborQueue.decodeScore(centroidOrdinalAndScore);
                centroids.seek(childrenFileOffsets + (long) (Long.BYTES * 2 + Integer.BYTES) * centroidOrdinal);
                long postingListOffset = centroids.readLong();
                long postingListLength = centroids.readLong();
                int parentOrd = centroids.readInt();
                return new PostingMetadata(postingListOffset, postingListLength, parentOrd, score);
            }

            private long nextCentroid() throws IOException {
                if (currentParentQueue.size() > 0) {
                    // return next centroid and maybe add a children from the current parent queue
                    return neighborQueue.popRawAndAddRaw(currentParentQueue.popRaw());
                } else if (parentsQueue.size() > 0) {
                    // current parent queue is empty, populate it again with the next parent
                    int pop = parentsQueue.pop();
                    populateOneChildrenGroup(
                        currentParentQueue,
                        centroids,
                        offset + 2L * Integer.BYTES * pop,
                        childrenOffset,
                        centroidQuantizeSize,
                        fieldInfo,
                        scorer,
                        quantizeQuery,
                        queryParams,
                        globalCentroidDp,
                        scores,
                        acceptCentroids,
                        bulkSize
                    );
                    return nextCentroid();
                } else {
                    return neighborQueue.popRaw();
                }
            }
        };
    }

    private static void populateOneChildrenGroup(
        NeighborQueue neighborQueue,
        IndexInput centroids,
        long parentOffset,
        long childrenOffset,
        long centroidQuantizeSize,
        FieldInfo fieldInfo,
        ES92Int7VectorsScorer scorer,
        byte[] quantizeQuery,
        OptimizedScalarQuantizer.QuantizationResult queryParams,
        float globalCentroidDp,
        float[] scores,
        FixedBitSet acceptCentroids,
        int bulkSize
    ) throws IOException {
        centroids.seek(parentOffset);
        int childrenOrdinal = centroids.readInt();
        int numChildren = centroids.readInt();
        centroids.seek(childrenOffset + centroidQuantizeSize * childrenOrdinal);
        score(
            neighborQueue,
            numChildren,
            childrenOrdinal,
            scorer,
            centroids,
            centroidQuantizeSize,
            quantizeQuery,
            queryParams,
            globalCentroidDp,
            fieldInfo.getVectorSimilarityFunction(),
            scores,
            acceptCentroids,
            bulkSize
        );
    }

    private static void score(
        NeighborQueue neighborQueue,
        int size,
        int scoresOffset,
        ES92Int7VectorsScorer scorer,
        IndexInput centroids,
        long centroidQuantizeSize,
        byte[] quantizeQuery,
        OptimizedScalarQuantizer.QuantizationResult queryCorrections,
        float centroidDp,
        VectorSimilarityFunction similarityFunction,
        float[] scores,
        FixedBitSet acceptCentroids,
        int bulkSize
    ) throws IOException {
        int limit = size - bulkSize + 1;
        int i = 0;
        for (; i < limit; i += bulkSize) {
            if (acceptCentroids == null || acceptCentroids.cardinality(scoresOffset + i, scoresOffset + i + bulkSize) > 0) {
                scorer.scoreBulk(
                    quantizeQuery,
                    queryCorrections.lowerInterval(),
                    queryCorrections.upperInterval(),
                    queryCorrections.quantizedComponentSum(),
                    queryCorrections.additionalCorrection(),
                    similarityFunction,
                    centroidDp,
                    scores,
                    bulkSize
                );
                for (int j = 0; j < bulkSize; j++) {
                    int centroidOrd = scoresOffset + i + j;
                    if (acceptCentroids == null || acceptCentroids.get(centroidOrd)) {
                        neighborQueue.add(centroidOrd, scores[j]);
                    }
                }
            } else {
                centroids.skipBytes(bulkSize * centroidQuantizeSize);
            }
        }

        int tailBulkSize = size - i;
        if (tailBulkSize > 0) {
            if (acceptCentroids == null || acceptCentroids.cardinality(scoresOffset + i, scoresOffset + i + tailBulkSize) > 0) {
                scorer.scoreBulk(
                    quantizeQuery,
                    queryCorrections.lowerInterval(),
                    queryCorrections.upperInterval(),
                    queryCorrections.quantizedComponentSum(),
                    queryCorrections.additionalCorrection(),
                    similarityFunction,
                    centroidDp,
                    scores,
                    tailBulkSize
                );
                for (int j = 0; j < tailBulkSize; j++) {
                    int centroidOrd = scoresOffset + i + j;
                    if (acceptCentroids == null || acceptCentroids.get(centroidOrd)) {
                        neighborQueue.add(centroidOrd, scores[j]);
                    }
                }
            } else {
                centroids.skipBytes(tailBulkSize * centroidQuantizeSize);
            }
        }

    }

    @Override
    public PostingVisitor getPostingVisitor(
        FieldInfo fieldInfo,
        IndexInput indexInput,
        float[] target,
        Bits acceptDocs,
        IndexInput centroidSlice
    ) throws IOException {
        FieldEntry entry = fields.get(fieldInfo.number);
        ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding = ((NextFieldEntry) entry).quantEncoding();
        int numParents = centroidSlice.readVInt();
        final QueryQuantizer queryQuantizer;
        if (numParents > 0) {
            // unusd
            int longestPostingList = centroidSlice.readVInt();
            IndexInput parentsSlice = centroidSlice.slice(
                "parents-slice",
                centroidSlice.getFilePointer(),
                (long) numParents * fieldInfo.getVectorDimension() * Float.BYTES
            );
            queryQuantizer = new QueryQuantizer(quantEncoding, fieldInfo, target, parentsSlice, entry.globalCentroid());
        } else {
            queryQuantizer = new QueryQuantizer(quantEncoding, fieldInfo, target, null, entry.globalCentroid());
        }

        return new MemorySegmentPostingsVisitor(queryQuantizer, quantEncoding, indexInput, entry, fieldInfo, acceptDocs);
    }

    private static class QueryQuantizer {
        private OptimizedScalarQuantizer.QuantizationResult queryCorrections;
        private final ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding;
        private final float[] target;
        private final float[] scratch;
        private final int[] quantizationScratch;
        private final byte[] quantizedQueryScratch;
        private final OptimizedScalarQuantizer quantizer;
        private final IndexInput parentsSlice;
        private final float[] globalCentroid;
        private final float[] centroidScratch;
        private int currentCentroidOrdinal = -2;

        QueryQuantizer(
            ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding,
            FieldInfo fieldInfo,
            float[] target,
            IndexInput parentsSlice,
            float[] globalCentroid
        ) {
            this.quantEncoding = quantEncoding;
            this.target = target;
            this.scratch = new float[fieldInfo.getVectorDimension()];
            this.centroidScratch = new float[fieldInfo.getVectorDimension()];
            this.quantizationScratch = new int[quantEncoding.discretizedDimensions(fieldInfo.getVectorDimension())];
            this.quantizedQueryScratch = new byte[quantEncoding.getQueryPackedLength(fieldInfo.getVectorDimension())];
            this.quantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction(), DEFAULT_LAMBDA, 1);
            this.parentsSlice = parentsSlice;
            this.globalCentroid = globalCentroid;
        }

        void quantizeQueryIfNecessary(int centroidOrdinal) throws IOException {
            if (centroidOrdinal != currentCentroidOrdinal) {
                final float[] queryCentroid;
                if (parentsSlice != null) {
                    parentsSlice.seek((long) centroidOrdinal * centroidScratch.length * Float.BYTES);
                    parentsSlice.readFloats(centroidScratch, 0, centroidScratch.length);
                    queryCentroid = centroidScratch;
                } else {
                    queryCentroid = globalCentroid;
                }
                queryCorrections = quantizer.scalarQuantize(target, scratch, quantizationScratch, quantEncoding.queryBits(), queryCentroid);
                quantEncoding.packQuery(quantizationScratch, quantizedQueryScratch);
                currentCentroidOrdinal = centroidOrdinal;
            }
        }

        OptimizedScalarQuantizer.QuantizationResult getQueryCorrections() {
            return queryCorrections;
        }

        byte[] getQuantizedTarget() {
            return quantizedQueryScratch;
        }
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
        // TODO: override if adding new files
        return super.getOffHeapByteSize(fieldInfo);
    }

    private static class MemorySegmentPostingsVisitor implements PostingVisitor {
        final long quantizedByteLength;
        final IndexInput indexInput;
        final FieldEntry entry;
        final FieldInfo fieldInfo;
        final Bits acceptDocs;
        final QueryQuantizer queryQuantizer;
        private final ESNextOSQVectorsScorer osqVectorsScorer;
        final float[] scores = new float[BULK_SIZE];
        final float[] correctionsLower = new float[BULK_SIZE];
        final float[] correctionsUpper = new float[BULK_SIZE];
        final int[] correctionsSum = new int[BULK_SIZE];
        final float[] correctionsAdd = new float[BULK_SIZE];
        final int[] docIdsScratch = new int[BULK_SIZE];
        byte docEncoding;
        int docBase = 0;

        int vectors;
        float centroidDp;
        float centroidDistance;
        long slicePos;
        int centroidOrd;

        final DocIdsWriter idsWriter = new DocIdsWriter();
        final VectorSimilarityFunction similarityFunction;
        final float[] correctiveValues = new float[3];
        final long quantizedVectorByteSize;

        MemorySegmentPostingsVisitor(
            QueryQuantizer queryQuantizer,
            ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding,
            IndexInput indexInput,
            FieldEntry entry,
            FieldInfo fieldInfo,
            Bits acceptDocs
        ) throws IOException {
            this.indexInput = indexInput;
            this.similarityFunction = fieldInfo.getVectorSimilarityFunction();
            this.entry = entry;
            this.fieldInfo = fieldInfo;
            this.acceptDocs = acceptDocs;
            this.queryQuantizer = queryQuantizer;
            this.quantizedVectorByteSize = quantEncoding.getDocPackedLength(fieldInfo.getVectorDimension());
            this.quantizedByteLength = quantizedVectorByteSize + (Float.BYTES * 3) + Short.BYTES;
            this.osqVectorsScorer = ESVectorUtil.getESNextOSQVectorsScorer(
                indexInput,
                quantEncoding.queryBits(),
                quantEncoding.bits(),
                fieldInfo.getVectorDimension(),
                (int) quantizedVectorByteSize,
                BULK_SIZE
            );
        }

        @Override
        public int resetPostingsScorer(PostingMetadata metadata) throws IOException {
            float score = metadata.centroidScore();
            indexInput.seek(metadata.offset());
            centroidDp = Float.intBitsToFloat(indexInput.readInt());
            vectors = indexInput.readVInt();
            docEncoding = indexInput.readByte();
            docBase = 0;
            slicePos = indexInput.getFilePointer();
            centroidDistance = similarityFunction == EUCLIDEAN ? ((1 / score) - 1) - centroidDp : score - 1;
            centroidOrd = metadata.centroidOrdinal();
            return vectors;
        }

        private float scoreIndividually() throws IOException {
            float maxScore = Float.NEGATIVE_INFINITY;
            // score individually, first the quantized byte chunk
            for (int j = 0; j < BULK_SIZE; j++) {
                int doc = docIdsScratch[j];
                if (doc != -1) {
                    float qcDist = osqVectorsScorer.quantizeScore(queryQuantizer.getQuantizedTarget());
                    scores[j] = qcDist;
                } else {
                    indexInput.skipBytes(quantizedVectorByteSize);
                }
            }
            // read in all corrections
            indexInput.readFloats(correctionsLower, 0, BULK_SIZE);
            indexInput.readFloats(correctionsUpper, 0, BULK_SIZE);
            for (int j = 0; j < BULK_SIZE; j++) {
                correctionsSum[j] = Short.toUnsignedInt(indexInput.readShort());
            }
            indexInput.readFloats(correctionsAdd, 0, BULK_SIZE);
            // Now apply corrections
            float lower = queryQuantizer.getQueryCorrections().lowerInterval();
            float upper = queryQuantizer.getQueryCorrections().upperInterval();
            int quantizedComponentSum = queryQuantizer.getQueryCorrections().quantizedComponentSum();
            for (int j = 0; j < BULK_SIZE; j++) {
                int doc = docIdsScratch[j];
                if (doc != -1) {
                    scores[j] = osqVectorsScorer.score(
                        lower,
                        upper,
                        quantizedComponentSum,
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

        private static int docToBulkScore(int[] docIds, Bits acceptDocs) {
            assert acceptDocs != null : "acceptDocs must not be null";
            int docToScore = BULK_SIZE;
            for (int i = 0; i < BULK_SIZE; i++) {
                if (acceptDocs.get(docIds[i]) == false) {
                    docIds[i] = -1;
                    docToScore--;
                }
            }
            return docToScore;
        }

        private void collectBulk(KnnCollector knnCollector, float[] scores) {
            for (int i = 0; i < BULK_SIZE; i++) {
                final int doc = docIdsScratch[i];
                if (doc != -1) {
                    knnCollector.collect(doc, scores[i]);
                }
            }
        }

        private void readDocIds(int count) throws IOException {
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
                final int docsToBulkScore = acceptDocs == null ? BULK_SIZE : docToBulkScore(docIdsScratch, acceptDocs);
                if (docsToBulkScore == 0) {
                    indexInput.skipBytes(quantizedByteLength * BULK_SIZE);
                    continue;
                }
                queryQuantizer.quantizeQueryIfNecessary(centroidOrd);
                final float maxScore;
                if (docsToBulkScore < BULK_SIZE / 2) {
                    maxScore = scoreIndividually();
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
                    collectBulk(knnCollector, scores);
                }
                scoredDocs += docsToBulkScore;
            }
            // process tail
            // read the doc ids
            if (i < vectors) {
                readDocIds(vectors - i);
            }
            int count = 0;
            for (; i < vectors; i++) {
                int doc = docIdsScratch[count++];
                if (acceptDocs == null || acceptDocs.get(doc)) {
                    queryQuantizer.quantizeQueryIfNecessary(centroidOrd);
                    float qcDist = osqVectorsScorer.quantizeScore(queryQuantizer.getQuantizedTarget());
                    indexInput.readFloats(correctiveValues, 0, 3);
                    final int quantizedComponentSum = Short.toUnsignedInt(indexInput.readShort());
                    float score = osqVectorsScorer.score(
                        queryQuantizer.getQueryCorrections().lowerInterval(),
                        queryQuantizer.getQueryCorrections().upperInterval(),
                        queryQuantizer.getQueryCorrections().quantizedComponentSum(),
                        centroidDistance,
                        fieldInfo.getVectorSimilarityFunction(),
                        0f,
                        correctiveValues[0],
                        correctiveValues[1],
                        quantizedComponentSum,
                        correctiveValues[2],
                        qcDist
                    );
                    scoredDocs++;
                    knnCollector.collect(doc, score);
                } else {
                    indexInput.skipBytes(quantizedByteLength);
                }
            }
            if (scoredDocs > 0) {
                knnCollector.incVisitedCount(scoredDocs);
            }
            return scoredDocs;
        }
    }

}
