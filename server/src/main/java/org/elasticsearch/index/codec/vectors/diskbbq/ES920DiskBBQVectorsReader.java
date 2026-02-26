/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.codec.vectors.GenericFlatVectorReaders;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.cluster.NeighborQueue;
import org.elasticsearch.simdvec.ES91OSQVectorsScorer;
import org.elasticsearch.simdvec.ES92Int7VectorsScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;

import static org.apache.lucene.codecs.lucene102.Lucene102BinaryQuantizedVectorsFormat.QUERY_BITS;
import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.elasticsearch.index.codec.vectors.BQVectorUtils.discretize;
import static org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer.DEFAULT_LAMBDA;
import static org.elasticsearch.index.codec.vectors.diskbbq.ES920DiskBBQVectorsFormat.BULK_SIZE;
import static org.elasticsearch.simdvec.ESVectorUtil.transposeHalfByte;

/**
 * Default implementation of {@link IVFVectorsReader}. It scores the posting lists centroids using
 * brute force and then scores the top ones using the posting list.
 */
public class ES920DiskBBQVectorsReader extends IVFVectorsReader {

    ES920DiskBBQVectorsReader(SegmentReadState state, GenericFlatVectorReaders.LoadFlatVectorsReader getFormatReader) throws IOException {
        super(state, getFormatReader);
    }

    public CentroidIterator getPostingListPrefetchIterator(CentroidIterator centroidIterator, IndexInput postingListSlice)
        throws IOException {
        return new PrefetchingCentroidIterator(centroidIterator, postingListSlice);
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
        final float globalCentroidDp = fieldEntry.globalCentroidDp();
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
        final ES92Int7VectorsScorer scorer = ESVectorUtil.getES92Int7VectorsScorer(centroids, fieldInfo.getVectorDimension(), BULK_SIZE);
        centroids.seek(0L);
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
                globalCentroidDp,
                visitRatio * centroidOversampling
            );
        } else {
            centroidIterator = getCentroidIteratorNoParent(
                fieldInfo,
                centroids,
                numCentroids,
                scorer,
                quantized,
                queryParams,
                globalCentroidDp
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
    ) {
        // nothing more to read
        return new FieldEntry(
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
            BULK_SIZE
        );
    }

    private static CentroidIterator getCentroidIteratorNoParent(
        FieldInfo fieldInfo,
        IndexInput centroids,
        int numCentroids,
        ES92Int7VectorsScorer scorer,
        byte[] quantizeQuery,
        OptimizedScalarQuantizer.QuantizationResult queryParams,
        float globalCentroidDp
    ) throws IOException {
        final NeighborQueue neighborQueue = new NeighborQueue(numCentroids, true);
        score(
            neighborQueue,
            numCentroids,
            0,
            scorer,
            quantizeQuery,
            queryParams,
            globalCentroidDp,
            fieldInfo.getVectorSimilarityFunction(),
            new float[BULK_SIZE]
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
                int centroidOrdinal = neighborQueue.decodeNodeId(centroidOrdinalAndScore);
                float score = neighborQueue.decodeScore(centroidOrdinalAndScore);
                centroids.seek(offset + (long) Long.BYTES * 2 * centroidOrdinal);
                long postingListOffset = centroids.readLong();
                long postingListLength = centroids.readLong();
                return new PostingMetadata(postingListOffset, postingListLength, centroidOrdinal, score);
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
        float centroidRatio
    ) throws IOException {
        // build the three queues we are going to use
        final NeighborQueue parentsQueue = new NeighborQueue(numParents, true);
        final int maxChildrenSize = centroids.readVInt();
        final NeighborQueue currentParentQueue = new NeighborQueue(maxChildrenSize, true);
        final int bufferSize = (int) Math.min(Math.max(centroidRatio * numCentroids, 1), numCentroids);
        final NeighborQueue neighborQueue = new NeighborQueue(bufferSize, true);
        // score the parents
        final float[] scores = new float[BULK_SIZE];
        score(
            parentsQueue,
            numParents,
            0,
            scorer,
            quantizeQuery,
            queryParams,
            globalCentroidDp,
            fieldInfo.getVectorSimilarityFunction(),
            scores
        );
        final long centroidQuantizeSize = fieldInfo.getVectorDimension() + 3 * Float.BYTES + Integer.BYTES;
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
                scores
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
                centroids.seek(childrenFileOffsets + (long) Long.BYTES * 2 * centroidOrdinal);
                long postingListOffset = centroids.readLong();
                long postingListLength = centroids.readLong();
                return new PostingMetadata(postingListOffset, postingListLength, centroidOrdinal, score);
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
                        scores
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
        float[] scores
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
            quantizeQuery,
            queryParams,
            globalCentroidDp,
            fieldInfo.getVectorSimilarityFunction(),
            scores
        );
    }

    private static void score(
        NeighborQueue neighborQueue,
        int size,
        int scoresOffset,
        ES92Int7VectorsScorer scorer,
        byte[] quantizeQuery,
        OptimizedScalarQuantizer.QuantizationResult queryCorrections,
        float centroidDp,
        VectorSimilarityFunction similarityFunction,
        float[] scores
    ) throws IOException {
        int limit = size - BULK_SIZE + 1;
        int i = 0;
        for (; i < limit; i += BULK_SIZE) {
            scorer.scoreBulk(
                quantizeQuery,
                queryCorrections.lowerInterval(),
                queryCorrections.upperInterval(),
                queryCorrections.quantizedComponentSum(),
                queryCorrections.additionalCorrection(),
                similarityFunction,
                centroidDp,
                scores,
                BULK_SIZE
            );
            for (int j = 0; j < BULK_SIZE; j++) {
                neighborQueue.add(scoresOffset + i + j, scores[j]);
            }
        }

        for (; i < size; i++) {
            float score = scorer.score(
                quantizeQuery,
                queryCorrections.lowerInterval(),
                queryCorrections.upperInterval(),
                queryCorrections.quantizedComponentSum(),
                queryCorrections.additionalCorrection(),
                similarityFunction,
                centroidDp
            );
            neighborQueue.add(scoresOffset + i, score);
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
        // max postings list size, no longer utilized
        indexInput.readVInt();
        QueryQuantizer queryQuantizer = new QueryQuantizer(fieldInfo, target);
        return new MemorySegmentPostingsVisitor(queryQuantizer, indexInput, entry, fieldInfo, acceptDocs);
    }

    private static class QueryQuantizer {
        private OptimizedScalarQuantizer.QuantizationResult queryCorrections;
        private final float[] target;
        private final float[] scratch;
        private final int[] quantizationScratch;
        private final byte[] quantizedQuery;
        private final OptimizedScalarQuantizer quantizer;
        private float[] currentCentroid;
        private int nextCentroidOrdinal = -1;
        private int currentCentroidOrdinal = -2;

        QueryQuantizer(FieldInfo fieldInfo, float[] target) {
            assert fieldInfo.getVectorSimilarityFunction() != COSINE || VectorUtil.isUnitVector(target);
            this.target = target;
            this.scratch = new float[fieldInfo.getVectorDimension()];
            this.quantizationScratch = new int[fieldInfo.getVectorDimension()];
            this.quantizedQuery = new byte[QUERY_BITS * discretize(fieldInfo.getVectorDimension(), 64) / 8];
            this.quantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction(), DEFAULT_LAMBDA, 1);
        }

        void reset(float[] centroid, int centroidOrdinal) {
            this.currentCentroid = centroid;
            this.nextCentroidOrdinal = centroidOrdinal;
        }

        void quantizeQueryIfNecessary() throws IOException {
            if (this.nextCentroidOrdinal != currentCentroidOrdinal) {
                queryCorrections = quantizer.scalarQuantize(target, scratch, quantizationScratch, (byte) 4, currentCentroid);
                transposeHalfByte(quantizationScratch, quantizedQuery);
                currentCentroidOrdinal = this.nextCentroidOrdinal;
            }
        }

        OptimizedScalarQuantizer.QuantizationResult getQueryCorrections() {
            return queryCorrections;
        }

        byte[] getQuantizedTarget() {
            return quantizedQuery;
        }
    }

    private static class MemorySegmentPostingsVisitor implements PostingVisitor {
        final long quantizedByteLength;
        final IndexInput indexInput;
        final FieldEntry entry;
        final FieldInfo fieldInfo;
        final Bits acceptDocs;
        private final ES91OSQVectorsScorer osqVectorsScorer;
        final float[] scores = new float[BULK_SIZE];
        final float[] correctionsLower = new float[BULK_SIZE];
        final float[] correctionsUpper = new float[BULK_SIZE];
        final int[] correctionsSum = new int[BULK_SIZE];
        final float[] correctionsAdd = new float[BULK_SIZE];
        final int[] docIdsScratch = new int[BULK_SIZE];
        byte docEncoding;
        int docBase = 0;
        private final QueryQuantizer queryQuantizer;

        int vectors;
        float centroidDp;
        final float[] centroid;
        long slicePos;

        final DocIdsWriter idsWriter = new DocIdsWriter();
        final float[] correctiveValues = new float[3];
        final long quantizedVectorByteSize;

        MemorySegmentPostingsVisitor(
            QueryQuantizer queryQuantizer,
            IndexInput indexInput,
            FieldEntry entry,
            FieldInfo fieldInfo,
            Bits acceptDocs
        ) throws IOException {
            this.queryQuantizer = queryQuantizer;
            this.indexInput = indexInput;
            this.entry = entry;
            this.fieldInfo = fieldInfo;
            this.acceptDocs = acceptDocs;
            centroid = new float[fieldInfo.getVectorDimension()];
            final int discretizedDimensions = discretize(fieldInfo.getVectorDimension(), 64);
            quantizedByteLength = discretizedDimensions / 8 + (Float.BYTES * 3) + Short.BYTES;
            quantizedVectorByteSize = (discretizedDimensions / 8);
            osqVectorsScorer = ESVectorUtil.getES91OSQVectorsScorer(indexInput, fieldInfo.getVectorDimension(), BULK_SIZE);
        }

        @Override
        public int resetPostingsScorer(PostingMetadata postingMetadata) throws IOException {
            indexInput.seek(postingMetadata.offset());
            indexInput.readFloats(centroid, 0, centroid.length);
            centroidDp = Float.intBitsToFloat(indexInput.readInt());
            vectors = indexInput.readVInt();
            docEncoding = indexInput.readByte();
            docBase = 0;
            slicePos = indexInput.getFilePointer();
            queryQuantizer.reset(centroid, postingMetadata.queryCentroidOrdinal());
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
            for (int j = 0; j < BULK_SIZE; j++) {
                int doc = docIdsScratch[j];
                if (doc != -1) {
                    scores[j] = osqVectorsScorer.score(
                        queryQuantizer.getQueryCorrections().lowerInterval(),
                        queryQuantizer.getQueryCorrections().upperInterval(),
                        queryQuantizer.getQueryCorrections().quantizedComponentSum(),
                        queryQuantizer.getQueryCorrections().additionalCorrection(),
                        fieldInfo.getVectorSimilarityFunction(),
                        centroidDp,
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
                queryQuantizer.quantizeQueryIfNecessary();
                final float maxScore;
                if (docsToBulkScore < BULK_SIZE / 2) {
                    maxScore = scoreIndividually();
                } else {
                    maxScore = osqVectorsScorer.scoreBulk(
                        queryQuantizer.getQuantizedTarget(),
                        queryQuantizer.getQueryCorrections().lowerInterval(),
                        queryQuantizer.getQueryCorrections().upperInterval(),
                        queryQuantizer.getQueryCorrections().quantizedComponentSum(),
                        queryQuantizer.getQueryCorrections().additionalCorrection(),
                        fieldInfo.getVectorSimilarityFunction(),
                        centroidDp,
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
                    queryQuantizer.quantizeQueryIfNecessary();
                    float qcDist = osqVectorsScorer.quantizeScore(queryQuantizer.getQuantizedTarget());
                    indexInput.readFloats(correctiveValues, 0, 3);
                    final int quantizedComponentSum = Short.toUnsignedInt(indexInput.readShort());
                    float score = osqVectorsScorer.score(
                        queryQuantizer.getQueryCorrections().lowerInterval(),
                        queryQuantizer.getQueryCorrections().upperInterval(),
                        queryQuantizer.getQueryCorrections().quantizedComponentSum(),
                        queryQuantizer.getQueryCorrections().additionalCorrection(),
                        fieldInfo.getVectorSimilarityFunction(),
                        centroidDp,
                        correctiveValues[0],
                        correctiveValues[1],
                        quantizedComponentSum,
                        correctiveValues[2],
                        qcDist
                    );
                    scoredDocs++;
                    if (knnCollector.minCompetitiveSimilarity() < score) {
                        knnCollector.collect(doc, score);
                    }
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
