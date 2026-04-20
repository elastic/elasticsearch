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
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;
import org.elasticsearch.index.codec.vectors.diskbbq.PrefetchingCentroidIterator;
import org.elasticsearch.index.codec.vectors.diskbbq.VectorPreconditioner;
import org.elasticsearch.search.vectors.BulkKnnCollector;
import org.elasticsearch.search.vectors.ESAcceptDocs;
import org.elasticsearch.simdvec.ES92Int7VectorsScorer;
import org.elasticsearch.simdvec.ES940OSQVectorsScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
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
        VectorPreconditioner {

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
    protected int getNumberOfVectors(NextFieldEntry entry, FloatVectorValues values, IndexInput centroidSlice, ESAcceptDocs esAcceptDocs)
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
        final NextFieldEntry fieldEntry = fields.get(fieldInfo.number);
        // build optmization filters if possible
        final FixedBitSet acceptCentroids = getCentroidFilter(centroids, numCentroids, values, acceptDocs, approximateCost);
        final int numParents = centroids.readVInt();
        final FixedBitSet acceptParents = getParentCentroidFilter(centroids, numParents, numCentroids, acceptDocs, fieldEntry.numSlices);
        // build centroid search helpers
        final int bulkSize = fieldEntry.getBulkSize();
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
        // build iterator
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
                acceptParents,
                acceptCentroids,
                bulkSize
            );
        } else {
            if (acceptCentroids != null && acceptParents != null) {
                acceptCentroids.and(acceptParents);
            }
            centroidIterator = getCentroidIteratorNoParent(
                fieldInfo,
                centroids,
                numCentroids,
                scorer,
                quantized,
                queryParams,
                fieldEntry.globalCentroidDp(),
                acceptCentroids != null ? acceptCentroids : acceptParents,
                bulkSize
            );
        }
        return getPostingListPrefetchIterator(centroidIterator, postingListSlice);
    }

    private FixedBitSet getCentroidFilter(
        IndexInput centroids,
        int numCentroids,
        FloatVectorValues values,
        AcceptDocs acceptDocs,
        float approximateCost
    ) throws IOException {
        float approximateDocsPerCentroid = approximateCost / numCentroids;
        if (approximateDocsPerCentroid <= 1.25) {
            // TODO: we need to make this call to build the iterator, otherwise accept docs breaks all together
            approximateDocsPerCentroid = (float) acceptDocs.cost() / numCentroids;
        }
        final int bitsRequired = DirectWriter.bitsRequired(numCentroids);
        final long sizeLookup = DirectWriter.bytesRequired(values.size(), bitsRequired);
        long fp = centroids.getFilePointer();
        final FixedBitSet acceptCentroids;
        if (approximateDocsPerCentroid > 1.25 || numCentroids == 1 || acceptDocs instanceof ESAcceptDocs.ESAcceptDocsAll) {
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
        centroids.seek(fp + sizeLookup);
        return acceptCentroids;
    }

    private FixedBitSet getParentCentroidFilter(
        IndexInput centroids,
        int numParents,
        int numCentroids,
        AcceptDocs acceptDocs,
        int numSlices
    ) throws IOException {
        if (numSlices <= 0) {
            return null;
        }
        long fp = centroids.getFilePointer();
        FixedBitSet acceptParents = null;
        if (acceptDocs instanceof ESAcceptDocs esAcceptDocs) {
            // build a parent centroids filter
            int slice = esAcceptDocs.sliceOrd();
            // a slice must be provided
            assert slice >= 0 && slice < numSlices : "sliceOrd out of range for centroid slices";
            final int startOffset;
            final int endOffset;
            if (slice == 0) {
                startOffset = 0;
                endOffset = centroids.readInt();
            } else {
                centroids.skipBytes((long) (slice - 1) * Integer.BYTES);
                startOffset = centroids.readInt();
                endOffset = centroids.readInt();
            }
            if (numParents > 0) {
                acceptParents = new FixedBitSet(numParents);
                assert startOffset >= 0 && endOffset <= numParents;
            } else {
                acceptParents = new FixedBitSet(numCentroids);
                assert startOffset >= 0 && endOffset <= numCentroids;
            }
            acceptParents.set(startOffset, endOffset);
        }
        centroids.seek(fp + (long) numSlices * Integer.BYTES);
        return acceptParents;
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
        ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding = ESNextDiskBBQVectorsFormat.QuantEncoding.fromId(input.readInt());
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
            bulkSize,
            preconditionerOffset,
            preconditionerLength,
            numSlices,
            maxSliceSize
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

    protected static class NextFieldEntry extends FieldEntry {
        private final ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding;
        protected final long preconditionerOffset;
        protected final long preconditionerLength;
        // -1 "not sliced".
        // 0 "sliced but on flush".
        // > 0 "sliced but on merge, is the number of slices".
        final int numSlices;
        final int maxSliceSize;

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
            int bulkSize,
            long preconditionerOffset,
            long preconditionerLength,
            int numSlices,
            int maxSliceSize
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
            this.preconditionerOffset = preconditionerOffset;
            this.preconditionerLength = preconditionerLength;
            this.numSlices = numSlices;
            this.maxSliceSize = maxSliceSize;
        }

        public ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding() {
            return quantEncoding;
        }

        public long preconditionerOffset() {
            return preconditionerOffset;
        }

        public long preconditionerLength() {
            return preconditionerLength;
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
                // NO_ORDINAL indicates that the global centroid should be used for query quantization
                return new PostingMetadata(postingListOffset, postingListLength, NO_ORDINAL, score);
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
        FixedBitSet acceptParents,
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
                if (acceptParents == null || acceptParents.get(i)) {
                    parentsQueue.add(i, 0.5f);
                }
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
                acceptParents,
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
        FloatVectorValues values,
        IndexInput indexInput,
        float[] target,
        Bits needsScoring,
        IndexInput centroidSlice,
        ESAcceptDocs acceptDocs
    ) throws IOException {
        NextFieldEntry entry = fields.get(fieldInfo.number);
        if (entry.numSlices > 0) {
            final int bitsRequired = DirectWriter.bitsRequired(entry.maxSliceSize);
            final long sizeLookup = DirectWriter.bytesRequired(entry.numSlices, bitsRequired);
            centroidSlice.skipBytes(sizeLookup);
        }
        final int bitsRequired = DirectWriter.bitsRequired(entry.numCentroids());
        final long sizeLookup = DirectWriter.bytesRequired(values.size(), bitsRequired);
        centroidSlice.skipBytes(sizeLookup);
        ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding = entry.quantEncoding();
        int numParents = centroidSlice.readVInt();
        if (entry.numSlices > 0) {
            // skip slice offsets
            centroidSlice.skipBytes((long) entry.numSlices * Integer.BYTES);
        }
        final QueryQuantizer queryQuantizer;
        if (numParents > 0) {
            // unused
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
        if (entry.numSlices == 0) {
            // should only happen in sliced flushed segments
            assert entry.numCentroids() == 1;
            int startDoc;
            int endDoc;
            if (acceptDocs == null) {
                startDoc = 0;
                endDoc = values.ordToDoc(values.size() - 1);
            } else {
                ESAcceptDocs.SliceAcceptDocs sliceAcceptDocs = acceptDocs.sliceAcceptDocs();
                startDoc = sliceAcceptDocs.startDoc();
                endDoc = sliceAcceptDocs.endDoc();
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
        private final ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding;
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
        final FloatVectorValues floatVectorValues;

        SlicedMemorySegmentPostingsVisitor(
            QueryQuantizer queryQuantizer,
            ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding,
            IndexInput indexInput,
            FieldEntry entry,
            FieldInfo fieldInfo,
            Bits acceptDocs,
            FloatVectorValues values,
            int startDocId,
            int endDocId
        ) throws IOException {
            super(queryQuantizer, quantEncoding, indexInput, entry, fieldInfo, acceptDocs);
            this.startDocId = startDocId;
            this.endDocId = endDocId;
            this.floatVectorValues = values;
        }

        @Override
        public int resetPostingsScorer(PostingMetadata metadata) throws IOException {
            int totalVectors = super.resetPostingsScorer(metadata);
            int totalBlocks = totalVectors / BULK_SIZE;
            KnnVectorValues.DocIndexIterator iterator = floatVectorValues.iterator();
            if (iterator.advance(startDocId) > endDocId) {
                this.vectors = 0;
                return 0;
            }
            int minOrd = iterator.index();
            int docId = iterator.advance(endDocId);
            int maxOrd;
            if (docId == DocIdSetIterator.NO_MORE_DOCS) {
                maxOrd = floatVectorValues.size() - 1;
            } else {
                maxOrd = iterator.index();
            }
            assert maxOrd - minOrd + 1 <= totalVectors;
            int startBlock = minOrd / BULK_SIZE;
            int endBlock = maxOrd / BULK_SIZE;
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
        protected void readDocIds(int count) {
            for (int j = 0; j < count; j++) {
                int docId = floatVectorValues.ordToDoc(docBase++);
                if (docId >= startDocId && docId <= endDocId) {
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
            ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding,
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
                quantEncoding.bits() == 4
                    ? ES940OSQVectorsScorer.SymmetricInt4Encoding.PACKED_NIBBLE
                    : ES940OSQVectorsScorer.SymmetricInt4Encoding.STRIPED
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

        private static int docToBulkScore(int[] docIds, int[] offsets, Bits acceptDocs, int bulkSize) {
            assert acceptDocs != null : "acceptDocs must not be null";
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
                final int docsToBulkScore = acceptDocs == null
                    ? BULK_SIZE
                    : docToBulkScore(docIdsScratch, offsetsScratch, acceptDocs, BULK_SIZE);
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
                final int docsToBulkScore = acceptDocs == null
                    ? tailSize
                    : docToBulkScore(docIdsScratch, offsetsScratch, acceptDocs, tailSize);
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
