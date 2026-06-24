/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.search.vectors.ESAcceptDocs;
import org.elasticsearch.simdvec.ES92Int7VectorsScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.List;

class FlatCentroidIndex implements CentroidIndex {

    FlatCentroidIndex() {
        final ESNextDiskBBQVectorsReader.NextFieldEntry fieldEntry = fields.get(fieldInfo.number);
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
    }

    private static FixedBitSet getCentroidFilter(
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

    private static FixedBitSet getParentCentroidFilter(
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
    public CentroidIterator getIterator() {
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
    }
}
