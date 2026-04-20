/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.vectors;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;
import org.elasticsearch.index.codec.vectors.diskbbq.VectorPreconditioner;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** A {@link IVFKnnFloatVectorQuery} that uses the IVF search strategy. */
public class IVFKnnFloatVectorQuery extends AbstractIVFKnnVectorQuery implements PostFilterableKnnQuery {

    private boolean isQueryPreconditioned = false;
    private float[] query;
    protected final float[] originalQuery;
    private final Map<Integer, FixedBitSet> skipCentroidsPerLeaf;
    private final ConcurrentHashMap<Integer, FixedBitSet> visitedCentroidsPerLeaf = new ConcurrentHashMap<>();

    /**
     * Creates a new {@link IVFKnnFloatVectorQuery} with the given parameters.
     *
     * @param field      the field to search
     * @param query      the query vector
     * @param k          the number of nearest neighbors to return
     * @param numCands   the number of nearest neighbors to gather per shard
     * @param filter     the filter to apply to the results
     * @param visitRatio the ratio of vectors to score for the IVF search strategy
     */
    public IVFKnnFloatVectorQuery(
        String field,
        float[] query,
        int k,
        int numCands,
        Query filter,
        float visitRatio,
        boolean doPrecondition
    ) {
        this(field, query, k, numCands, filter, visitRatio, doPrecondition, false, null);
    }

    IVFKnnFloatVectorQuery(
        String field,
        float[] query,
        int k,
        int numCands,
        Query filter,
        float visitRatio,
        boolean doPrecondition,
        boolean shouldPostFilter,
        Map<Integer, FixedBitSet> skipCentroidsPerLeaf
    ) {
        super(field, visitRatio, k, numCands, filter, doPrecondition, shouldPostFilter);
        this.query = query;
        this.originalQuery = query.clone();
        this.skipCentroidsPerLeaf = skipCentroidsPerLeaf;
    }

    public float[] getQuery() {
        return query;
    }

    FixedBitSet getSkipCentroids(int leafOrd) {
        return skipCentroidsPerLeaf != null ? skipCentroidsPerLeaf.get(leafOrd) : null;
    }

    void storeVisitedCentroids(int leafOrd, FixedBitSet visited) {
        visitedCentroidsPerLeaf.put(leafOrd, visited);
    }

    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        buffer.append(getClass().getSimpleName())
            .append(":")
            .append(this.field)
            .append("[")
            .append(query[0])
            .append(",...]")
            .append("[")
            .append(k)
            .append("]");
        if (this.filter != null) {
            buffer.append("[").append(this.filter).append("]");
        }
        if (this.shouldPostFilter) {
            buffer.append("[true]");
        }
        return buffer.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (super.equals(o) == false) return false;
        IVFKnnFloatVectorQuery that = (IVFKnnFloatVectorQuery) o;
        return Arrays.equals(query, that.query);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Arrays.hashCode(query);
        return result;
    }

    @Override
    protected int countTotalVectors(List<LeafReaderContext> leaves) throws IOException {
        int totalVectors = 0;
        for (LeafReaderContext leaf : leaves) {
            FloatVectorValues fvv = leaf.reader().getFloatVectorValues(field);
            if (fvv != null) {
                totalVectors += fvv.size();
            }
        }
        return totalVectors;
    }

    @Override
    protected synchronized void preconditionQuery(LeafReaderContext context) throws IOException {
        if (isQueryPreconditioned) {
            // already preconditioned
            return;
        }
        LeafReader reader = context.reader();
        SegmentReader segmentReader = Lucene.tryUnwrapSegmentReader(reader);
        if (segmentReader == null) {
            // ignore and continue to the next leaf context to see if we can get a segment reader there
            return;
        }
        KnnVectorsReader fieldsReader = segmentReader.getVectorReader();
        if (fieldsReader instanceof PerFieldKnnVectorsFormat.FieldsReader) {
            KnnVectorsReader knnVectorsReader = ((PerFieldKnnVectorsFormat.FieldsReader) fieldsReader).getFieldReader(field);
            if (knnVectorsReader instanceof VectorPreconditioner) {
                FieldInfo fieldInfo = segmentReader.getFieldInfos().fieldInfo(field);
                Preconditioner preconditioner = ((VectorPreconditioner) knnVectorsReader).getPreconditioner(fieldInfo);
                if (preconditioner != null) {
                    final float[] out = new float[query.length];
                    preconditioner.applyTransform(query, out);
                    // have to keep the copy to avoid issues with reused arrays by the caller of IVFKnnFloatVectorQuery which expects
                    // a non-preconditioned query vector to still exist
                    query = out;
                    isQueryPreconditioned = true;
                }
            }
        }
    }

    @Override
    protected TopDocs approximateSearch(
        LeafReaderContext context,
        AcceptDocs acceptDocs,
        int visitedLimit,
        IVFCollectorManager knnCollectorManager,
        float visitRatio
    ) throws IOException {
        LeafReader reader = context.reader();
        FloatVectorValues floatVectorValues = reader.getFloatVectorValues(field);
        if (floatVectorValues == null) {
            FloatVectorValues.checkField(reader, field);
            return NO_RESULTS;
        }
        if (floatVectorValues.size() == 0) {
            return NO_RESULTS;
        }
        IVFKnnSearchStrategy strategy = new IVFKnnSearchStrategy(
            visitRatio,
            numCands,
            k,
            knnCollectorManager.longAccumulator,
            getSkipCentroids(context.ord)
        );
        AbstractMaxScoreKnnCollector knnCollector = knnCollectorManager.newCollector(visitedLimit, strategy, context);
        if (knnCollector == null) {
            return NO_RESULTS;
        }
        strategy.setCollector(knnCollector);
        reader.searchNearestVectors(field, query, knnCollector, acceptDocs);

        storeVisitedCentroids(context.ord, strategy.visitedCentroids());

        TopDocs results = knnCollector.topDocs();
        return results != null ? results : NO_RESULTS;
    }

    /**
     * Returns the original (non-preconditioned) query vector.
     */
    float[] getOriginalQuery() {
        return originalQuery;
    }

    /**
     * Merges previously skipped centroids with centroids visited in the current round.
     */
    Map<Integer, FixedBitSet> mergeSkipCentroids() {
        Map<Integer, FixedBitSet> mergedSkip = new HashMap<>();
        if (skipCentroidsPerLeaf != null) {
            for (var entry : skipCentroidsPerLeaf.entrySet()) {
                mergedSkip.put(entry.getKey(), entry.getValue().clone());
            }
        }
        for (var entry : visitedCentroidsPerLeaf.entrySet()) {
            mergedSkip.merge(entry.getKey(), entry.getValue().clone(), (existing, visited) -> {
                existing.or(visited);
                return existing;
            });
        }
        return mergedSkip;
    }

    @Override
    PostFilterableKnnQuery createPostFilterDelegate(int scaledK, int scaledNumCands, float scaledVisitRatio) {
        return new IVFKnnFloatVectorQuery(
            field,
            originalQuery.clone(),
            scaledK,
            scaledNumCands,
            null,
            scaledVisitRatio,
            doPrecondition,
            true,
            null
        );
    }

    @Override
    public TopDocs capturedResults() {
        return capturedResults;
    }

    @Override
    public PostFilterableKnnQuery createRetryQuery(IndexReader reader) {
        Map<Integer, FixedBitSet> mergedSkip = mergeSkipCentroids();
        return new IVFKnnFloatVectorQuery(
            field,
            originalQuery.clone(),
            k,
            numCands,
            null,
            providedVisitRatio,
            doPrecondition,
            true,
            mergedSkip
        );
    }

    @Override
    public long vectorOpsCount() {
        return vectorOpsCount;
    }

    protected void skipCentroids(Map<Integer, FixedBitSet> mergedSkip) {
        this.skipCentroidsPerLeaf.clear();
        this.skipCentroidsPerLeaf.putAll(mergedSkip);
    }
}
