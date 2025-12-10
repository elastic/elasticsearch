/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.vectors;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** A {@link IVFKnnFloatVectorQuery} that uses the IVF search strategy. */
public class IVFKnnFloatVectorQuery extends AbstractIVFKnnVectorQuery {

    private final float[] query;

    /**
     * Creates a new {@link IVFKnnFloatVectorQuery} with the given parameters.
     * @param field the field to search
     * @param query the query vector
     * @param k the number of nearest neighbors to return
     * @param numCands the number of nearest neighbors to gather per shard
     * @param filter the filter to apply to the results
     * @param visitRatio the ratio of vectors to score for the IVF search strategy
     */
    public IVFKnnFloatVectorQuery(String field, float[] query, int k, int numCands, Query filter, float visitRatio) {
        super(field, visitRatio, k, numCands, filter);
        this.query = query;
    }

    public float[] getQuery() {
        return query;
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
    protected TopDocs approximateSearch(
        LeafReaderContext context,
        AcceptDocs filterDocs,
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

        // Check if we should apply true post-filtering
        boolean applyPostFilter = filterDocs instanceof ESAcceptDocs.PostFilterEsAcceptDocs;

        IVFKnnSearchStrategy strategy = new IVFKnnSearchStrategy(visitRatio, knnCollectorManager.longAccumulator);
        // for post-filtering oversample
        AbstractMaxScoreKnnCollector knnCollector = applyPostFilter
            ? knnCollectorManager.newOptimisticCollector(
                visitedLimit,
                strategy,
                context,
                Math.round(
                    (1 + (3 * (1f - (float) ((ESAcceptDocs.PostFilterEsAcceptDocs) filterDocs).approximateCost() / floatVectorValues
                        .size()))) * k
                )
            )
            : knnCollectorManager.newCollector(visitedLimit, strategy, context);
        if (knnCollector == null) {
            return NO_RESULTS;
        }
        strategy.setCollector(knnCollector);
        reader.searchNearestVectors(field, query, knnCollector, filterDocs);
        TopDocs results = knnCollector.topDocs();

        // Apply post-filtering if needed
        if (applyPostFilter && results != null && results.scoreDocs.length > 0) {
            results = applyPostFilter(results, (ESAcceptDocs.PostFilterEsAcceptDocs) filterDocs);
        }

        return results != null ? results : NO_RESULTS;
    }

    private TopDocs applyPostFilter(TopDocs results, ESAcceptDocs.PostFilterEsAcceptDocs filterDocs) throws IOException {
        if (results.scoreDocs.length == 0) {
            return results;
        }

        var scorerSupplier = filterDocs.weight().scorerSupplier(filterDocs.context());
        if (scorerSupplier == null) {
            // No documents match the filter - should we revisit this or if we end up returning less than `k` ?
            return NO_RESULTS;
        }
        DocIdSetIterator filterIterator = scorerSupplier.get(NO_MORE_DOCS).iterator();

        Arrays.sort(results.scoreDocs, Comparator.comparingInt(sd -> sd.doc));
        ArrayList<ScoreDoc> filteredDocs = new ArrayList<>(results.scoreDocs.length);
        assert filterIterator.docID() == -1;
        for (ScoreDoc scoreDoc : results.scoreDocs) {
            if (filterIterator.docID() == NO_MORE_DOCS) {
                break;
            }
            if (filterIterator.docID() > scoreDoc.doc) {
                continue;
            }
            if (filterIterator.advance(scoreDoc.doc) == scoreDoc.doc) {
                filteredDocs.add(scoreDoc);
            }
        }

        filteredDocs.sort(Comparator.comparingDouble((ScoreDoc sd) -> sd.score).reversed());
        int numResults = Math.min(k, filteredDocs.size());
        ScoreDoc[] topK = new ScoreDoc[numResults];
        for (int i = 0; i < numResults; i++) {
            topK[i] = filteredDocs.get(i);
        }

        return new TopDocs(new TotalHits(numResults, results.totalHits.relation()), topK);
    }
}
