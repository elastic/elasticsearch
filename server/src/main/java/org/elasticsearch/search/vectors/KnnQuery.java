/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.search.vectors.KnnScoreDocQueryBuilder.findSegmentStarts;

/**
 * This gathers the k highest scoring documents from the inner query.
 */
public class KnnQuery extends Query {

    private final Query innerQuery;
    private final int k;

    public KnnQuery(Query innerQuery, int k) {
        this.innerQuery = innerQuery;
        this.k = k;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Query rewritten = innerQuery.rewrite(searcher);
        // Now we execute the query and gather the top k results
        TopDocs topDocs = searcher.search(rewritten, k);
        if (topDocs.totalHits.value == 0) {
            return new MatchNoDocsQuery();
        }
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        Arrays.sort(scoreDocs, (a, b) -> Float.compare(a.doc, b.doc));
        int numDocs = scoreDocs.length;
        int[] docs = new int[numDocs];
        float[] scores = new float[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = scoreDocs[i].doc;
            scores[i] = scoreDocs[i].score;
        }
        IndexReader reader = searcher.getIndexReader();
        int[] segmentStarts = findSegmentStarts(reader, docs);
        return new KnnScoreDocQuery(docs, scores, segmentStarts, reader.getContext().id());
    }

    @Override
    public String toString(String field) {
        return "KnnQuery{" + "innerQuery=" + innerQuery + ", k=" + k + '}';
    }

    @Override
    public void visit(QueryVisitor queryVisitor) {
        innerQuery.visit(queryVisitor);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KnnQuery knnQuery = (KnnQuery) o;
        return k == knnQuery.k && Objects.equals(innerQuery, knnQuery.innerQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(innerQuery, k);
    }
}
