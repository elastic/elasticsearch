/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.HitQueue;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.search.vectors.KnnScoreDocQueryBuilder.findSegmentStarts;

/**
 * Gathers the k highest scoring documents from the inner query, diversified by parent documents.
 */
public class DiversifyingChildrenKnnQuery extends Query {

    private final Query innerQuery;
    private final int k;
    private final BitSetProducer parentsFilter;

    public DiversifyingChildrenKnnQuery(Query query, int k, BitSetProducer parentsFilter) {
        this.innerQuery = query;
        this.k = k;
        this.parentsFilter = parentsFilter;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Query rewritten = innerQuery.rewrite(searcher);
        if (rewritten instanceof MatchNoDocsQuery) {
            return rewritten;
        }
        HitQueue queue = new HitQueue(k, true);
        ScoreDoc topDoc = queue.top();
        Weight innerWeight = rewritten.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        IndexReader reader = searcher.getIndexReader();
        for (LeafReaderContext leafReaderContext : reader.leaves()) {
            Scorer scorer = innerWeight.scorer(leafReaderContext);
            BitSet parentBitSet = parentsFilter.getBitSet(leafReaderContext);
            if (scorer == null || parentBitSet == null) {
                continue;
            }
            DiversifyingChildrenScorer diversifyingScorer = new DiversifyingChildrenScorer(scorer, parentBitSet);
            while (diversifyingScorer.nextParent() != DocIdSetIterator.NO_MORE_DOCS) {
                float score = diversifyingScorer.score();
                if (score > topDoc.score) {
                    topDoc.score = score;
                    topDoc.doc = diversifyingScorer.bestChild() + leafReaderContext.docBase;
                    topDoc = queue.updateTop();
                }
            }
        }
        // Remove any remaining sentinel values
        while (queue.size() > 0 && queue.top().score < 0) {
            queue.pop();
        }

        ScoreDoc[] topScoreDocs = new ScoreDoc[queue.size()];
        for (int i = topScoreDocs.length - 1; i >= 0; i--) {
            topScoreDocs[i] = queue.pop();
        }
        Arrays.sort(topScoreDocs, (a, b) -> Float.compare(a.doc, b.doc));
        int[] docs = new int[topScoreDocs.length];
        float[] scores = new float[topScoreDocs.length];
        for (int i = 0; i < topScoreDocs.length; i++) {
            docs[i] = topScoreDocs[i].doc;
            scores[i] = topScoreDocs[i].score;
        }
        int[] segmentStarts = findSegmentStarts(reader, docs);
        return new KnnScoreDocQuery(docs, scores, segmentStarts, reader.getContext().id());
    }

    @Override
    public void visit(QueryVisitor queryVisitor) {
        queryVisitor.visitLeaf(this);
    }

    @Override
    public String toString(String field) {
        return "DiversifyingChildrenKnnQuery{" + "innerQuery=" + innerQuery + ", k=" + k + ", parentsFilter=" + parentsFilter + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DiversifyingChildrenKnnQuery that = (DiversifyingChildrenKnnQuery) o;
        return k == that.k && Objects.equals(innerQuery, that.innerQuery) && Objects.equals(parentsFilter, that.parentsFilter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(innerQuery, k, parentsFilter);
    }

    static class DiversifyingChildrenScorer {
        private final Scorer scorer;
        private final DocIdSetIterator acceptedChildrenIterator;
        private final BitSet parentBitSet;
        private int currentParent = -1;
        private int bestChild = -1;
        private float currentScore = Float.NEGATIVE_INFINITY;

        protected DiversifyingChildrenScorer(Scorer scorer, BitSet parentBitSet) {
            this.acceptedChildrenIterator = scorer.iterator();
            this.scorer = scorer;
            this.parentBitSet = parentBitSet;
        }

        public int bestChild() {
            return bestChild;
        }

        public int nextParent() throws IOException {
            int nextChild = acceptedChildrenIterator.docID();
            if (nextChild == -1) {
                nextChild = acceptedChildrenIterator.nextDoc();
            }
            if (nextChild == DocIdSetIterator.NO_MORE_DOCS) {
                currentParent = DocIdSetIterator.NO_MORE_DOCS;
                return currentParent;
            }
            currentScore = Float.NEGATIVE_INFINITY;
            currentParent = parentBitSet.nextSetBit(nextChild);
            do {
                float score = scorer.score();
                if (score > currentScore) {
                    bestChild = nextChild;
                    currentScore = score;
                }
            } while ((nextChild = acceptedChildrenIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS && nextChild < currentParent);
            return currentParent;
        }

        public float score() throws IOException {
            return currentScore;
        }
    }
}
