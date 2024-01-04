/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;


import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.HitQueue;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenFloatKnnVectorQuery;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ESDiversifyingChildrenFloatKnnVectorQuery extends DiversifyingChildrenFloatKnnVectorQuery {

    private final int numChildrenPerParent;
    private final BitSetProducer parentsFilter;
    private final float[] query;
    /**
     * Create a ToParentBlockJoinByteVectorQuery.
     *
     * @param field         the query field
     * @param query         the vector query
     * @param childFilter   the child filter
     * @param k             how many parent documents to return given the matching children
     * @param parentsFilter Filter identifying the parent documents.
     */
    public ESDiversifyingChildrenFloatKnnVectorQuery(
        String field,
        float[] query,
        Query childFilter,
        int k,
        BitSetProducer parentsFilter,
        int numChildrenPerParent
    ) {
        super(field, query, childFilter, k, parentsFilter);
        this.numChildrenPerParent = numChildrenPerParent;
        if (this.numChildrenPerParent < 1) {
            throw new IllegalArgumentException("numChildrenPerParent must be >= 1");
        }
        this.parentsFilter = parentsFilter;
        this.query = query;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        IndexReader reader = indexSearcher.getIndexReader();

        // This gathers the nearest single child for each parent
        Query rewritten = super.rewrite(indexSearcher);
        if (numChildrenPerParent == 1) {
            return rewritten;
        }
        // Iterate these matched docs to attempt to gather and score at least numChildrenPerParent children for each parent
        // returning the top numChildrenPerParent children for each parent
        // The matchingChildren used to iterate the current matching children to gather the appropriate parents & children
        Weight matchingChildren = rewritten.createWeight(indexSearcher, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        Weight childrenFilter = getFilter() == null ? null : indexSearcher.rewrite( new BooleanQuery.Builder()
            .add(getFilter(), BooleanClause.Occur.FILTER)
            .add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER)
            .build()).createWeight(indexSearcher, ScoreMode.COMPLETE_NO_SCORES, 1.0f);

        List<ScoreDoc> topChildren = new ArrayList<>();
        for (LeafReaderContext context : reader.leaves()) {
            final FieldInfo fi = context.reader().getFieldInfos().fieldInfo(field);
            if (fi == null || fi.getVectorDimension() == 0) {
                // The field does not exist or does not index vectors
                continue;
            }
            if (fi.getVectorEncoding() != VectorEncoding.BYTE) {
                return null;
            }
            final BitSet parentBitSet = parentsFilter.getBitSet(context);
            if (parentBitSet == null) {
                continue;
            }
            Scorer originallyMatchedChildren = matchingChildren.scorer(context);
            if (originallyMatchedChildren == null) {
                continue;
            }
            Scorer childFilterScorer = childrenFilter == null ? null : childrenFilter.scorer(context);
            FloatVectorValues values = context.reader().getFloatVectorValues(field);
            VectorSimilarityFunction function = fi.getVectorSimilarityFunction();
            DocIdSetIterator childFilterIterator = childFilterScorer == null ?
                DocIdSetIterator.all(context.reader().maxDoc()) :
                childFilterScorer.iterator();
            ParentBlockJoinByteVectorScorer scorer = new ParentBlockJoinByteVectorScorer(
                originallyMatchedChildren.iterator(),
                childFilterIterator,
                parentBitSet,
                new RandomVectorScorer() {
                    @Override
                    public float score(int node) throws IOException {
                        values.advance(node);
                        return function.compare(query, values.vectorValue());
                    }

                    @Override
                    public int maxOrd() {
                        return values.size();
                    }
                },
                numChildrenPerParent
            );
            while (scorer.nextParent() != DocIdSetIterator.NO_MORE_DOCS) {
                PriorityQueue<ScoreDoc> childDoc = scorer.scoredChildren();
                while(childDoc.size() > 0) {
                    topChildren.add(childDoc.pop());
                }
            }
        }
        topChildren.sort(Comparator.comparingInt(scoreDoc -> scoreDoc.doc));
        ScoreDoc[] scoreDocs = topChildren.toArray(new ScoreDoc[0]);
        int numDocs = scoreDocs.length;
        int[] docs = new int[numDocs];
        float[] scores = new float[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = scoreDocs[i].doc;
            scores[i] = scoreDocs[i].score;
        }

        int[] segmentStarts = KnnScoreDocQueryBuilder.findSegmentStarts(reader, docs);
        return new KnnScoreDocQuery(docs, scores, segmentStarts, reader.getContext().id());
    }

    private static class ParentBlockJoinByteVectorScorer {
        private final RandomVectorScorer vectorScorer;
        private final DocIdSetIterator previouslyFoundChildren;
        private final DocIdSetIterator childFilterIterator;
        private final BitSet parentBitSet;
        private int currentParent = -1;
        private final HitQueue queue;

        protected ParentBlockJoinByteVectorScorer(
            DocIdSetIterator previouslyFoundChildren,
            DocIdSetIterator childFilterIterator,
            BitSet parentBitSet,
            RandomVectorScorer vectorScorer,
            int numChildrenPerParent) {
            this.vectorScorer = vectorScorer;
            this.childFilterIterator = childFilterIterator;
            this.previouslyFoundChildren = previouslyFoundChildren;
            this.parentBitSet = parentBitSet;
            this.queue = new HitQueue(numChildrenPerParent, true);
        }

        public PriorityQueue<ScoreDoc> scoredChildren() {
            while (queue.size() > 0 && queue.top().score < 0) {
                queue.pop();
            }
            return queue;
        }

        public int nextParent() throws IOException {
            queue.clear();
            int nextChild = previouslyFoundChildren.docID();
            if (nextChild == -1) {
                nextChild = previouslyFoundChildren.nextDoc();
            }
            if (nextChild == DocIdSetIterator.NO_MORE_DOCS) {
                currentParent = DocIdSetIterator.NO_MORE_DOCS;
                return currentParent;
            }
            currentParent = parentBitSet.nextSetBit(nextChild);
            // The first child of `currentParent`
            nextChild = parentBitSet.prevSetBit(nextChild) + 1;
            // Get to the first child of `currentParent` that matches `childFilterIterator`
            nextChild = childFilterIterator.advance(nextChild);
            ScoreDoc topDoc = queue.top();
            // now iterate over all children of the current parent in childFilterIterator
            do {
                float score = vectorScorer.score(nextChild);
                if (score > topDoc.score) {
                    topDoc.score = score;
                    topDoc.doc = nextChild;
                    topDoc = queue.updateTop();
                }
            } while ((nextChild = childFilterIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS
                && nextChild < currentParent);
            return currentParent;
        }
    }
}
