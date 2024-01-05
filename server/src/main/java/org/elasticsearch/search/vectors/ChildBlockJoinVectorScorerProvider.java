/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.HitQueue;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenByteKnnVectorQuery;
import org.apache.lucene.search.join.DiversifyingChildrenFloatKnnVectorQuery;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * This class is used to score vector child documents given the results of a {@link DiversifyingChildrenByteKnnVectorQuery}
 * or {@link DiversifyingChildrenFloatKnnVectorQuery}.
 * It iterates the previously matched children, and allows for scoring and finding `numChildrenPerParent` for each parent.
 */
class ChildBlockJoinVectorScorerProvider {

    interface VectorScorer {
        float score() throws IOException;

        DocIdSetIterator iterator();
    }

    interface RandomVectorScorerProvider {
        VectorScorer get(LeafReaderContext context) throws IOException;
    }

    private final Weight childrenFilter;
    private final Weight nearestChildren;
    private final RandomVectorScorerProvider scorerProvider;
    private final BitSetProducer parentsFilter;
    private final int numChildrenPerParent;

    protected ChildBlockJoinVectorScorerProvider(
        @Nullable Weight childrenFilter,
        Weight nearestChildren,
        RandomVectorScorerProvider scorerProvider,
        BitSetProducer parentsFilter,
        int numChildrenPerParent
    ) {
        this.childrenFilter = childrenFilter;
        this.nearestChildren = nearestChildren;
        this.scorerProvider = scorerProvider;
        this.parentsFilter = parentsFilter;
        this.numChildrenPerParent = numChildrenPerParent;
    }

    public ChildBlockJoinVectorScorer scorer(LeafReaderContext context) throws IOException {
        Scorer originallyMatchedChildren = nearestChildren.scorer(context);
        if (originallyMatchedChildren == null) {
            return null;
        }
        final BitSet parentBitSet = parentsFilter.getBitSet(context);
        if (parentBitSet == null) {
            return null;
        }
        Scorer childFilterScorer = childrenFilter == null ? null : childrenFilter.scorer(context);
        VectorScorer scorer = scorerProvider.get(context);
        DocIdSetIterator childFilterIterator = childFilterScorer == null
            ? DocIdSetIterator.all(context.reader().maxDoc())
            : childFilterScorer.iterator();
        DocIdSetIterator childVectorIterator = ConjunctionUtils.createConjunction(
            List.of(childFilterIterator, scorer.iterator()),
            List.of()
        );

        return new ChildBlockJoinVectorScorer(
            context.docBase,
            originallyMatchedChildren.iterator(),
            childVectorIterator,
            parentBitSet,
            scorer,
            numChildrenPerParent
        );
    }

    static class ChildBlockJoinVectorScorer {
        private final VectorScorer vectorScorer;
        private final DocIdSetIterator previouslyFoundChildren;
        private final DocIdSetIterator childFilterIterator;
        private final BitSet parentBitSet;
        private int currentParent = -1;
        private final HitQueue queue;
        private final int docBase;

        protected ChildBlockJoinVectorScorer(
            int docBase,
            DocIdSetIterator previouslyFoundChildren,
            DocIdSetIterator childFilterIterator,
            BitSet parentBitSet,
            VectorScorer vectorScorer,
            int numChildrenPerParent
        ) {
            this.docBase = docBase;
            this.vectorScorer = vectorScorer;
            this.childFilterIterator = childFilterIterator;
            this.previouslyFoundChildren = previouslyFoundChildren;
            this.parentBitSet = parentBitSet;
            this.queue = new HitQueue(numChildrenPerParent, false);
        }

        public PriorityQueue<ScoreDoc> scoredChildren() {
            while (queue.size() > 0 && queue.top() == null) {
                queue.pop();
            }
            return queue;
        }

        public int nextParent() throws IOException {
            queue.clear();
            int nextChild = previouslyFoundChildren.nextDoc();
            if (nextChild == DocIdSetIterator.NO_MORE_DOCS) {
                currentParent = DocIdSetIterator.NO_MORE_DOCS;
                return currentParent;
            }
            currentParent = parentBitSet.nextSetBit(nextChild);
            // The first child of `currentParent`
            nextChild = parentBitSet.prevSetBit(nextChild) + 1;
            // Get to the first child of `currentParent` that matches `childFilterIterator`
            if (childFilterIterator.docID() < nextChild) {
                nextChild = childFilterIterator.advance(nextChild);
            }
            // now iterate over all children of the current parent in childFilterIterator
            do {
                float score = vectorScorer.score();
                queue.insertWithOverflow(new ScoreDoc(nextChild + docBase, score));
            } while ((nextChild = childFilterIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS && nextChild < currentParent);
            return currentParent;
        }
    }
}
