/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenByteKnnVectorQuery;
import org.apache.lucene.search.join.DiversifyingChildrenFloatKnnVectorQuery;
import org.apache.lucene.util.BitSet;

import java.io.IOException;
import java.util.Objects;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * This query is used to score vector child documents given the results of a {@link DiversifyingChildrenByteKnnVectorQuery}
 * or {@link DiversifyingChildrenFloatKnnVectorQuery}.
 */
public class ESDiversifyingChildrenKnnVectorQuery extends Query {

    private final Query nearestChildren;
    private final Query exactKnnQuery;
    private final BitSetProducer parentsFilter;

    public ESDiversifyingChildrenKnnVectorQuery(Query nearestChildren, Query exactKnnQuery, BitSetProducer parentsFilter) {
        this.nearestChildren = nearestChildren;
        this.exactKnnQuery = exactKnnQuery;
        this.parentsFilter = parentsFilter;
    }

    Query getNearestChildren() {
        return nearestChildren;
    }

    Query getExactKnnQuery() {
        return exactKnnQuery;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        Query nearestChildren = this.nearestChildren.rewrite(indexSearcher);

        if (nearestChildren instanceof MatchNoDocsQuery) {
            return nearestChildren;
        }
        if (nearestChildren == this.nearestChildren) {
            return this;
        }
        return new ESDiversifyingChildrenKnnVectorQuery(nearestChildren, exactKnnQuery, parentsFilter);
    }

    @Override
    public String toString(String field) {
        return "ESDiversifyingChildrenKnnVectorQuery{nearestChildren=" + nearestChildren + '}';
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ESDiversifyingChildrenKnnVectorQuery that = (ESDiversifyingChildrenKnnVectorQuery) o;
        return Objects.equals(nearestChildren, that.nearestChildren)
            && Objects.equals(exactKnnQuery, that.exactKnnQuery)
            && Objects.equals(parentsFilter, that.parentsFilter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nearestChildren, exactKnnQuery, parentsFilter);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        final Weight nearestChildrenWeight = nearestChildren.createWeight(searcher, scoreMode, boost);
        final Weight exactKnnWeight = exactKnnQuery.createWeight(searcher, scoreMode, boost);

        return new ESDiversifyingChildrenKnnVectorWeight(this, nearestChildrenWeight, exactKnnWeight, parentsFilter);
    }

    static final class ESDiversifyingChildrenKnnVectorWeight extends Weight {
        private final Weight nearestChildren, exactKnn;
        private final BitSetProducer parentsFilter;

        /**
         * @param query The query
         * @param nearestChildren The query that matches the nearest children
         * @param exactKnn The query that matches the exact KNN children
         * @param parentsFilter The filter that matches the parents of the nearest children
         */
        ESDiversifyingChildrenKnnVectorWeight(Query query, Weight nearestChildren, Weight exactKnn, BitSetProducer parentsFilter) {
            super(query);
            this.nearestChildren = nearestChildren;
            this.exactKnn = exactKnn;
            this.parentsFilter = parentsFilter;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return null;
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            Scorer originallyMatchedChildren = nearestChildren.scorer(context);
            if (originallyMatchedChildren == null) {
                return null;
            }
            final BitSet parentBitSet = parentsFilter.getBitSet(context);
            if (parentBitSet == null) {
                return null;
            }
            Scorer scorer = exactKnn.scorer(context);
            if (scorer == null) {
                return null;
            }
            DocIdSetIterator childVectorIterator = scorer.iterator();
            return new ESDiversifyingChildrenKnnVectorScorer(this, originallyMatchedChildren, childVectorIterator, parentBitSet, scorer);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return true;
        }
    }

    static final class ESDiversifyingChildrenKnnVectorScorer extends Scorer {
        private final Scorer originallyMatchedChildren, vectorScorer;
        private final DocIdSetIterator childFilterIterator, originallyMatchedChildrenIterator;
        private final BitSet parentBitSet;
        private final double inverseParentPercentage;
        private int currentParent = 0, currentDocID = -1;

        ESDiversifyingChildrenKnnVectorScorer(
            Weight weight,
            Scorer originallyMatchedChildren,
            DocIdSetIterator childFilterIterator,
            BitSet parentBitSet,
            Scorer vectorScorer
        ) {
            super(weight);
            this.vectorScorer = vectorScorer;
            this.childFilterIterator = childFilterIterator;
            this.originallyMatchedChildren = originallyMatchedChildren;
            this.originallyMatchedChildrenIterator = originallyMatchedChildren.iterator();
            this.parentBitSet = parentBitSet;
            this.inverseParentPercentage = 1.0 / ((double) parentBitSet.cardinality() / parentBitSet.length());
        }

        @Override
        public int docID() {
            return currentDocID;
        }

        @Override
        public float score() throws IOException {
            if (currentDocID == NO_MORE_DOCS || currentDocID == -1) {
                throw new ArrayIndexOutOfBoundsException(currentDocID);
            }
            return vectorScorer.score();
        }

        @Override
        public DocIdSetIterator iterator() {
            return new DocIdSetIterator() {
                @Override
                public int docID() {
                    return currentDocID;
                }

                @Override
                public int nextDoc() throws IOException {
                    return advance(currentDocID + 1);
                }

                @Override
                public int advance(int target) throws IOException {
                    // We are advancing passed the current parent, so we need to find the next parent
                    if (target >= currentParent) {
                        if (target == NO_MORE_DOCS) {
                            return currentDocID = currentParent = NO_MORE_DOCS;
                        }
                        // We can find the next parent by finding the next child
                        // This is because originally, only one child pre parent was matched
                        advanceToNextParent(target);
                        return currentDocID;
                    } else {
                        int nextMatchedChild = childFilterIterator.advance(target);
                        if (nextMatchedChild == NO_MORE_DOCS) {
                            return currentDocID = currentParent = NO_MORE_DOCS;
                        }
                        // If we haven't iterated passed the current parent, simply return the matching child
                        if (nextMatchedChild < currentParent) {
                            return currentDocID = nextMatchedChild;
                        }
                        advanceToNextParent(nextMatchedChild);
                        return currentDocID;
                    }
                }

                private void advanceToNextParent(int target) throws IOException {
                    // Iterate to the next child, and then find the parent of that child
                    int nextMatchedChild = originallyMatchedChildrenIterator.advance(target);
                    if (nextMatchedChild == NO_MORE_DOCS) {
                        currentDocID = currentParent = NO_MORE_DOCS;
                        return;
                    }
                    assert parentBitSet.get(nextMatchedChild) == false;
                    currentParent = parentBitSet.nextSetBit(nextMatchedChild);
                    // Go to the first child of this parent block
                    int firstChild = parentBitSet.prevSetBit(currentParent - 1) + 1;
                    // If we are before the first child, advance to the first child
                    // Otherwise, we are already at or after the first child in this block by previous iteration
                    if (childFilterIterator.docID() < firstChild) {
                        currentDocID = childFilterIterator.advance(firstChild);
                    } else {
                        currentDocID = childFilterIterator.docID();
                    }
                    if (currentDocID == NO_MORE_DOCS) {
                        currentParent = NO_MORE_DOCS;
                        return;
                    }
                    assert currentDocID < currentParent;
                    assert parentBitSet.get(currentDocID) == false;
                }

                @Override
                public long cost() {
                    return (long) (originallyMatchedChildrenIterator.cost() * inverseParentPercentage);
                }
            };
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
            return originallyMatchedChildren.getMaxScore(upTo);
        }
    }
}
