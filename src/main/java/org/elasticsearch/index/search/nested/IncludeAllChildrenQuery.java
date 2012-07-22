package org.elasticsearch.index.search.nested;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.docset.FixedBitDocSet;

import java.io.IOException;
import java.util.Set;

/**
 * A special query that accepts a top level parent matching query, and returns all the children of that parent as
 * well. This is handy when deleting by query.
 */
public class IncludeAllChildrenQuery extends Query {


    private final Filter parentFilter;
    private final Query parentQuery;

    // If we are rewritten, this is the original childQuery we
    // were passed; we use this for .equals() and
    // .hashCode().  This makes rewritten query equal the
    // original, so that user does not have to .rewrite() their
    // query before searching:
    private final Query origParentQuery;


    public IncludeAllChildrenQuery(Query parentQuery, Filter parentFilter) {
        this.origParentQuery = parentQuery;
        this.parentQuery = parentQuery;
        this.parentFilter = parentFilter;
    }

    IncludeAllChildrenQuery(Query origParentQuery, Query parentQuery, Filter parentFilter) {
        this.origParentQuery = origParentQuery;
        this.parentQuery = parentQuery;
        this.parentFilter = parentFilter;
    }

    @Override
    public Weight createWeight(Searcher searcher) throws IOException {
        return new IncludeAllChildrenWeight(parentQuery, parentQuery.createWeight(searcher), parentFilter);
    }

    static class IncludeAllChildrenWeight extends Weight {

        private final Query parentQuery;
        private final Weight parentWeight;
        private final Filter parentsFilter;

        IncludeAllChildrenWeight(Query parentQuery, Weight parentWeight, Filter parentsFilter) {
            this.parentQuery = parentQuery;
            this.parentWeight = parentWeight;
            this.parentsFilter = parentsFilter;
        }

        @Override
        public Query getQuery() {
            return parentQuery;
        }

        @Override
        public float getValue() {
            return parentWeight.getValue();
        }

        @Override
        public float sumOfSquaredWeights() throws IOException {
            return parentWeight.sumOfSquaredWeights() * parentQuery.getBoost() * parentQuery.getBoost();
        }

        @Override
        public void normalize(float norm) {
            parentWeight.normalize(norm * parentQuery.getBoost());
        }

        @Override
        public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) throws IOException {
            final Scorer parentScorer = parentWeight.scorer(reader, true, false);

            // no matches
            if (parentScorer == null) {
                return null;
            }

            final int firstParentDoc = parentScorer.nextDoc();
            if (firstParentDoc == DocIdSetIterator.NO_MORE_DOCS) {
                // No matches
                return null;
            }

            DocIdSet parents = parentsFilter.getDocIdSet(reader);
            if (parents == null) {
                // No matches
                return null;
            }
            if (parents instanceof FixedBitDocSet) {
                parents = ((FixedBitDocSet) parents).set();
            }
            if (!(parents instanceof FixedBitSet)) {
                throw new IllegalStateException("parentFilter must return OpenBitSet; got " + parents);
            }


            return new IncludeAllChildrenScorer(this, parentScorer, (FixedBitSet) parents, firstParentDoc);
        }

        @Override
        public Explanation explain(IndexReader reader, int doc) throws IOException {
            return null;
        }

        @Override
        public boolean scoresDocsOutOfOrder() {
            return false;
        }
    }

    static class IncludeAllChildrenScorer extends Scorer {

        private final Scorer parentScorer;
        private final FixedBitSet parentBits;

        private int currentChildPointer = -1;
        private int currentParentPointer = -1;

        private int currentDoc = -1;

        IncludeAllChildrenScorer(Weight weight, Scorer parentScorer, FixedBitSet parentBits, int currentParentPointer) {
            super(weight);
            this.parentScorer = parentScorer;
            this.parentBits = parentBits;
            this.currentParentPointer = currentParentPointer;
            if (currentParentPointer == 0) {
                currentChildPointer = 0;
            } else {
                this.currentChildPointer = parentBits.prevSetBit(currentParentPointer - 1);
                if (currentChildPointer == -1) {
                    // no previous set parent, we delete from doc 0
                    currentChildPointer = 0;
                } else {
                    currentChildPointer++; // we only care about children
                }
            }

            currentDoc = currentChildPointer;
        }

        @Override
        public void visitSubScorers(Query parent, BooleanClause.Occur relationship, ScorerVisitor<Query, Query, Scorer> visitor) {
            super.visitSubScorers(parent, relationship, visitor);
            parentScorer.visitScorers(visitor);
        }

        @Override
        public int nextDoc() throws IOException {
            if (currentParentPointer == NO_MORE_DOCS) {
                return (currentDoc = NO_MORE_DOCS);
            }

            if (currentChildPointer == currentParentPointer) {
                // we need to return the current parent as well, but prepare to return
                // the next set of children
                currentDoc = currentParentPointer;
                currentParentPointer = parentScorer.nextDoc();
                if (currentParentPointer != NO_MORE_DOCS) {
                    currentChildPointer = parentBits.prevSetBit(currentParentPointer - 1);
                    if (currentChildPointer == -1) {
                        // no previous set parent, just set the child to the current parent
                        currentChildPointer = currentParentPointer;
                    } else {
                        currentChildPointer++; // we only care about children
                    }
                }
            } else {
                currentDoc = currentChildPointer++;
            }

            assert currentDoc != -1;
            return currentDoc;
        }

        @Override
        public int advance(int target) throws IOException {
            if (target == NO_MORE_DOCS) {
                return (currentDoc = NO_MORE_DOCS);
            }

            if (target == 0) {
                return nextDoc();
            }

            currentParentPointer = parentScorer.advance(target);
            if (currentParentPointer == NO_MORE_DOCS) {
                return (currentDoc = NO_MORE_DOCS);
            }
            if (currentParentPointer == 0) {
                currentChildPointer = 0;
            } else {
                currentChildPointer = parentBits.prevSetBit(currentParentPointer - 1);
                if (currentChildPointer == -1) {
                    // no previous set parent, just set the child to 0 to delete all up to the parent
                    currentChildPointer = 0;
                } else {
                    currentChildPointer++; // we only care about children
                }
            }

            currentDoc = currentChildPointer;

            return currentDoc;
        }

        @Override
        public float score() throws IOException {
            return parentScorer.score();
        }

        @Override
        public int docID() {
            return currentDoc;
        }
    }

    @Override
    public void extractTerms(Set<Term> terms) {
        parentQuery.extractTerms(terms);
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        final Query parentRewrite = parentQuery.rewrite(reader);
        if (parentRewrite != parentQuery) {
            Query rewritten = new IncludeAllChildrenQuery(parentQuery, parentRewrite, parentFilter);
            rewritten.setBoost(getBoost());
            return rewritten;
        } else {
            return this;
        }
    }

    @Override
    public String toString(String field) {
        return "IncludeAllChildrenQuery (" + parentQuery.toString() + ")";
    }

    @Override
    public boolean equals(Object _other) {
        if (_other instanceof IncludeAllChildrenQuery) {
            final IncludeAllChildrenQuery other = (IncludeAllChildrenQuery) _other;
            return origParentQuery.equals(other.origParentQuery) &&
                    parentFilter.equals(other.parentFilter);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int hash = 1;
        hash = prime * hash + origParentQuery.hashCode();
        hash = prime * hash + parentFilter.hashCode();
        return hash;
    }

    @Override
    public Object clone() {
        return new IncludeAllChildrenQuery((Query) origParentQuery.clone(),
                parentFilter);
    }
}
