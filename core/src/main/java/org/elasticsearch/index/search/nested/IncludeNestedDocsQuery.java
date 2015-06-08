/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.search.nested;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.join.BitDocIdSetFilter;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

/**
 * A special query that accepts a top level parent matching query, and returns the nested docs of the matching parent
 * doc as well. This is handy when deleting by query, don't use it for other purposes.
 *
 * @elasticsearch.internal
 */
public class IncludeNestedDocsQuery extends Query {

    private final BitDocIdSetFilter parentFilter;
    private final Query parentQuery;

    // If we are rewritten, this is the original childQuery we
    // were passed; we use this for .equals() and
    // .hashCode().  This makes rewritten query equal the
    // original, so that user does not have to .rewrite() their
    // query before searching:
    private final Query origParentQuery;


    public IncludeNestedDocsQuery(Query parentQuery, BitDocIdSetFilter parentFilter) {
        this.origParentQuery = parentQuery;
        this.parentQuery = parentQuery;
        this.parentFilter = parentFilter;
    }

    // For rewriting
    IncludeNestedDocsQuery(Query rewrite, Query originalQuery, IncludeNestedDocsQuery previousInstance) {
        this.origParentQuery = originalQuery;
        this.parentQuery = rewrite;
        this.parentFilter = previousInstance.parentFilter;
        setBoost(previousInstance.getBoost());
    }

    // For cloning
    IncludeNestedDocsQuery(Query originalQuery, IncludeNestedDocsQuery previousInstance) {
        this.origParentQuery = originalQuery;
        this.parentQuery = originalQuery;
        this.parentFilter = previousInstance.parentFilter;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        return new IncludeNestedDocsWeight(this, parentQuery, parentQuery.createWeight(searcher, needsScores), parentFilter);
    }

    static class IncludeNestedDocsWeight extends Weight {

        private final Query parentQuery;
        private final Weight parentWeight;
        private final BitDocIdSetFilter parentsFilter;

        IncludeNestedDocsWeight(Query query, Query parentQuery, Weight parentWeight, BitDocIdSetFilter parentsFilter) {
            super(query);
            this.parentQuery = parentQuery;
            this.parentWeight = parentWeight;
            this.parentsFilter = parentsFilter;
        }

        @Override
        public void extractTerms(Set<Term> terms) {
            parentWeight.extractTerms(terms);
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
            parentWeight.normalize(norm, topLevelBoost);
        }

        @Override
        public float getValueForNormalization() throws IOException {
            return parentWeight.getValueForNormalization(); // this query is never boosted so just delegate...
        }

        @Override
        public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
            final Scorer parentScorer = parentWeight.scorer(context, acceptDocs);

            // no matches
            if (parentScorer == null) {
                return null;
            }

            BitDocIdSet parents = parentsFilter.getDocIdSet(context);
            if (parents == null) {
                // No matches
                return null;
            }

            int firstParentDoc = parentScorer.nextDoc();
            if (firstParentDoc == DocIdSetIterator.NO_MORE_DOCS) {
                // No matches
                return null;
            }
            return new IncludeNestedDocsScorer(this, parentScorer, parents, firstParentDoc);
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return null; //Query is used internally and not by users, so explain can be empty
        }
    }

    static class IncludeNestedDocsScorer extends Scorer {

        final Scorer parentScorer;
        final BitSet parentBits;

        int currentChildPointer = -1;
        int currentParentPointer = -1;
        int currentDoc = -1;

        IncludeNestedDocsScorer(Weight weight, Scorer parentScorer, BitDocIdSet parentBits, int currentParentPointer) {
            super(weight);
            this.parentScorer = parentScorer;
            this.parentBits = parentBits.bits();
            this.currentParentPointer = currentParentPointer;
            if (currentParentPointer == 0) {
                currentChildPointer = 0;
            } else {
                this.currentChildPointer = this.parentBits.prevSetBit(currentParentPointer - 1);
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
        public Collection<ChildScorer> getChildren() {
            return parentScorer.getChildren();
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

            if (target < currentParentPointer) {
                currentDoc = currentParentPointer = parentScorer.advance(target);
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
            } else {
                currentDoc = currentChildPointer++;
            }

            return currentDoc;
        }

        @Override
        public float score() throws IOException {
            return parentScorer.score();
        }

        @Override
        public int freq() throws IOException {
            return parentScorer.freq();
        }

        @Override
        public int docID() {
            return currentDoc;
        }

        @Override
        public long cost() {
            return parentScorer.cost();
        }
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        final Query parentRewrite = parentQuery.rewrite(reader);
        if (parentRewrite != parentQuery) {
            return new IncludeNestedDocsQuery(parentRewrite, parentQuery, this);
        } else {
            return this;
        }
    }

    @Override
    public String toString(String field) {
        return "IncludeNestedDocsQuery (" + parentQuery.toString() + ")";
    }

    @Override
    public boolean equals(Object _other) {
        if (_other instanceof IncludeNestedDocsQuery) {
            final IncludeNestedDocsQuery other = (IncludeNestedDocsQuery) _other;
            return origParentQuery.equals(other.origParentQuery) && parentFilter.equals(other.parentFilter);
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
    public Query clone() {
        Query clonedQuery = origParentQuery.clone();
        return new IncludeNestedDocsQuery(clonedQuery, this);
    }
}
