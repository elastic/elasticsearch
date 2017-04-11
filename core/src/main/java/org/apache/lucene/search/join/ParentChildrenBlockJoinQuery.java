/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.search.join;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;

import java.io.IOException;
import java.util.Set;

/**
 * A query that returns all the matching child documents for a specific parent document
 * indexed together in the same block. The provided child query determines which matching
 * child doc is being returned.
 *
 */
// FORKED: backported from lucene 6.5 to ES, because lucene 6.4 doesn't have this query
public class ParentChildrenBlockJoinQuery extends Query {

    private final BitSetProducer parentFilter;
    private final Query childQuery;
    private final int parentDocId;

    /**
     * Creates a <code>ParentChildrenBlockJoinQuery</code> instance
     *
     * @param parentFilter  A filter identifying parent documents.
     * @param childQuery    A child query that determines which child docs are matching
     * @param parentDocId   The top level doc id of that parent to return children documents for
     */
    public ParentChildrenBlockJoinQuery(BitSetProducer parentFilter, Query childQuery, int parentDocId) {
        this.parentFilter = parentFilter;
        this.childQuery = childQuery;
        this.parentDocId = parentDocId;
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        ParentChildrenBlockJoinQuery other = (ParentChildrenBlockJoinQuery) obj;
        return parentFilter.equals(other.parentFilter)
            && childQuery.equals(other.childQuery)
            && parentDocId == other.parentDocId;
    }

    @Override
    public int hashCode() {
        int hash = classHash();
        hash = 31 * hash + parentFilter.hashCode();
        hash = 31 * hash +  childQuery.hashCode();
        hash = 31 * hash + parentDocId;
        return hash;
    }

    @Override
    public String toString(String field) {
        return "ParentChildrenBlockJoinQuery (" + childQuery + ")";
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        final Query childRewrite = childQuery.rewrite(reader);
        if (childRewrite != childQuery) {
            return new ParentChildrenBlockJoinQuery(parentFilter, childRewrite, parentDocId);
        } else {
            return super.rewrite(reader);
        }
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        final Weight childWeight = childQuery.createWeight(searcher, needsScores);
        final int readerIndex = ReaderUtil.subIndex(parentDocId, searcher.getIndexReader().leaves());
        return new Weight(this) {

            @Override
            public void extractTerms(Set<Term> terms) {
                childWeight.extractTerms(terms);
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                return Explanation.noMatch("Not implemented, use ToParentBlockJoinQuery explain why a document matched");
            }

            @Override
            public float getValueForNormalization() throws IOException {
                return childWeight.getValueForNormalization();
            }

            @Override
            public void normalize(float norm, float boost) {
                childWeight.normalize(norm, boost);
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                // Childs docs only reside in a single segment, so no need to evaluate all segments
                if (context.ord != readerIndex) {
                    return null;
                }

                final int localParentDocId = parentDocId - context.docBase;
                // If parentDocId == 0 then a parent doc doesn't have child docs, because child docs are stored
                // before the parent doc and because parent doc is 0 we can safely assume that there are no child docs.
                if (localParentDocId == 0) {
                    return null;
                }

                final BitSet parents = parentFilter.getBitSet(context);
                final int firstChildDocId = parents.prevSetBit(localParentDocId - 1) + 1;
                // A parent doc doesn't have child docs, so we can early exit here:
                if (firstChildDocId == localParentDocId) {
                    return null;
                }

                final Scorer childrenScorer = childWeight.scorer(context);
                if (childrenScorer == null) {
                    return null;
                }
                DocIdSetIterator childrenIterator = childrenScorer.iterator();
                final DocIdSetIterator it = new DocIdSetIterator() {

                    int doc = -1;

                    @Override
                    public int docID() {
                        return doc;
                    }

                    @Override
                    public int nextDoc() throws IOException {
                        return advance(doc + 1);
                    }

                    @Override
                    public int advance(int target) throws IOException {
                        target = Math.max(firstChildDocId, target);
                        if (target >= localParentDocId) {
                            // We're outside the child nested scope, so it is done
                            return doc = NO_MORE_DOCS;
                        } else {
                            int advanced = childrenIterator.advance(target);
                            if (advanced >= localParentDocId) {
                                // We're outside the child nested scope, so it is done
                                return doc = NO_MORE_DOCS;
                            } else {
                                return doc = advanced;
                            }
                        }
                    }

                    @Override
                    public long cost() {
                        return Math.min(childrenIterator.cost(), localParentDocId - firstChildDocId);
                    }

                };
                return new Scorer(this) {
                    @Override
                    public int docID() {
                        return it.docID();
                    }

                    @Override
                    public float score() throws IOException {
                        return childrenScorer.score();
                    }

                    @Override
                    public int freq() throws IOException {
                        return childrenScorer.freq();
                    }

                    @Override
                    public DocIdSetIterator iterator() {
                        return it;
                    }
                };
            }
        };
    }
}
