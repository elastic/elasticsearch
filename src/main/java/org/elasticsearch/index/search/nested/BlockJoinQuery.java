/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.docset.FixedBitDocSet;
import org.elasticsearch.common.lucene.search.NoopCollector;

import java.io.IOException;
import java.util.Locale;
import java.util.Set;

/**
 * This query requires that you index
 * children and parent docs as a single block, using the
 * {@link org.apache.lucene.index.IndexWriter#addDocuments} or {@link
 * org.apache.lucene.index.IndexWriter#updateDocuments} API.  In each block, the
 * child documents must appear first, ending with the parent
 * document.  At search time you provide a Filter
 * identifying the parents, however this Filter must provide
 * an {@link org.apache.lucene.util.FixedBitSet} per sub-reader.
 * <p/>
 * <p>Once the block index is built, use this query to wrap
 * any sub-query matching only child docs and join matches in that
 * child document space up to the parent document space.
 * You can then use this Query as a clause with
 * other queries in the parent document space.</p>
 * <p/>
 * <p>The child documents must be orthogonal to the parent
 * documents: the wrapped child query must never
 * return a parent document.</p>
 * <p/>
 * If you'd like to retrieve {@link TopGroups} for the
 * resulting query, use the {@link BlockJoinCollector}.
 * Note that this is not necessary, ie, if you simply want
 * to collect the parent documents and don't need to see
 * which child documents matched under that parent, then
 * you can use any collector.
 * <p/>
 * <p><b>NOTE</b>: If the overall query contains parent-only
 * matches, for example you OR a parent-only query with a
 * joined child-only query, then the resulting collected documents
 * will be correct, however the {@link TopGroups} you get
 * from {@link BlockJoinCollector} will not contain every
 * child for parents that had matched.
 * <p/>
 * <p>See {@link org.apache.lucene.search.join} for an
 * overview. </p>
 *
 * @lucene.experimental
 */

// LUCENE MONITOR: Track CHANGE
public class BlockJoinQuery extends Query {

    public static enum ScoreMode {None, Avg, Max, Total}

    ;

    private final Filter parentsFilter;
    private final Query childQuery;

    private Collector childCollector = NoopCollector.NOOP_COLLECTOR;

    public BlockJoinQuery setCollector(Collector collector) {
        this.childCollector = collector;
        return this;
    }

    // If we are rewritten, this is the original childQuery we
    // were passed; we use this for .equals() and
    // .hashCode().  This makes rewritten query equal the
    // original, so that user does not have to .rewrite() their
    // query before searching:
    private final Query origChildQuery;
    private final ScoreMode scoreMode;

    public BlockJoinQuery(Query childQuery, Filter parentsFilter, ScoreMode scoreMode) {
        super();
        this.origChildQuery = childQuery;
        this.childQuery = childQuery;
        this.parentsFilter = parentsFilter;
        this.scoreMode = scoreMode;
    }

    private BlockJoinQuery(Query origChildQuery, Query childQuery, Filter parentsFilter, ScoreMode scoreMode) {
        super();
        this.origChildQuery = origChildQuery;
        this.childQuery = childQuery;
        this.parentsFilter = parentsFilter;
        this.scoreMode = scoreMode;
    }

    @Override
    public Weight createWeight(Searcher searcher) throws IOException {
        return new BlockJoinWeight(this, childQuery.createWeight(searcher), parentsFilter, scoreMode, childCollector);
    }

    private static class BlockJoinWeight extends Weight {
        private final Query joinQuery;
        private final Weight childWeight;
        private final Filter parentsFilter;
        private final ScoreMode scoreMode;
        private final Collector childCollector;

        public BlockJoinWeight(Query joinQuery, Weight childWeight, Filter parentsFilter, ScoreMode scoreMode, Collector childCollector) {
            super();
            this.joinQuery = joinQuery;
            this.childWeight = childWeight;
            this.parentsFilter = parentsFilter;
            this.scoreMode = scoreMode;
            this.childCollector = childCollector;
        }

        @Override
        public Query getQuery() {
            return joinQuery;
        }

        @Override
        public float getValue() {
            return childWeight.getValue();
        }

        @Override
        public float sumOfSquaredWeights() throws IOException {
            return childWeight.sumOfSquaredWeights() * joinQuery.getBoost() * joinQuery.getBoost();
        }

        @Override
        public void normalize(float norm) {
            childWeight.normalize(norm * joinQuery.getBoost());
        }

        @Override
        public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) throws IOException {
            // Pass scoreDocsInOrder true, topScorer false to our sub:
            final Scorer childScorer = childWeight.scorer(reader, true, false);

            if (childScorer == null) {
                // No matches
                return null;
            }

            final int firstChildDoc = childScorer.nextDoc();
            if (firstChildDoc == DocIdSetIterator.NO_MORE_DOCS) {
                // No matches
                return null;
            }

            DocIdSet parents = parentsFilter.getDocIdSet(reader);
            // TODO NESTED: We have random access in ES, not sure I understand what can be gain?
            // TODO: once we do random-access filters we can
            // generalize this:
            if (parents == null) {
                // No matches
                return null;
            }
            // CHANGE:
            if (parents instanceof FixedBitDocSet) {
                parents = ((FixedBitDocSet) parents).set();
            }
            if (!(parents instanceof FixedBitSet)) {
                throw new IllegalStateException("parentFilter must return OpenBitSet; got " + parents);
            }

            // CHANGE:
            if (childCollector != null) {
                childCollector.setNextReader(reader, 0);
                childCollector.setScorer(childScorer);
            }

            return new BlockJoinScorer(this, childScorer, (FixedBitSet) parents, firstChildDoc, scoreMode, childCollector);
        }

        @Override
        public Explanation explain(IndexReader reader, int doc) throws IOException {
            BlockJoinScorer scorer = (BlockJoinScorer) scorer(reader, true, false);
            if (scorer != null) {
                if (scorer.advance(doc) == doc) {
                    return scorer.explain();
                }
            }
            return new ComplexExplanation(false, 0.0f, "Not a match");
        }

        @Override
        public boolean scoresDocsOutOfOrder() {
            return false;
        }
    }

    static class BlockJoinScorer extends Scorer {
        private final Scorer childScorer;
        private final FixedBitSet parentBits;
        private final ScoreMode scoreMode;
        private final Collector childCollector;
        private int prevParentDoc;
        private int parentDoc = -1;
        private float parentScore;
        private int nextChildDoc;

        private int[] pendingChildDocs = new int[5];
        private float[] pendingChildScores;
        private int childDocUpto;

        public BlockJoinScorer(Weight weight, Scorer childScorer, FixedBitSet parentBits, int firstChildDoc, ScoreMode scoreMode, Collector childCollector) {
            super(weight);
            //System.out.println("Q.init firstChildDoc=" + firstChildDoc);
            this.parentBits = parentBits;
            this.childScorer = childScorer;
            this.scoreMode = scoreMode;
            this.childCollector = childCollector;
            if (scoreMode != ScoreMode.None) {
                pendingChildScores = new float[5];
            }
            nextChildDoc = firstChildDoc;
        }

        @Override
        public void visitSubScorers(Query parent, BooleanClause.Occur relationship,
                                    ScorerVisitor<Query, Query, Scorer> visitor) {
            super.visitSubScorers(parent, relationship, visitor);
            //childScorer.visitSubScorers(weight.getQuery(), BooleanClause.Occur.MUST, visitor);
            childScorer.visitScorers(visitor);
        }

        int getChildCount() {
            return childDocUpto;
        }

        int[] swapChildDocs(int[] other) {
            final int[] ret = pendingChildDocs;
            if (other == null) {
                pendingChildDocs = new int[5];
            } else {
                pendingChildDocs = other;
            }
            return ret;
        }

        float[] swapChildScores(float[] other) {
            if (scoreMode == ScoreMode.None) {
                throw new IllegalStateException("ScoreMode is None");
            }
            final float[] ret = pendingChildScores;
            if (other == null) {
                pendingChildScores = new float[5];
            } else {
                pendingChildScores = other;
            }
            return ret;
        }

        @Override
        public int nextDoc() throws IOException {
            //System.out.println("Q.nextDoc() nextChildDoc=" + nextChildDoc);

            if (nextChildDoc == NO_MORE_DOCS) {
                //System.out.println("  end");
                return parentDoc = NO_MORE_DOCS;
            }

            // Gather all children sharing the same parent as nextChildDoc
            parentDoc = parentBits.nextSetBit(nextChildDoc);
            //System.out.println("  parentDoc=" + parentDoc);
            assert parentDoc != -1;

            float totalScore = 0;
            float maxScore = Float.NEGATIVE_INFINITY;

            childDocUpto = 0;
            do {
                //System.out.println("  c=" + nextChildDoc);
                if (pendingChildDocs.length == childDocUpto) {
                    pendingChildDocs = ArrayUtil.grow(pendingChildDocs);
                    if (scoreMode != ScoreMode.None) {
                        pendingChildScores = ArrayUtil.grow(pendingChildScores);
                    }
                }
                pendingChildDocs[childDocUpto] = nextChildDoc;
                if (scoreMode != ScoreMode.None) {
                    // TODO: specialize this into dedicated classes per-scoreMode
                    final float childScore = childScorer.score();
                    pendingChildScores[childDocUpto] = childScore;
                    maxScore = Math.max(childScore, maxScore);
                    totalScore += childScore;
                }

                // CHANGE:
                childCollector.collect(nextChildDoc);

                childDocUpto++;
                nextChildDoc = childScorer.nextDoc();
            } while (nextChildDoc < parentDoc);
            //System.out.println("  nextChildDoc=" + nextChildDoc);

            // Parent & child docs are supposed to be orthogonal:
            assert nextChildDoc != parentDoc;

            switch (scoreMode) {
                case Avg:
                    parentScore = totalScore / childDocUpto;
                    break;
                case Max:
                    parentScore = maxScore;
                    break;
                case Total:
                    parentScore = totalScore;
                    break;
                case None:
                    break;
            }

            //System.out.println("  return parentDoc=" + parentDoc);
            return parentDoc;
        }

        @Override
        public int docID() {
            return parentDoc;
        }

        @Override
        public float score() throws IOException {
            return parentScore;
        }

        @Override
        public int advance(int parentTarget) throws IOException {

            //System.out.println("Q.advance parentTarget=" + parentTarget);
            if (parentTarget == NO_MORE_DOCS) {
                return parentDoc = NO_MORE_DOCS;
            }

            if (parentTarget == 0) {
                // Callers should only be passing in a docID from
                // the parent space, so this means this parent
                // has no children (it got docID 0), so it cannot
                // possibly match.  We must handle this case
                // separately otherwise we pass invalid -1 to
                // prevSetBit below:
                return nextDoc();
            }

            prevParentDoc = parentBits.prevSetBit(parentTarget - 1);

            //System.out.println("  rolled back to prevParentDoc=" + prevParentDoc + " vs parentDoc=" + parentDoc);
            assert prevParentDoc >= parentDoc;
            if (prevParentDoc > nextChildDoc) {
                nextChildDoc = childScorer.advance(prevParentDoc);
                // System.out.println("  childScorer advanced to child docID=" + nextChildDoc);
                //} else {
                //System.out.println("  skip childScorer advance");
            }

            // Parent & child docs are supposed to be orthogonal:
            assert nextChildDoc != prevParentDoc;

            final int nd = nextDoc();
            //System.out.println("  return nextParentDoc=" + nd);
            return nd;
        }

        public Explanation explain() throws IOException {
            int start = prevParentDoc + 1; // +1 b/c prevParentDoc is previous parent doc
            int end = parentDoc - 1; // -1 b/c parentDoc is parent doc
            ComplexExplanation explanation = new ComplexExplanation(
                    true, score(), String.format(Locale.ROOT, "Score based on score mode %s and child doc range from %d to %d", scoreMode, start, end)
            );

            for (int i = 0; i < childDocUpto; i++) {
                int childDoc = pendingChildDocs[i];
                float childScore = pendingChildScores[i];
                explanation.addDetail(new Explanation(childScore, String.format(Locale.ROOT, "Child[%d]", childDoc)));
            }

            return explanation;
        }

    }

    @Override
    public void extractTerms(Set<Term> terms) {
        childQuery.extractTerms(terms);
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        final Query childRewrite = childQuery.rewrite(reader);
        if (childRewrite != childQuery) {
            Query rewritten = new BlockJoinQuery(childQuery,
                    childRewrite,
                    parentsFilter,
                    scoreMode).setCollector(childCollector);
            rewritten.setBoost(getBoost());
            return rewritten;
        } else {
            return this;
        }
    }

    @Override
    public String toString(String field) {
        return "BlockJoinQuery (" + childQuery.toString() + ")";
    }

    @Override
    public boolean equals(Object _other) {
        if (_other instanceof BlockJoinQuery) {
            final BlockJoinQuery other = (BlockJoinQuery) _other;
            return origChildQuery.equals(other.origChildQuery) &&
                    parentsFilter.equals(other.parentsFilter) &&
                    scoreMode == other.scoreMode;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int hash = 1;
        hash = prime * hash + origChildQuery.hashCode();
        hash = prime * hash + scoreMode.hashCode();
        hash = prime * hash + parentsFilter.hashCode();
        return hash;
    }

    @Override
    public Object clone() {
        return new BlockJoinQuery((Query) origChildQuery.clone(),
                parentsFilter,
                scoreMode).setCollector(childCollector);
    }
}
