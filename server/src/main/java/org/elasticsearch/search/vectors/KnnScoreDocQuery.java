/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * A query that matches the provided docs with their scores.
 *
 * Note: this query was adapted from Lucene's DocAndScoreQuery from the class
 * {@link org.apache.lucene.search.KnnFloatVectorQuery}, which is package-private.
 * There are no changes to the behavior, just some renames.
 */
public class KnnScoreDocQuery extends Query {
    private final int[] docs;
    private final float[] scores;

    // the indexes in docs and scores corresponding to the first matching document in each segment.
    // If a segment has no matching documents, it should be assigned the index of the next segment that does.
    // There should be a final entry that is always docs.length-1.
    private final int[] segmentStarts;

    // an object identifying the reader context that was used to build this query
    private final Object contextIdentity;

    /**
     * Creates a query.
     *
     * @param docs the global doc IDs of documents that match, in ascending order
     * @param scores the scores of the matching documents
     * @param reader IndexReader
     */
    KnnScoreDocQuery(int[] docs, float[] scores, IndexReader reader) {
        this.docs = docs;
        this.scores = scores;
        this.segmentStarts = findSegmentStarts(reader, docs);
        this.contextIdentity = reader.getContext().id();
    }

    private static int[] findSegmentStarts(IndexReader reader, int[] docs) {
        int[] starts = new int[reader.leaves().size() + 1];
        starts[starts.length - 1] = docs.length;
        if (starts.length == 2) {
            return starts;
        }
        int resultIndex = 0;
        for (int i = 1; i < starts.length - 1; i++) {
            int upper = reader.leaves().get(i).docBase;
            resultIndex = Arrays.binarySearch(docs, resultIndex, docs.length, upper);
            if (resultIndex < 0) {
                resultIndex = -1 - resultIndex;
            }
            starts[i] = resultIndex;
        }
        return starts;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        if (docs.length == 0) {
            return new MatchNoDocsQuery();
        }
        return this;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        if (searcher.getIndexReader().getContext().id() != contextIdentity) {
            throw new IllegalStateException("This KnnScoreDocQuery was created by a different reader");
        }
        return new Weight(this) {

            @Override
            public int count(LeafReaderContext context) {
                return segmentStarts[context.ord + 1] - segmentStarts[context.ord];
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) {
                int found = Arrays.binarySearch(docs, doc + context.docBase);
                if (found < 0) {
                    return Explanation.noMatch("not in top k documents");
                }
                return Explanation.match(scores[found] * boost, "within top k documents");
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                // Segment starts indicate how many docs are in the segment,
                // upper equalling lower indicates no documents for this segment
                if (segmentStarts[context.ord] == segmentStarts[context.ord + 1]) {
                    return null;
                }
                Scorer scorer = new Scorer() {
                    final int lower = segmentStarts[context.ord];
                    final int upper = segmentStarts[context.ord + 1];
                    int upTo = -1;

                    @Override
                    public DocIdSetIterator iterator() {
                        return new DocIdSetIterator() {
                            @Override
                            public int docID() {
                                return currentDocId();
                            }

                            @Override
                            public int nextDoc() {
                                if (upTo == -1) {
                                    upTo = lower;
                                } else {
                                    ++upTo;
                                }
                                return currentDocId();
                            }

                            @Override
                            public int advance(int target) throws IOException {
                                return slowAdvance(target);
                            }

                            @Override
                            public long cost() {
                                return upper - lower;
                            }
                        };
                    }

                    @Override
                    public float getMaxScore(int docId) {
                        // NO_MORE_DOCS indicates the maximum score for all docs in this segment
                        // Anything less than must be accounted for via the docBase.
                        if (docId != NO_MORE_DOCS) {
                            docId += context.docBase;
                        }
                        float maxScore = 0;
                        for (int idx = Math.max(lower, upTo); idx < upper && docs[idx] <= docId; idx++) {
                            maxScore = Math.max(maxScore, scores[idx] * boost);
                        }
                        return maxScore;
                    }

                    @Override
                    public float score() {
                        return scores[upTo] * boost;
                    }

                    @Override
                    public int advanceShallow(int docId) {
                        int start = Math.max(upTo, lower);
                        int docIdIndex = Arrays.binarySearch(docs, start, upper, docId + context.docBase);
                        if (docIdIndex < 0) {
                            docIdIndex = -1 - docIdIndex;
                        }
                        if (docIdIndex >= upper) {
                            return NO_MORE_DOCS;
                        }
                        return docs[docIdIndex];
                    }

                    @Override
                    public int docID() {
                        return currentDocId();
                    }

                    private int currentDocId() {
                        if (upTo == -1) {
                            return -1;
                        }
                        if (upTo >= upper) {
                            return NO_MORE_DOCS;
                        }
                        return docs[upTo] - context.docBase;
                    }

                };
                return new DefaultScorerSupplier(scorer);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }
        };
    }

    // visible for testing
    int[] docs() {
        return docs;
    }

    // visible for testing
    float[] scores() {
        return scores;
    }

    @Override
    public String toString(String field) {
        return "ScoreAndDocQuery";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        return Arrays.equals(docs, ((KnnScoreDocQuery) obj).docs)
            && Arrays.equals(scores, ((KnnScoreDocQuery) obj).scores)
            && Arrays.equals(segmentStarts, ((KnnScoreDocQuery) obj).segmentStarts)
            && contextIdentity == ((KnnScoreDocQuery) obj).contextIdentity;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), Arrays.hashCode(docs), Arrays.hashCode(scores), Arrays.hashCode(segmentStarts), contextIdentity);
    }
}
