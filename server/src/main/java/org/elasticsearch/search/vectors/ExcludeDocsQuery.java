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
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * A {@link Query} that matches every document <em>except</em> a fixed set of excluded ones.
 * <p>
 * The excluded IDs are stored as a sorted {@code int[]} of <em>global</em> doc IDs (IDs in the
 * top-level {@link IndexReader}'s space, not per-segment). At search time each leaf segment
 * translates that into its own local space:
 * <ol>
 *   <li>Binary-search the excluded array to find the slice that falls inside the leaf's
 *       {@code [docBase, docBase + maxDoc)} range.</li>
 *   <li>Expose a {@link DocIdSetIterator} that walks {@code [0, leafMaxDoc)} while a pointer
 *       steps through the excluded slice, skipping over excluded entries on the fly. No
 *       per-leaf bitset is allocated.</li>
 * </ol>
 * The query is pinned to a specific {@link IndexReader} via its context ID and refuses to run
 * against any other reader, since global doc IDs are only meaningful within the reader they
 * were collected from.
 */
class ExcludeDocsQuery extends Query {
    private final int[] excludedDocs;
    private final Object readerContextId;

    ExcludeDocsQuery(int[] excludedDocs, IndexReader reader) {
        this.excludedDocs = Objects.requireNonNull(excludedDocs);
        this.readerContextId = reader.getContext().id();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        if (searcher.getIndexReader().getContext().id() != readerContextId) {
            throw new IllegalStateException("This ExcludeDocsQuery was created by a different reader");
        }
        return new Weight(this) {
            @Override
            public Explanation explain(LeafReaderContext context, int doc) {
                int globalDoc = doc + context.docBase;
                if (Arrays.binarySearch(excludedDocs, globalDoc) >= 0) {
                    return Explanation.noMatch("excluded doc");
                }
                return Explanation.match(0f, "not excluded");
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) {
                final int leafMaxDoc = context.reader().maxDoc();
                final int docBase = context.docBase;
                int end = docBase + leafMaxDoc;

                // Locate the [from, to) slice of excludedDocs that intersects this leaf's
                // global range [docBase, end). Arrays.binarySearch returns the insertion
                // point encoded as -(insertion_point) - 1 when no exact match is found.
                int from = Arrays.binarySearch(excludedDocs, docBase);
                if (from < 0) {
                    from = -from - 1;
                }
                int to = Arrays.binarySearch(excludedDocs, from, excludedDocs.length, end);
                if (to < 0) {
                    to = -to - 1;
                }

                final int sliceFrom = from;
                final int sliceTo = to;
                final long cardinality = leafMaxDoc - (long) (sliceTo - sliceFrom);
                if (cardinality == 0) {
                    return null;
                }

                DocIdSetIterator iterator = new DocIdSetIterator() {
                    int doc = -1;
                    int excIdx = sliceFrom;

                    @Override
                    public int docID() {
                        return doc;
                    }

                    @Override
                    public int nextDoc() {
                        return advance(doc + 1);
                    }

                    @Override
                    public int advance(int target) {
                        while (excIdx < sliceTo && excludedDocs[excIdx] - docBase < target) {
                            excIdx++;
                        }
                        while (excIdx < sliceTo && excludedDocs[excIdx] - docBase == target) {
                            target++;
                            excIdx++;
                        }
                        if (target >= leafMaxDoc) {
                            return doc = NO_MORE_DOCS;
                        }
                        return doc = target;
                    }

                    @Override
                    public long cost() {
                        return cardinality;
                    }
                };

                return new DefaultScorerSupplier(new ConstantScoreScorer(0f, ScoreMode.COMPLETE_NO_SCORES, iterator));
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }

    @Override
    public String toString(String field) {
        return "ExcludeDocsQuery[count=" + excludedDocs.length + "]";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExcludeDocsQuery that = (ExcludeDocsQuery) o;
        return Arrays.equals(excludedDocs, that.excludedDocs) && readerContextId == that.readerContextId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), Arrays.hashCode(excludedDocs), readerContextId);
    }
}
