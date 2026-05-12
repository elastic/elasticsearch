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
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;

import java.util.Arrays;
import java.util.Objects;

/**
 * A {@link Query} that matches every document <em>except</em> a fixed set of excluded ones.
 * <p>
 * This is used on the HNSW kNN retry path: when a search round does not yield enough hits
 * passing the post-filter, we retry the graph traversal while telling the vector reader to
 * skip the docs we have already collected. This query produces the "everything except these"
 * accept-docs bitset that drives that retry.
 * <p>
 * The excluded IDs are stored as a sorted {@code int[]} of <em>global</em> doc IDs (IDs in the
 * top-level {@link IndexReader}'s space, not per-segment). At search time each leaf segment
 * translates that into its own local space:
 * <ol>
 *   <li>Binary-search the excluded array to find the slice that falls inside the leaf's
 *       {@code [docBase, docBase + maxDoc)} range.</li>
 *   <li>Allocate a leaf-sized {@link FixedBitSet}, set every bit, then clear only the bits
 *       for the excluded docs. The excluded set is expected to be much smaller than the
 *       number of docs in the leaf, so set-all-then-clear is cheaper than building an accept
 *       list bit by bit.</li>
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
                int leafMaxDoc = context.reader().maxDoc();
                int docBase = context.docBase;
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

                // Build the accept bitset by starting from "all accepted" and clearing only
                // the excluded positions (translated from global to leaf-local).
                int excludedCount = to - from;
                FixedBitSet leafBits = new FixedBitSet(leafMaxDoc);
                leafBits.set(0, leafMaxDoc);
                for (int i = from; i < to; i++) {
                    leafBits.clear(excludedDocs[i] - docBase);
                }
                int cardinality = leafMaxDoc - excludedCount;
                if (cardinality == 0) {
                    return null;
                }
                return new DefaultScorerSupplier(
                    new ConstantScoreScorer(0f, ScoreMode.COMPLETE_NO_SCORES, new BitSetIterator(leafBits, cardinality))
                );
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
