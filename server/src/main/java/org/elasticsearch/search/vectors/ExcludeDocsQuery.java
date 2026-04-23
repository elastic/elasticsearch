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
 * A query that excludes specific global doc IDs. Used by HNSW retry to prevent
 * re-visiting documents seen in previous rounds.
 * <p>
 * Stores a sorted {@code int[]} of excluded global doc IDs (typically 50-500 across retries).
 * Per-leaf, binary-searches for the leaf's range, then builds a dense accept bitset by
 * filling all bits and clearing only the excluded ones.
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

                int from = Arrays.binarySearch(excludedDocs, docBase);
                if (from < 0) {
                    from = -from - 1;
                }
                int to = Arrays.binarySearch(excludedDocs, from, excludedDocs.length, end);
                if (to < 0) {
                    to = -to - 1;
                }

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
