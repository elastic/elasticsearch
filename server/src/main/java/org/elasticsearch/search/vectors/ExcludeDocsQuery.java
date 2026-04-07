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

import java.util.Objects;

/**
 * A query that excludes specific global doc IDs. Used by HNSW retry to prevent
 * re-visiting documents seen in previous rounds.
 * <p>
 * Pre-computes an accept bitset at construction (all docs minus excluded),
 * then per-leaf extracts the local slice as a BitSetIterator.
 */
class ExcludeDocsQuery extends Query {
    private final FixedBitSet acceptDocs;
    private final Object readerContextId;

    ExcludeDocsQuery(FixedBitSet excludedDocs, IndexReader reader) {
        int maxDoc = reader.maxDoc();
        this.acceptDocs = new FixedBitSet(maxDoc);
        acceptDocs.set(0, maxDoc);
        acceptDocs.andNot(excludedDocs);
        this.readerContextId = reader.getContext().id();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new Weight(this) {
            @Override
            public Explanation explain(LeafReaderContext context, int doc) {
                return Explanation.noMatch("exclude docs filter");
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) {
                int maxDoc = context.reader().maxDoc();
                int docBase = context.docBase;
                FixedBitSet leafBits = new FixedBitSet(maxDoc);
                int end = docBase + maxDoc;
                for (int globalDoc = acceptDocs.nextSetBit(docBase); globalDoc != -1 && globalDoc < end;) {
                    leafBits.set(globalDoc - docBase);
                    int next = globalDoc + 1;
                    if (next >= acceptDocs.length()) {
                        break;
                    }
                    globalDoc = acceptDocs.nextSetBit(next);
                }
                int cardinality = leafBits.cardinality();
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
        return "ExcludeDocsQuery[accepted=" + acceptDocs.cardinality() + "]";
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
        return acceptDocs.equals(that.acceptDocs) && readerContextId == that.readerContextId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), acceptDocs, readerContextId);
    }
}
