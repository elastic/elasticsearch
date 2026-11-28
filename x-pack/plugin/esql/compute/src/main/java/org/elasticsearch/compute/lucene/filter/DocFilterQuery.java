/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.filter;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Runs a {@link DocFilter} tree as a Lucene query, so a source operator reads the docs that pass it through its ordinary
 * {@link BulkScorer} path. The bulk scorer cuts the requested range into windows, lets the tree reject docs in each, and
 * hands the survivors to the collector as one {@code DocIdStream}.
 * <p>
 * A tree keeps per-segment state and is not thread safe, so every bulk scorer builds its own from {@code tree}. The
 * scorer does not close it: use trees whose {@link DocFilter#close()} releases nothing. A query never matches through
 * {@link ScorerSupplier#get}; only the bulk path is supported.
 */
public final class DocFilterQuery extends Query {
    private static final Scorable CONSTANT_SCORE = new Scorable() {
        @Override
        public float score() {
            return 1f;
        }
    };

    private final Supplier<DocFilter> tree;
    private final int windowSize;
    private final String description;

    public DocFilterQuery(Supplier<DocFilter> tree, int windowSize, String description) {
        if (windowSize <= 0) {
            throw new IllegalArgumentException("window size must be positive, got [" + windowSize + "]");
        }
        this.tree = tree;
        this.windowSize = windowSize;
        this.description = description;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) {
                int maxDoc = context.reader().maxDoc();
                return new ScorerSupplier() {
                    @Override
                    public Scorer get(long leadCost) {
                        throw new UnsupportedOperationException("a doc filter query only runs through its bulk scorer");
                    }

                    @Override
                    public BulkScorer bulkScorer() {
                        return new TreeBulkScorer(tree.get(), context, windowSize);
                    }

                    @Override
                    public long cost() {
                        return maxDoc;
                    }
                };
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public String toString(String field) {
        return "DocFilterQuery[" + description + "]";
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    private static final class TreeBulkScorer extends BulkScorer {
        private final DocFilter root;
        private final LeafReaderContext leaf;
        private final int windowSize;
        private final FixedBitSet inPlay;

        private TreeBulkScorer(DocFilter root, LeafReaderContext leaf, int windowSize) {
            this.root = root;
            this.leaf = leaf;
            this.windowSize = windowSize;
            this.inPlay = new FixedBitSet(windowSize);
        }

        @Override
        public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
            collector.setScorer(CONSTANT_SCORE);
            int maxDoc = leaf.reader().maxDoc();
            int end = Math.min(max, maxDoc);
            for (int from = min; from < end; from += windowSize) {
                DocWindow window = new DocWindow(leaf, from, Math.min(end, from + windowSize));
                DocFilterPass.putLiveDocsInPlay(window, acceptDocs, inPlay);
                root.filter(window, inPlay);
                collector.collect(new BitSetDocIdStream(inPlay, from));
            }
            return end >= maxDoc ? DocIdSetIterator.NO_MORE_DOCS : end;
        }

        @Override
        public long cost() {
            return leaf.reader().maxDoc();
        }
    }
}
