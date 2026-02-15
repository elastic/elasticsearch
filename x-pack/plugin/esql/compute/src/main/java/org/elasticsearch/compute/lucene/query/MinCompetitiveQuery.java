/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Infrastructure to provide {@link LeafCollector#competitiveIterator} from a
 * {@link SharedMinCompetitive}.
 */
public class MinCompetitiveQuery implements Releasable {
    private static final Logger log = LogManager.getLogger(MinCompetitiveQuery.class);

    public record Factory(SharedMinCompetitive.Supplier minCompetitive, BiFunction<ShardContext, Page, Query> queryFunction) {
        public MinCompetitiveQuery build(BlockFactory blockFactory) {
            return new MinCompetitiveQuery(blockFactory, minCompetitive.get(), queryFunction);
        }
    }

    private final BlockFactory blockFactory;
    private final SharedMinCompetitive minCompetitive;
    private final BiFunction<ShardContext, Page, Query> queryFunction;
    private PerIndex perIndex;
    private DocIdSetIterator disi;

    private MinCompetitiveQuery(
        BlockFactory blockFactory,
        SharedMinCompetitive minCompetitive,
        BiFunction<ShardContext, Page, Query> queryFunction
    ) {
        this.blockFactory = blockFactory;
        this.minCompetitive = minCompetitive;
        this.queryFunction = queryFunction;
    }

    /**
     * The actual {@link DocIdSetIterator} matching min_competitive docs.
     */
    public DocIdSetIterator disi() {
        return disi;
    }

    /**
     * Read the {@code min_competitive} from {@link SharedMinCompetitive} and
     * build a {@link DocIdSetIterator} for the index represented by {@code ctx}
     * and the segment represented by {@code leaf}. If the {@code min_competitive},
     * {@code ctx}, and {@code leaf} are the same as the last time this was called
     * then this doesn't change anything.
     * <p>
     *     Reading form {@link SharedMinCompetitive} is a volatile read.
     * </p>
     */
    public void update(ShardContext ctx, LeafReaderContext leaf) throws IOException {
        this.disi = updatedDisi(ctx, leaf);
    }

    private DocIdSetIterator updatedDisi(ShardContext ctx, LeafReaderContext leaf) throws IOException {
        return perIndex(ctx).perMinValue(minCompetitive.get(blockFactory)).perLeaf(leaf).disi();
    }

    private PerIndex perIndex(ShardContext ctx) {
        if (perIndex == null || perIndex.ctx != ctx) {
            perIndex = new PerIndex(ctx);
        }
        return perIndex;
    }

    @Override
    public void close() {
        minCompetitive.decRef();
    }

    private class PerIndex {
        private final ShardContext ctx;
        private PerMinValue perMinValue;

        private PerIndex(ShardContext ctx) {
            this.ctx = ctx;
        }

        public PerMinValue perMinValue(Page value) throws IOException {
            if (perMinValue == null) {
                perMinValue = newPerMinValue(value);
            } else if (Objects.equals(perMinValue.value, value) == false) {
                perMinValue.close();
                perMinValue = newPerMinValue(value);
            }
            return perMinValue;
        }

        private PerMinValue newPerMinValue(Page value) throws IOException {
            try {
                Query query = queryFunction.apply(ctx, value);
                log.debug("updating min competitive to {} {}", value, query);
                Weight weight = query.createWeight(ctx.searcher(), ScoreMode.COMPLETE_NO_SCORES, 0.0F);
                PerMinValue result = new PerMinValue(value, weight);
                value = null;
                return result;
            } finally {
                Releasables.close(value);
            }
        }
    }

    private class PerMinValue implements Releasable {
        /**
         * The min competitive value this was built for. Here {@code null} means
         * "no min competitive value yet".
         */
        @Nullable
        private final Page value;
        private final Weight weight;
        private PerLeaf perLeaf;

        private PerMinValue(Page value, Weight weight) {
            this.value = value;
            this.weight = weight;
        }

        public PerLeaf perLeaf(LeafReaderContext leaf) throws IOException {
            if (perLeaf == null || perLeaf.createdThread != Thread.currentThread() || perLeaf.leaf != leaf) {
                Scorer scorer = weight.scorer(leaf);
                DocIdSetIterator disi = scorer == null ? DocIdSetIterator.empty() : scorer.iterator();
                perLeaf = new PerLeaf(Thread.currentThread(), leaf, disi);
            }
            return perLeaf;
        }

        @Override
        public void close() {
            Releasables.close(value);
        }
    }

    private class PerLeaf {
        private final Thread createdThread;
        private final LeafReaderContext leaf;
        private final DocIdSetIterator disi;

        private PerLeaf(Thread createdThread, LeafReaderContext leaf, DocIdSetIterator disi) {
            this.createdThread = createdThread;
            this.leaf = leaf;
            this.disi = disi;
        }

        public DocIdSetIterator disi() {
            return disi;
        }
    }
}
