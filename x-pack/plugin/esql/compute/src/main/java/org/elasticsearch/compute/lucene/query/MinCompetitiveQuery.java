/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Infrastructure to provide {@link LeafCollector#competitiveIterator} from a
 * {@link SharedMinCompetitive}.
 */
public class MinCompetitiveQuery implements Releasable {
    private static final Logger log = LogManager.getLogger(MinCompetitiveQuery.class);

    public record Factory(SharedMinCompetitive.Supplier minCompetitive, BuildMinCompetitiveQuery queryFunction) {
        public MinCompetitiveQuery build(BlockFactory blockFactory) {
            return new MinCompetitiveQuery(blockFactory, minCompetitive.get(), queryFunction);
        }
    }

    @FunctionalInterface
    public interface BuildMinCompetitiveQuery {
        Query build(ShardContext ctx, Page page) throws IOException;
    }

    private final BlockFactory blockFactory;
    private final SharedMinCompetitive minCompetitive;
    private final BuildMinCompetitiveQuery buildMinCompetitiveQuery;
    private PerIndex perIndex;
    private DocIdSetIterator disi;

    private int changedValue;
    private int matchAll;
    private int matchNone;
    private int greaterThanMinCompetitive;

    private long updateNanos;

    private MinCompetitiveQuery(
        BlockFactory blockFactory,
        SharedMinCompetitive minCompetitive,
        BuildMinCompetitiveQuery buildMinCompetitiveQuery
    ) {
        this.blockFactory = blockFactory;
        this.minCompetitive = minCompetitive;
        this.buildMinCompetitiveQuery = buildMinCompetitiveQuery;
    }

    public DocIdSetIterator disi() {
        return disi;
    }

    public void update(ShardContext ctx, LeafReaderContext leaf) throws IOException {
        long start = System.nanoTime();
        this.disi = updatedDisi(ctx, leaf);
        updateNanos += System.nanoTime() - start;
    }

    private DocIdSetIterator updatedDisi(ShardContext ctx, LeafReaderContext leaf) throws IOException {
        PerIndex index = perIndex(ctx);
        long generation = minCompetitive.generation();
        if (index.perMinValue == null || index.cachedGeneration != generation) {
            index.updatePerMinValue(minCompetitive.get(blockFactory), generation);
        }
        return index.perMinValue.perLeaf(leaf).disi();
    }

    private PerIndex perIndex(ShardContext ctx) {
        if (perIndex == null || perIndex.ctx != ctx) {
            perIndex = new PerIndex(ctx);
        }
        return perIndex;
    }

    public Status status() {
        return new Status(changedValue, matchAll, matchNone, greaterThanMinCompetitive, updateNanos);
    }

    @Override
    public void close() {
        minCompetitive.decRef();
    }

    private class PerIndex {
        private final ShardContext ctx;
        private long cachedGeneration = -1;
        private PerMinValue perMinValue;

        private PerIndex(ShardContext ctx) {
            this.ctx = ctx;
        }

        private void updatePerMinValue(Page value, long generation) throws IOException {
            if (perMinValue != null) {
                perMinValue.close();
            }
            perMinValue = newPerMinValue(value);
            cachedGeneration = generation;
        }

        private PerMinValue newPerMinValue(Page value) throws IOException {
            try {
                Query query = buildMinCompetitiveQuery(value);
                log.debug("updating min competitive to {} using {}", query, value);
                changedValue++;
                Weight weight = uncachedWeight(ctx, query);
                PerMinValue result = new PerMinValue(value, weight);
                value = null;
                return result;
            } finally {
                Releasables.close(value);
            }
        }

        private Query buildMinCompetitiveQuery(Page value) throws IOException {
            if (value == null) {
                matchAll++;
                return Queries.ALL_DOCS_INSTANCE;
            }
            Query q = buildMinCompetitiveQuery.build(ctx, value);
            if (q instanceof MatchAllDocsQuery) {
                matchAll++;
                return q;
            } else if (q instanceof MatchNoDocsQuery) {
                matchNone++;
                return q;
            }
            greaterThanMinCompetitive++;
            return q;
        }
    }

    private static Weight uncachedWeight(ShardContext ctx, Query query) throws IOException {
        IndexSearcher searcher = new IndexSearcher(ctx.searcher().getIndexReader());
        searcher.setQueryCache(null);
        return query.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 0.0F);
    }

    private class PerMinValue implements Releasable {
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
                DocIdSetIterator competitive = scorer == null ? DocIdSetIterator.empty() : scorer.iterator();
                perLeaf = new PerLeaf(Thread.currentThread(), leaf, competitive);
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

    public record Status(int changedValue, int matchAll, int matchNone, int greaterThanMinCompetitive, long updateNanos)
        implements
            Writeable,
            ToXContentObject {
        public static Status readFrom(StreamInput in) throws IOException {
            return new Status(in.readVInt(), in.readVInt(), in.readVInt(), in.readVInt(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(changedValue);
            out.writeVInt(matchAll);
            out.writeVInt(matchNone);
            out.writeVInt(greaterThanMinCompetitive);
            out.writeVLong(updateNanos);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("changed_value", changedValue);
            builder.field("match_all", matchAll);
            builder.field("match_none", matchNone);
            builder.field("greater_than_min_competitive", greaterThanMinCompetitive);
            builder.field("update_nanos", updateNanos);
            if (builder.humanReadable()) {
                builder.field("update_time", TimeValue.timeValueNanos(updateNanos));
            }
            return builder.endObject();
        }
    }
}
