/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
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
import java.util.Objects;

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

    /**
     * Number of times the min competitive changed, forcing the query to rebuild.
     * We still rebuild the query when moving to a new index but do not increment
     * the counter in that case
     */
    private int changedValue;
    /**
     * Number of times the {@code min_competitive} query produced a {@code match_all}.
     */
    private int matchAll;
    /**
     * Number of times the {@code min_competitive} query produced a {@code match_none}.
     */
    private int matchNone;
    /**
     * Number of times this produced a query bigger that isn't {@code match_all} or {@code match_none}.
     */
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

    /**
     * The actual {@link DocIdSetIterator} matching min_competitive docs.
     */
    public DocIdSetIterator disi() {
        return disi; // TODO the lucene implementations return a constant from this.
        // That constant closes over a mutable disi and delegates. Do we need that?
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
        long start = System.nanoTime();
        this.disi = updatedDisi(ctx, leaf);
        updateNanos += System.nanoTime() - start;
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

    public Status status() {
        return new Status(changedValue, matchAll, matchNone, greaterThanMinCompetitive, updateNanos);
    }

    @Override
    public void close() {
        Releasables.close(minCompetitive, perIndex);
    }

    private class PerIndex implements Releasable {
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
                changedValue++;
            }
            return perMinValue;
        }

        private PerMinValue newPerMinValue(Page value) throws IOException {
            try {
                Query query = buildMinCompetitiveQuery(value);
                log.debug("updating min competitive to {} using {}", query, value);
                Weight weight = query.createWeight(ctx.searcher(), ScoreMode.COMPLETE_NO_SCORES, 0.0F);
                PerMinValue result = new PerMinValue(value, weight);
                value = null;
                return result;
            } finally {
                Releasables.close(value);
            }
        }

        private Query buildMinCompetitiveQuery(Page value) throws IOException {
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

        @Override
        public void close() {
            Releasables.close(perMinValue);
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
            if (perLeaf == null
                || perLeaf.createdThread != Thread.currentThread() // Scorers tend to fail if they shift to other threads. Rebuild.
                || perLeaf.leaf != leaf) {
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
