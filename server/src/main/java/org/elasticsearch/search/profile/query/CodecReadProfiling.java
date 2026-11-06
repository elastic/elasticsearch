/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile.query;

import org.apache.lucene.index.LeafReader;
import org.elasticsearch.codec.profiler.MetricRecorder;
import org.elasticsearch.codec.profiler.Scope;
import org.elasticsearch.codec.profiler.Scopes;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.search.profile.CodecReadAttribution;
import org.elasticsearch.search.profile.CodecReads;
import org.elasticsearch.search.profile.ProfileSelection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.LongConsumer;

/**
 * The codec reads of one weight of a profiled query node. See {@link CodecReadAttribution} for the architecture.
 *
 * <p>Each call that builds a scorer or counts for a segment has its own {@link Leaf}, which the scorer it builds binds
 * again in each of its calls. A leaf creates a
 * {@link CodecReadAttribution} only when the first codec reader opens in it, so a node that opens no reader keeps
 * nothing. A reader reports to the node whose leaf was bound when it opened, because a nested node binds its leaf
 * inside the leaf of the outer node.
 *
 * <p>The segment key is {@code <index>:<shard>/<segment>}, as in the ES|QL profile, so one key selects the same segment
 * in both frontends. A leaf whose reader has no shard uses the Lucene segment name alone. Several leaves can read one
 * segment, for example when a segment is searched in partitions; {@link CodecReads#merge} adds their counts.
 *
 * <p>Leaves are added under a lock because they run on the threads that search the segments. Each leaf has one writer,
 * the thread that searches its segment. Snapshots are taken after the search ends.
 */
final class CodecReadProfiling {

    private final ProfileSelection selection;
    private final LongConsumer charge;
    private final List<CodecReadAttribution> leaves = Collections.synchronizedList(new ArrayList<>());

    /**
     * @param selection the codec read selection
     * @param charge    called with the bytes of the details before they are allocated; it can throw to refuse them
     */
    CodecReadProfiling(ProfileSelection selection, LongConsumer charge) {
        this.selection = selection;
        this.charge = charge;
    }

    /**
     * Creates the scope for the readers that one call opens on {@code reader}. The leaf reader describes the segment and
     * its fields, and is set on the attribution only while a codec reader opens.
     */
    Leaf leaf(@Nullable LeafReader reader) {
        return new Leaf(reader);
    }

    /** Returns the merged codec reads of all leaves. */
    CodecReads snapshot() {
        final List<CodecReads> parts = new ArrayList<>();
        synchronized (leaves) {
            for (CodecReadAttribution leaf : leaves) {
                parts.add(leaf.snapshot());
            }
        }
        return parts.isEmpty() ? CodecReads.EMPTY : CodecReads.merge(parts);
    }

    /** Returns the number of leaves in which a codec reader opened. */
    int leafCount() {
        return leaves.size();
    }

    /** The scope of one call. It registers its attribution with the node when the first reader opens. */
    final class Leaf implements Scope {
        @Nullable
        private final LeafReader reader;
        private CodecReadAttribution attribution;

        private Leaf(@Nullable LeafReader reader) {
            this.reader = reader;
        }

        /** Binds this scope on the current thread until the returned value is closed. */
        Releasable bind() {
            final Scopes.Binding binding = Scopes.bind(this);
            return binding::close;
        }

        @Override
        public MetricRecorder metrics(String codec, String segment, String field, Map<String, Boolean> facts) {
            if (attribution == null) {
                attribution = new CodecReadAttribution(selection, charge);
                final ShardId shardId = reader == null ? null : ShardUtils.extractShardId(reader);
                if (shardId != null) {
                    attribution.partition(shardId.getIndexName() + ":" + shardId.id());
                }
                leaves.add(attribution);
            }
            attribution.leaf(reader);
            try {
                return attribution.metrics(codec, segment, field, facts);
            } finally {
                attribution.leaf(null);
            }
        }
    }
}
