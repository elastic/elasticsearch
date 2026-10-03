/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;

import java.util.Objects;

/**
 * Supplies extensions for eviction policies. The default is a noop.
 */
public interface EvictionPolicyExtension {

    /**
     * How {@link PinnedWindowEvictionPolicy} should protect regions of one shard.
     * <p>
     * {@link Duration} protects regions inside the window, plus regions whose timestamp is unknown or still being backfilled.
     * {@link Always} protects every region of the shard. {@link Never} protects none of them, including those sentinel timestamps.
     * A negative {@link Duration} is rejected; use {@link Never} to protect nothing.
     */
    sealed interface PinnedWindow permits PinnedWindow.Duration, PinnedWindow.Always, PinnedWindow.Never {

        /**
         * Protect regions whose timestamp falls within {@code duration}, and regions with an unknown or backfill timestamp.
         * {@code duration} must not be negative.
         */
        record Duration(TimeValue duration) implements PinnedWindow {
            public Duration {
                Objects.requireNonNull(duration);
                if (duration.compareTo(TimeValue.ZERO) < 0) {
                    throw new IllegalArgumentException("pinned window duration must not be negative, but was [" + duration + "]");
                }
            }
        }

        /** Protect every region of the shard, including unknown and backfill timestamps. */
        record Always() implements PinnedWindow {}

        /** Protect no region of the shard, including unknown and backfill timestamps. */
        record Never() implements PinnedWindow {}
    }

    EvictionPolicyExtension NOOP = (shardId, configuredDuration) -> new PinnedWindow.Duration(configuredDuration);

    /**
     * Returns how {@code shardId} should be pinned when the {@link PinnedWindowEvictionPolicy} is used.
     * <p>
     * Called from {@link org.elasticsearch.blobcache.shared.EvictionPolicy#isProtected} and from the predicate produced by
     * {@link org.elasticsearch.blobcache.shared.EvictionPolicy#createPredicate}. The eviction scan holds the shared blob cache
     * monitor and invokes the predicate for every candidate region; metrics may call {@code isProtected} without that monitor.
     * Implementations must be thread-safe, must not perform I/O, and must not block. A slow call stalls cache admission for
     * the whole scan.
     *
     * @param shardId             shard whose cache regions are being considered for protection by the {@link PinnedWindowEvictionPolicy}
     * @param configuredDuration  current value of {@link PinnedWindowEvictionPolicy#PINNED_WINDOW_DURATION_SETTING}; not negative
     * @return how this shard should be pinned; never {@code null}
     */
    PinnedWindow pinnedWindowForShard(ShardId shardId, TimeValue configuredDuration);
}
