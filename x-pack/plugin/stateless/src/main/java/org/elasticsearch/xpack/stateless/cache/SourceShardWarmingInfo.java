/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.core.Nullable;

import java.util.Locale;
import java.util.Optional;

/// Created per recovery, before the request is sent, and completed when the response or failure arrives.
public final class SourceShardWarmingInfo {

    /// No request was sent for this recovery.
    public static final SourceShardWarmingInfo NOT_REQUESTED = new SourceShardWarmingInfo(false, 0L);

    /// @param shardBytesToWarm  the source's estimate of the bytes this shard warms
    /// @param sourceBytesToWarm the source's estimate of the bytes warmed by all its shards not already relocating to another node,
    ///                          including this one; `0` when the source could not compute it
    public record WarmingInfo(long shardBytesToWarm, long sourceBytesToWarm) {
        public static final WarmingInfo NONE = new WarmingInfo(0L, 0L);

        /// Fraction of `sourceBytesToWarm` accounted for by this shard, or `0` when `sourceBytesToWarm` is not positive.
        public double warmBytesRatio() {
            return sourceBytesToWarm > 0L ? shardBytesToWarm / (double) sourceBytesToWarm : 0.0;
        }
    }

    private final boolean requested;
    private final long shutdownSignalTimestamp;
    @Nullable
    private volatile WarmingInfo warmingInfo;

    private SourceShardWarmingInfo(boolean requested, long shutdownSignalTimestamp) {
        this.requested = requested;
        this.shutdownSignalTimestamp = shutdownSignalTimestamp;
    }

    /// A new pending holder for a request sent under the source's shutdown signal started at `shutdownSignalTimestamp`.
    public static SourceShardWarmingInfo newPending(long shutdownSignalTimestamp) {
        return new SourceShardWarmingInfo(true, shutdownSignalTimestamp);
    }

    public void complete(WarmingInfo warmingInfo) {
        assert requested;
        this.warmingInfo = warmingInfo;
    }

    public boolean isRequested() {
        return requested;
    }

    public boolean isDone() {
        return warmingInfo != null;
    }

    public long shutdownSignalTimestamp() {
        return shutdownSignalTimestamp;
    }

    public Optional<WarmingInfo> get(long currentShutdownSignalTimestamp) {
        final var answer = warmingInfo;
        if (status(answer, currentShutdownSignalTimestamp) == Status.AVAILABLE) {
            assert answer != null;
            return Optional.of(answer);
        } else {
            return Optional.empty();
        }
    }

    // This is only used for metrics. Can be removed once metrics are no longer needed.
    enum Status {
        /** The source answered for its current shutdown signal. */
        AVAILABLE,
        /** The source could not compute the warming info, the request failed, or it was sent under a since-replaced shutdown signal. */
        UNAVAILABLE,
        /** The request was sent but the answer has not arrived yet. */
        PENDING,
        NOT_REQUESTED;

        String attributeValue() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /// Status of the answer for the source's shutdown signal started at `currentShutdownSignalTimestamp`.
    Status status(long currentShutdownSignalTimestamp) {
        return status(warmingInfo, currentShutdownSignalTimestamp);
    }

    private Status status(@Nullable WarmingInfo answer, long currentShutdownSignalTimestamp) {
        if (requested == false) {
            return Status.NOT_REQUESTED;
        }
        if (answer == null) {
            return Status.PENDING;
        }
        if (shutdownSignalTimestamp != currentShutdownSignalTimestamp || answer.sourceBytesToWarm() <= 0L) {
            return Status.UNAVAILABLE;
        }
        return Status.AVAILABLE;
    }

    @Override
    public String toString() {
        return "SourceShardWarmingInfo{requested="
            + requested
            + ", shutdownSignalTimestamp="
            + shutdownSignalTimestamp
            + ", warmingInfo="
            + warmingInfo
            + '}';
    }
}
