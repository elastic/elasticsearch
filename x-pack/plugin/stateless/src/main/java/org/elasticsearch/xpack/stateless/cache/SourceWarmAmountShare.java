/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.core.Nullable;

import java.util.Locale;

/// The answer of a shutting-down relocation source to how much of its warm amount this shard accounts for, used by the warm-amount
/// heuristic of [SearchRecoveryTimeoutCalculationService]. Created per recovery, before the request is sent, and completed when the
/// response or failure arrives. Recovery never waits for it: a plan computed before the answer arrives does without it.
public final class SourceWarmAmountShare {

    /// No request was sent for this recovery.
    public static final SourceWarmAmountShare NOT_REQUESTED = new SourceWarmAmountShare(false, 0L);

    /// @param shardAmount  the source's estimate of the bytes this shard warms
    /// @param competingSum the source's estimate of the bytes warmed by all its shards competing for this target, including this one;
    ///                     `0` when the source could not compute it
    public record Share(long shardAmount, long competingSum) {
        public static final Share NONE = new Share(0L, 0L);
    }

    public enum Status {
        /** The source answered for its current shutdown signal and the competing sum is positive. */
        AVAILABLE,
        /** The source could not compute a share, the request failed, or it was sent under a since-replaced shutdown signal. */
        UNAVAILABLE,
        /** The request was sent but the answer has not arrived yet. */
        PENDING,
        NOT_REQUESTED;

        String attributeValue() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private final boolean requested;
    private final long shutdownSignalTimestamp;
    @Nullable
    private volatile Share share;

    private SourceWarmAmountShare(boolean requested, long shutdownSignalTimestamp) {
        this.requested = requested;
        this.shutdownSignalTimestamp = shutdownSignalTimestamp;
    }

    /// A pending answer for a request sent under the source's shutdown signal started at `shutdownSignalTimestamp`.
    public static SourceWarmAmountShare requested(long shutdownSignalTimestamp) {
        return new SourceWarmAmountShare(true, shutdownSignalTimestamp);
    }

    public void complete(Share share) {
        assert requested;
        this.share = share;
    }

    public boolean isRequested() {
        return requested;
    }

    public long shutdownSignalTimestamp() {
        return shutdownSignalTimestamp;
    }

    /// The answer, or `null` while it is pending (or when no request was sent).
    @Nullable
    public Share share() {
        return share;
    }

    /// Whether the answer can be used by a plan for the source's shutdown signal started at `currentShutdownSignalTimestamp`.
    public Status status(long currentShutdownSignalTimestamp) {
        if (requested == false) {
            return Status.NOT_REQUESTED;
        }
        final var answer = share;
        if (answer == null) {
            return Status.PENDING;
        }
        if (shutdownSignalTimestamp != currentShutdownSignalTimestamp || answer.competingSum() <= 0L) {
            return Status.UNAVAILABLE;
        }
        return Status.AVAILABLE;
    }

    @Override
    public String toString() {
        return "SourceWarmAmountShare{requested="
            + requested
            + ", shutdownSignalTimestamp="
            + shutdownSignalTimestamp
            + ", share="
            + share
            + '}';
    }
}
