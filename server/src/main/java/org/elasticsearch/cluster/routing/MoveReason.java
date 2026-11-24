/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.core.Nullable;

import java.util.Objects;

/**
 * Why a shard copy is being relocated.
 *
 * @param type the kind of move
 * @param canRemainDecider the label of the {@code AllocationDecider} whose {@code canRemain} decision triggered the move, or {@code null}
 *                         if the move was not triggered by a decider or the deciding decider is not known
 */
public record MoveReason(MoveType type, @Nullable String canRemainDecider) {

    // Shared instances for reasons without a decider label, which is the common case when decision labels are not preserved.
    public static final MoveReason CANNOT_REMAIN = new MoveReason(MoveType.CANNOT_REMAIN, null);
    public static final MoveReason NOT_PREFERRED = new MoveReason(MoveType.NOT_PREFERRED, null);
    public static final MoveReason REBALANCE = new MoveReason(MoveType.REBALANCE, null);
    public static final MoveReason COMMAND = new MoveReason(MoveType.COMMAND, null);
    public static final MoveReason RESTARTED = new MoveReason(MoveType.RESTARTED, null);
    /**
     * The reason the desired balance computation gives for relocating a copy to where the previous desired balance already wanted it.
     */
    public static final MoveReason INHERITED = new MoveReason(MoveType.INHERITED, null);

    public MoveReason {
        Objects.requireNonNull(type);
        assert canRemainDecider == null || type == MoveType.CANNOT_REMAIN || type == MoveType.NOT_PREFERRED
            : "only moves triggered by a canRemain decision have a decider label, but got [" + type + "] with [" + canRemainDecider + "]";
    }

    /**
     * @return the shared reason for the given type, which has no decider label
     */
    public static MoveReason of(MoveType type) {
        return switch (type) {
            case CANNOT_REMAIN -> CANNOT_REMAIN;
            case NOT_PREFERRED -> NOT_PREFERRED;
            case REBALANCE -> REBALANCE;
            case COMMAND -> COMMAND;
            case RESTARTED -> RESTARTED;
            case INHERITED -> INHERITED;
        };
    }

    /**
     * @return the reason for the given type and decider label, avoiding an allocation when there is no label
     */
    public static MoveReason of(MoveType type, @Nullable String canRemainDecider) {
        return canRemainDecider == null ? of(type) : new MoveReason(type, canRemainDecider);
    }
}
