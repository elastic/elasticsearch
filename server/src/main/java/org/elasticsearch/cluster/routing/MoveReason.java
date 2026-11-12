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

    /**
     * The reason the desired balance computation gives for relocating a copy to where the previous desired balance already wanted it.
     */
    public static final MoveReason INHERITED = of(MoveType.INHERITED);

    public MoveReason {
        Objects.requireNonNull(type);
    }

    public static MoveReason of(MoveType type) {
        return new MoveReason(type, null);
    }
}
