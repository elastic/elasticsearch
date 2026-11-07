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

/**
 * Why a started shard copy is being relocated.
 */
public enum MoveType {
    /** The allocator decided the copy cannot remain on its node ({@code canRemain} is {@code NO}). */
    CANNOT_REMAIN("move(cannot-remain)", RecoveryPriority.RELOCATION_CAN_REMAIN_NO),
    /** The allocator decided the copy should not remain on its node ({@code canRemain} is {@code NOT_PREFERRED}). */
    NOT_PREFERRED("move(not-preferred)", RecoveryPriority.RELOCATION_CAN_REMAIN_NOT_PREFERRED),
    /** The allocator is moving the copy to improve the balance. */
    REBALANCE("move(rebalance)", RecoveryPriority.RELOCATE_REBALANCING),
    /** An operator requested the move with an allocation command. */
    COMMAND("move command", RecoveryPriority.RELOCATION_CAN_REMAIN_NO),
    /** A replica relocation was restarted because its primary relocated; the relocation keeps its previous priority. */
    RESTARTED("restarting relocation", null),
    /** The desired balance computation moved the copy to where the previous desired balance already wanted it. */
    INHERITED("computation", null);

    private final String reason;
    private final RecoveryPriority recoveryPriority;

    MoveType(String reason, @Nullable RecoveryPriority recoveryPriority) {
        this.reason = reason;
        this.recoveryPriority = recoveryPriority;
    }

    /**
     * @return the reason string used in logs and metrics
     */
    public String reason() {
        return reason;
    }

    /**
     * @return the recovery priority the allocator gives to relocations of this type, or {@code null} if the type has no fixed priority
     */
    @Nullable
    public RecoveryPriority recoveryPriority() {
        return recoveryPriority;
    }
}
