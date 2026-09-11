/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.recovery;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.indices.recovery.RecoveryState;

import java.io.IOException;
import java.util.Objects;

/// Combines a shard's recovery state with a snapshot of the recovery gate that may be blocking it.
public final class ShardRecoveryInfo implements Writeable {

    public static final TransportVersion GATE_IN_RECOVERY_RESPONSE = TransportVersion.fromName("gate_in_recovery_response");

    /// Sentinel duration used when the recovery is not blocked by a gate.
    public static final long NOT_BLOCKED_MILLIS = -1L;

    private final RecoveryState recoveryState;
    private final @Nullable String gate;
    private final long blockedForMillis;

    /// @param recoveryState the underlying shard recovery state
    /// @param gate the name of the [org.elasticsearch.indices.recovery.RecoveryGate] blocking the recovery,
    ///             or `null` if it is not blocked
    /// @param blockedForMillis how long the recovery gate had been blocking when this information was captured
    public ShardRecoveryInfo(RecoveryState recoveryState, @Nullable String gate, long blockedForMillis) {
        this.recoveryState = Objects.requireNonNull(recoveryState);
        this.gate = gate;
        assert gate == null ? blockedForMillis == NOT_BLOCKED_MILLIS : blockedForMillis >= 0L
            : "blockedForMillis must be -1 without a gate and non-negative with a gate";
        this.blockedForMillis = blockedForMillis;
    }

    public ShardRecoveryInfo(StreamInput in) throws IOException {
        recoveryState = RecoveryState.readRecoveryState(in);
        if (in.getTransportVersion().supports(GATE_IN_RECOVERY_RESPONSE)) {
            gate = in.readOptionalString();
            blockedForMillis = gate == null ? NOT_BLOCKED_MILLIS : in.readVLong();
        } else {
            gate = null;
            blockedForMillis = NOT_BLOCKED_MILLIS;
        }
    }

    public RecoveryState recoveryState() {
        return recoveryState;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        recoveryState.writeTo(out);
        if (out.getTransportVersion().supports(GATE_IN_RECOVERY_RESPONSE)) {
            final String blockedByGate = blockedByGate();
            out.writeOptionalString(blockedByGate);
            if (blockedByGate != null) {
                out.writeVLong(blockedForMillis);
            }
        }
    }

    /// Returns the blocking recovery gate only while the recovery is still queued in the `CREATED` stage.
    @Nullable
    public String blockedByGate() {
        if (gate != null && recoveryState.getStage() == RecoveryState.Stage.CREATED) {
            return gate;
        }
        return null;
    }

    long blockedForMillis() {
        return blockedForMillis;
    }
}
