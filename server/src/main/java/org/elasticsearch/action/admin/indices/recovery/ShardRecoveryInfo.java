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

public final class ShardRecoveryInfo implements Writeable {

    public static final TransportVersion GATE_IN_RECOVERY_RESPONSE = TransportVersion.fromName("gate_in_recovery_response");

    private final RecoveryState recoveryState;
    private final @Nullable String gate;

    /// @param recoveryState the underlying shard recovery state
    /// @param gate the name of the [org.elasticsearch.indices.recovery.RecoveryGate] blocking the recovery,
    ///             or `null` if it is not blocked
    public ShardRecoveryInfo(RecoveryState recoveryState, @Nullable String gate) {
        this.recoveryState = Objects.requireNonNull(recoveryState);
        this.gate = gate;
    }

    public ShardRecoveryInfo(StreamInput in) throws IOException {
        this(
            RecoveryState.readRecoveryState(in),
            in.getTransportVersion().supports(GATE_IN_RECOVERY_RESPONSE) ? in.readOptionalString() : null
        );
    }

    public RecoveryState recoveryState() {
        return recoveryState;
    }

    @Nullable
    public String gate() {
        return gate;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        recoveryState.writeTo(out);
        if (out.getTransportVersion().supports(GATE_IN_RECOVERY_RESPONSE)) {
            out.writeOptionalString(gate);
        }
    }
}
