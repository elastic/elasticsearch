/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

final class RecoveryTranslogOperationsResponse extends TransportResponse {
    final long localCheckpoint;

    RecoveryTranslogOperationsResponse(final long localCheckpoint) {
        this.localCheckpoint = localCheckpoint;
    }

    RecoveryTranslogOperationsResponse(final StreamInput in) throws IOException {
        super(in);
        // before 6.0.0 we received an empty response so we have to maintain that
        if (in.getVersion().onOrAfter(Version.V_6_0_0_alpha1)) {
            localCheckpoint = in.readZLong();
        } else {
            localCheckpoint = SequenceNumbers.UNASSIGNED_SEQ_NO;
        }
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        // before 6.0.0 we responded with an empty response so we have to maintain that
        if (out.getVersion().onOrAfter(Version.V_6_0_0_alpha1)) {
            out.writeZLong(localCheckpoint);
        }
    }
}
