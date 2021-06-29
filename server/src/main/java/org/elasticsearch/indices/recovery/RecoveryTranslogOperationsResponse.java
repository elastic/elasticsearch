/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

final class RecoveryTranslogOperationsResponse extends TransportResponse {
    final long localCheckpoint;

    RecoveryTranslogOperationsResponse(final long localCheckpoint) {
        this.localCheckpoint = localCheckpoint;
    }

    RecoveryTranslogOperationsResponse(final StreamInput in) throws IOException {
        super(in);
        localCheckpoint = in.readZLong();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeZLong(localCheckpoint);
    }
}
