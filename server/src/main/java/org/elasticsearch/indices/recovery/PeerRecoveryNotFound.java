/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class PeerRecoveryNotFound extends ResourceNotFoundException {

    public PeerRecoveryNotFound(final long recoveryId, final ShardId shardId, final String targetAllocationId) {
        super(
            "Peer recovery for "
                + shardId
                + " with [recoveryId: "
                + recoveryId
                + ", targetAllocationId: "
                + targetAllocationId
                + "] not found."
        );
    }

    public PeerRecoveryNotFound(StreamInput in) throws IOException {
        super(in);
    }
}
