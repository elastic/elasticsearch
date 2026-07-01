/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/// Exception indicating that a recovery was cancelled by request from the master node.
public class RecoveryCancelledException extends RecoveryFailedException {

    public static final TransportVersion RECOVERY_CANCELLED_EXCEPTION_VERSION = TransportVersion.fromName("recovery_cancelled_exception");

    public RecoveryCancelledException(ShardId shardId, @Nullable DiscoveryNode sourceNode, DiscoveryNode targetNode) {
        super(shardId, sourceNode, targetNode, "recovery cancelled by master node", null);
    }

    public RecoveryCancelledException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;  // this exception doesn't imply a bug, no need for a stack trace
    }
}
