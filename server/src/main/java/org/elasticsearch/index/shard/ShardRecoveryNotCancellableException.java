/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

/// Exception indicating that a direct recovery cancellation could not be applied (usually because the shard's recovery type
/// does not support direct cancellation of started recoveries).
public class ShardRecoveryNotCancellableException extends Exception {

    public ShardRecoveryNotCancellableException(ShardId shardId, String reason) {
        super("Unable to direct cancel recovery for shard " + shardId + " for reason: " + reason);
    }
}
