/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.index.engine;

import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.ShardId;

import java.util.OptionalLong;

/**
 * An exception represents that an operation was processed before on the {@link FollowingEngine} of the primary of a follower.
 * The field {@code existingPrimaryTerm} is empty only if the operation is below the global checkpoint; otherwise it should be non-empty.
 */
public final class AlreadyProcessedFollowingEngineException extends VersionConflictEngineException {
    private final long seqNo;
    private final OptionalLong existingPrimaryTerm;

    AlreadyProcessedFollowingEngineException(ShardId shardId, long seqNo, OptionalLong existingPrimaryTerm) {
        super(shardId, "operation [{}] was processed before with term [{}]", null, seqNo, existingPrimaryTerm);
        this.seqNo = seqNo;
        this.existingPrimaryTerm = existingPrimaryTerm;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public OptionalLong getExistingPrimaryTerm() {
        return existingPrimaryTerm;
    }
}
