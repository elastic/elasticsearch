/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.index.engine;

import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.ShardId;

public final class AlreadyProcessedFollowingEngineException extends VersionConflictEngineException {
    AlreadyProcessedFollowingEngineException(ShardId shardId, long seqNo) {
        super(shardId, "operation [{}] was processed before", null, seqNo);
    }
}
