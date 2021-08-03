/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery.plan;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;

public interface RecoveryPlannerService {
    void computeRecoveryPlan(ShardId shardId,
                             String shardIdentifier,
                             Store.MetadataSnapshot sourceMetadata,
                             Store.MetadataSnapshot targetMetadata,
                             long startingSeqNo,
                             int translogOps,
                             ActionListener<ShardRecoveryPlan> listener);
}
