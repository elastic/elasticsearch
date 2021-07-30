/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery.plan;

import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.util.List;

public class ShardRecoveryPlanners implements ShardRecoveryPlanner {

    private final List<ShardRecoveryPlanner> planners;

    public ShardRecoveryPlanners(List<ShardRecoveryPlanner> planners) {
        this.planners = planners;
    }

    @Override
    public ShardRecoveryPlan computePlan(String shardIdentifier,
                                         Store.MetadataSnapshot sourceMetadata,
                                         Store.MetadataSnapshot targetMetadata,
                                         long startingSeqNo,
                                         int translogOps,
                                         Store.RecoveryDiff sourceTargetDiff,
                                         List<StoreFileMetadata> filesMissingInTarget,
                                         List<ShardSnapshot> availableSnapshots) {
        for (ShardRecoveryPlanner planner : planners) {
            ShardRecoveryPlan plan = planner.computePlan(shardIdentifier,
                sourceMetadata,
                targetMetadata,
                startingSeqNo,
                translogOps,
                sourceTargetDiff,
                filesMissingInTarget,
                availableSnapshots
            );

            if (plan != null) {
                return plan;
            }
        }

        return null;
    }
}
