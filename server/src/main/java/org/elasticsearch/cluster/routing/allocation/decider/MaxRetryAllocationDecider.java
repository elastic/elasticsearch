/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.routing.RelocationFailureInfo;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.Setting;

/**
 * An allocation decider that prevents shards from being allocated on any node if the shards allocation has been retried N times without
 * success. This means if a shard has been INITIALIZING N times in a row without being moved to STARTED the shard will be ignored until
 * the setting for {@code index.allocation.max_retry} is raised. The default value is {@code 5}.
 * Note: This allocation decider also allows allocation of repeatedly failing shards when the {@code /_cluster/reroute?retry_failed=true}
 * API is manually invoked. This allows single retries without raising the limits.
 *
 */
public class MaxRetryAllocationDecider extends AllocationDecider {

    public static final Setting<Integer> SETTING_ALLOCATION_MAX_RETRY = Setting.intSetting(
        "index.allocation.max_retries",
        5,
        0,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope,
        Setting.Property.NotCopyableOnResize
    );

    private static final String RETRY_FAILED_API = "POST /_cluster/reroute?retry_failed";

    public static final String NAME = "max_retry";

    private static final Decision YES_NO_FAILURES = Decision.single(Decision.Type.YES, NAME, "shard has no previous failures");

    private static final Decision YES_SIMULATING = Decision.single(Decision.Type.YES, NAME, "previous failures ignored when simulating");

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (allocation.isSimulating()) {
            return YES_SIMULATING;
        }

        final int maxRetries = SETTING_ALLOCATION_MAX_RETRY.get(allocation.metadata().getIndexSafe(shardRouting.index()).getSettings());
        final var unassignedInfo = shardRouting.unassignedInfo();
        final int numFailedAllocations = unassignedInfo == null ? 0 : unassignedInfo.getNumFailedAllocations();
        if (numFailedAllocations > 0) {
            final var decision = numFailedAllocations >= maxRetries ? Decision.NO : Decision.YES;
            return allocation.debugDecision() ? debugDecision(decision, unassignedInfo, numFailedAllocations, maxRetries) : decision;
        }

        final var relocationFailureInfo = shardRouting.relocationFailureInfo();
        final int numFailedRelocations = relocationFailureInfo == null ? 0 : relocationFailureInfo.failedRelocations();
        if (numFailedRelocations > 0) {
            final var decision = numFailedRelocations >= maxRetries ? Decision.NO : Decision.YES;
            return allocation.debugDecision() ? debugDecision(decision, relocationFailureInfo, numFailedRelocations, maxRetries) : decision;
        }

        return YES_NO_FAILURES;
    }

    private static Decision debugDecision(Decision decision, UnassignedInfo info, int numFailedAllocations, int maxRetries) {
        if (decision.type() == Decision.Type.NO) {
            return Decision.single(
                Decision.Type.NO,
                NAME,
                "shard has exceeded the maximum number of retries [%d] on failed allocation attempts - manually call [%s] to retry, [%s]",
                maxRetries,
                RETRY_FAILED_API,
                info.toString()
            );
        } else {
            return Decision.single(
                Decision.Type.YES,
                NAME,
                "shard has failed allocating [%d] times but [%d] retries are allowed",
                numFailedAllocations,
                maxRetries
            );
        }
    }

    private static Decision debugDecision(Decision decision, RelocationFailureInfo info, int numFailedRelocations, int maxRetries) {
        if (decision.type() == Decision.Type.NO) {
            return Decision.single(
                Decision.Type.NO,
                NAME,
                "shard has exceeded the maximum number of retries [%d] on failed relocation attempts - manually call [%s] to retry, [%s]",
                maxRetries,
                RETRY_FAILED_API,
                info.toString()
            );
        } else {
            return Decision.single(
                Decision.Type.YES,
                NAME,
                "shard has failed relocating [%d] times but [%d] retries are allowed",
                numFailedRelocations,
                maxRetries
            );
        }
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, allocation);
    }

    @Override
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        assert shardRouting.primary() : "must not call canForceAllocatePrimary on a non-primary shard " + shardRouting;
        // check if we have passed the maximum retry threshold through canAllocate,
        // if so, we don't want to force the primary allocation here
        return canAllocate(shardRouting, node, allocation);
    }

    @Override
    public Decision canForceAllocateDuringReplace(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, node, allocation);
    }
}
