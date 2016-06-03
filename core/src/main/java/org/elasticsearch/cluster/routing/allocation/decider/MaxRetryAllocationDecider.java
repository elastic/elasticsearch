/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

/**
 * An allocation decider that prevents shards from being allocated on any node if the shards allocation has been retried N times without
 * success. This means if a shard has been INITIALIZING N times in a row without being moved to STARTED the shard will be ignored until
 * the setting for <tt>index.allocation.max_retry</tt> is raised. The default value is <tt>5</tt>.
 * Note: This allocation decider also allows allocation of repeatedly failing shards when the <tt>/_cluster/reroute?retry_failed=true</tt>
 * API is manually invoked. This allows single retries without raising the limits.
 *
 * @see RoutingAllocation#isRetryFailed()
 */
public class MaxRetryAllocationDecider extends AllocationDecider {

    public static final Setting<Integer> SETTING_ALLOCATION_MAX_RETRY = Setting.intSetting("index.allocation.max_retries", 5, 0,
        Setting.Property.Dynamic, Setting.Property.IndexScope);

    public static final String NAME = "max_retry";

    /**
     * Initializes a new {@link MaxRetryAllocationDecider}
     *
     * @param settings {@link Settings} used by this {@link AllocationDecider}
     */
    @Inject
    public MaxRetryAllocationDecider(Settings settings) {
        super(settings);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
        if (unassignedInfo != null && unassignedInfo.getNumFailedAllocations() > 0) {
            final IndexMetaData indexMetaData = allocation.metaData().getIndexSafe(shardRouting.index());
            final int maxRetry = SETTING_ALLOCATION_MAX_RETRY.get(indexMetaData.getSettings());
            if (allocation.isRetryFailed()) { // manual allocation - retry
                // if we are called via the _reroute API we ignore the failure counter and try to allocate
                // this improves the usability since people don't need to raise the limits to issue retries since a simple _reroute call is
                // enough to manually retry.
                return allocation.decision(Decision.YES, NAME, "shard has already failed allocating ["
                    + unassignedInfo.getNumFailedAllocations() + "] times vs. [" + maxRetry + "] retries allowed "
                    + unassignedInfo.toString() + " - retrying once on manual allocation");
            } else if (unassignedInfo.getNumFailedAllocations() >= maxRetry) {
                return allocation.decision(Decision.NO, NAME, "shard has already failed allocating ["
                    + unassignedInfo.getNumFailedAllocations() + "] times vs. [" + maxRetry + "] retries allowed "
                    + unassignedInfo.toString() + " - manually call [/_cluster/reroute?retry_failed=true] to retry");
            }
        }
        return allocation.decision(Decision.YES, NAME, "shard has no previous failures");
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, allocation);
    }
}
