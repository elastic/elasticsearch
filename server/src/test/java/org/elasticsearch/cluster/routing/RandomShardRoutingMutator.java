/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import java.util.Set;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomInt;

/**
 * Utility class the makes random modifications to ShardRouting
 */
public final class RandomShardRoutingMutator {
    private RandomShardRoutingMutator() {

    }

    public static ShardRouting randomChange(ShardRouting shardRouting, Set<String> nodes) {
        switch (randomInt(2)) {
            case 0:
                if (shardRouting.unassigned() == false && shardRouting.primary() == false) {
                    shardRouting = shardRouting.moveToUnassigned(new UnassignedInfo(randomReason(), randomAlphaOfLength(10)));
                } else if (shardRouting.unassignedInfo() != null) {
                    shardRouting = shardRouting.updateUnassigned(new UnassignedInfo(randomReason(), randomAlphaOfLength(10)),
                        shardRouting.recoverySource());
                }
                break;
            case 1:
                if (shardRouting.unassigned() && nodes.isEmpty() == false) {
                    shardRouting = shardRouting.initialize(randomFrom(nodes), null, -1);
                }
                break;
            case 2:
                if (shardRouting.initializing()) {
                    shardRouting = shardRouting.moveToStarted();
                }
                break;
        }
        return shardRouting;
    }


    public static UnassignedInfo.Reason randomReason() {
        switch (randomInt(9)) {
            case 0:
                return UnassignedInfo.Reason.INDEX_CREATED;
            case 1:
                return UnassignedInfo.Reason.CLUSTER_RECOVERED;
            case 2:
                return UnassignedInfo.Reason.INDEX_REOPENED;
            case 3:
                return UnassignedInfo.Reason.DANGLING_INDEX_IMPORTED;
            case 4:
                return UnassignedInfo.Reason.NEW_INDEX_RESTORED;
            case 5:
                return UnassignedInfo.Reason.EXISTING_INDEX_RESTORED;
            case 6:
                return UnassignedInfo.Reason.REPLICA_ADDED;
            case 7:
                return UnassignedInfo.Reason.ALLOCATION_FAILED;
            case 8:
                return UnassignedInfo.Reason.NODE_LEFT;
            default:
                return UnassignedInfo.Reason.REROUTE_CANCELLED;
        }
    }
}
