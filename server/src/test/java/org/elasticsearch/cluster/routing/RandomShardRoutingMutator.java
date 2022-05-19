/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import java.util.Set;

import static org.elasticsearch.cluster.routing.UnassignedInfoTests.randomUnassignedInfo;
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
                    shardRouting = shardRouting.moveToUnassigned(randomUnassignedInfo(randomAlphaOfLength(10), false));
                } else if (shardRouting.unassignedInfo() != null) {
                    shardRouting = shardRouting.updateUnassigned(
                        randomUnassignedInfo(randomAlphaOfLength(10), false),
                        shardRouting.recoverySource()
                    );
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
}
