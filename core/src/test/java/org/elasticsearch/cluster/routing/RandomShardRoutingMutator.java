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

package org.elasticsearch.cluster.routing;

import static org.elasticsearch.test.ESTestCase.randomAsciiOfLength;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomInt;

/**
 * Utility class the makes random modifications to ShardRouting
 */
public final class RandomShardRoutingMutator {
    private RandomShardRoutingMutator() {

    }

    public static ShardRouting randomChange(ShardRouting shardRouting, String[] nodes) {
        switch (randomInt(2)) {
            case 0:
                if (shardRouting.unassigned() == false && shardRouting.primary() == false) {
                    shardRouting = shardRouting.moveToUnassigned(new UnassignedInfo(randomReason(), randomAsciiOfLength(10)));
                } else if (shardRouting.unassignedInfo() != null) {
                    shardRouting = shardRouting.updateUnassignedInfo(new UnassignedInfo(randomReason(), randomAsciiOfLength(10)));
                }
                break;
            case 1:
                if (shardRouting.unassigned()) {
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
