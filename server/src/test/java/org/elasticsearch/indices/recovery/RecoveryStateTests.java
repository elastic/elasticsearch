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

package org.elasticsearch.indices.recovery;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class RecoveryStateTests extends ESTestCase {

    public void testRecoveryStateIndexToXContent() {
        String expected = "{\"size\":{\"total_in_bytes\":0,\"reused_in_bytes\":0,\"recovered_in_bytes\":0,\"percent\":\"0.0%\"}," +
            "\"files\":{\"total\":0,\"reused\":0,\"recovered\":0,\"percent\":\"0.0%\",\"detailed\":[]}," +
            "\"total_time_in_millis\":0,\"source_throttle_time_in_millis\":0,\"target_throttle_time_in_millis\":0}";
        final DiscoveryNode discoveryNode = new DiscoveryNode("1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        ShardRouting shardRouting = TestShardRouting.newShardRouting(new ShardId("bla", "_na_", 0), discoveryNode.getId(),
            randomBoolean(), ShardRoutingState.INITIALIZING);
        RecoveryState recoveryState = new RecoveryState(shardRouting, discoveryNode,
            shardRouting.recoverySource().getType() == RecoverySource.Type.PEER ? discoveryNode : null);
        ToXContent.MapParams params = new ToXContent.MapParams(Collections.singletonMap(RecoveryState.Fields.DETAILED, "true"));
        String indexRecoveryState = Strings.toString(recoveryState.getIndex(), false, false, params);
        assertEquals(indexRecoveryState, expected);
    }
}
