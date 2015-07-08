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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.io.IOException;

public class ShardRoutingTests extends ElasticsearchTestCase {

    public void testFrozenAfterRead() throws IOException {
        ShardRouting routing = TestShardRouting.newShardRouting("foo", 1, "node_1", null, null, false, ShardRoutingState.INITIALIZING, 1);
        routing.moveToPrimary();
        assertTrue(routing.primary());
        routing.moveFromPrimary();
        assertFalse(routing.primary());
        BytesStreamOutput out = new BytesStreamOutput();
        routing.writeTo(out);
        ShardRouting newRouting = ShardRouting.readShardRoutingEntry(StreamInput.wrap(out.bytes()));
        try {
            newRouting.moveToPrimary();
            fail("must be frozen");
        } catch (IllegalStateException ex) {
            // expected
        }
    }


    public void testFrozenOnRoutingTable() {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();
        for (ShardRouting routing : clusterState.routingTable().allShards()) {
            long version = routing.version();
            assertTrue(routing.isFrozen());
            try {
                routing.moveToPrimary();
                fail("must be frozen");
            } catch (IllegalStateException ex) {
                // expected
            }
            try {
                routing.moveToStarted();
                fail("must be frozen");
            } catch (IllegalStateException ex) {
                // expected
            }

            try {
                routing.moveFromPrimary();
                fail("must be frozen");
            } catch (IllegalStateException ex) {
                // expected
            }

            try {
                routing.assignToNode("boom");
                fail("must be frozen");
            } catch (IllegalStateException ex) {
                // expected
            }
            try {
                routing.cancelRelocation();
                fail("must be frozen");
            } catch (IllegalStateException ex) {
                // expected
            }
            try {
                routing.moveToUnassigned(new UnassignedInfo(UnassignedInfo.Reason.REPLICA_ADDED, "foobar"));
                fail("must be frozen");
            } catch (IllegalStateException ex) {
                // expected
            }

            try {
                routing.relocate("foobar");
                fail("must be frozen");
            } catch (IllegalStateException ex) {
                // expected
            }
            try {
                routing.reinitializeShard();
                fail("must be frozen");
            } catch (IllegalStateException ex) {
                // expected
            }
            assertEquals(version, routing.version());
        }
    }
}
