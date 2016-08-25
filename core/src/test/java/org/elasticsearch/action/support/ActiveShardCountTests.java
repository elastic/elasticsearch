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

package org.elasticsearch.action.support;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Tests for the {@link ActiveShardCount} class
 */
public class ActiveShardCountTests extends ESTestCase {

    public void testFromIntValue() {
        assertSame(ActiveShardCount.from(0), ActiveShardCount.NONE);
        final int value = randomIntBetween(1, 50);
        assertEquals(ActiveShardCount.from(value).toString(), Integer.toString(value));
        expectThrows(IllegalArgumentException.class, () -> ActiveShardCount.from(randomIntBetween(-10, -1)));
    }

    public void testSerialization() throws IOException {
        doWriteRead(ActiveShardCount.ALL);
        doWriteRead(ActiveShardCount.DEFAULT);
        doWriteRead(ActiveShardCount.NONE);
        doWriteRead(ActiveShardCount.from(randomIntBetween(1, 50)));
    }

    public void testParseString() {
        assertSame(ActiveShardCount.parseString("all"), ActiveShardCount.ALL);
        assertSame(ActiveShardCount.parseString(null), ActiveShardCount.DEFAULT);
        assertSame(ActiveShardCount.parseString("0"), ActiveShardCount.NONE);
        int value = randomIntBetween(1, 50);
        assertEquals(ActiveShardCount.parseString(value + ""), ActiveShardCount.from(value));
        expectThrows(IllegalArgumentException.class, () -> ActiveShardCount.parseString(randomAsciiOfLengthBetween(4, 8)));
        expectThrows(IllegalArgumentException.class, () -> ActiveShardCount.parseString("-1")); // magic numbers not exposed through API
        expectThrows(IllegalArgumentException.class, () -> ActiveShardCount.parseString("-2"));
        expectThrows(IllegalArgumentException.class, () -> ActiveShardCount.parseString(randomIntBetween(-10, -3) + ""));
    }

    public void testValidate() {
        assertTrue(ActiveShardCount.parseString("all").validate(randomIntBetween(0, 10)));
        final int numReplicas = randomIntBetween(0, 10);
        assertTrue(ActiveShardCount.from(randomIntBetween(0, numReplicas + 1)).validate(numReplicas));
        // invalid values shouldn't validate
        assertFalse(ActiveShardCount.from(numReplicas + randomIntBetween(2, 10)).validate(numReplicas));
    }

    private void doWriteRead(ActiveShardCount activeShardCount) throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();
        activeShardCount.writeTo(out);
        final ByteBufferStreamInput in = new ByteBufferStreamInput(ByteBuffer.wrap(out.bytes().toBytesRef().bytes));
        ActiveShardCount readActiveShardCount = ActiveShardCount.readFrom(in);
        if (activeShardCount == ActiveShardCount.DEFAULT
                || activeShardCount == ActiveShardCount.ALL
                || activeShardCount == ActiveShardCount.NONE) {
            assertSame(activeShardCount, readActiveShardCount);
        } else {
            assertEquals(activeShardCount, readActiveShardCount);
        }
    }

    public void testEnoughShardsActiveZero() {
        final String indexName = "test-idx";
        final int numberOfShards = randomIntBetween(1, 5);
        final int numberOfReplicas = randomIntBetween(4, 7);
        final ActiveShardCount waitForActiveShards = ActiveShardCount.NONE;
        ClusterState clusterState = initializeWithNewIndex(indexName, numberOfShards, numberOfReplicas);
        assertTrue(waitForActiveShards.enoughShardsActive(clusterState, indexName));
        clusterState = startPrimaries(clusterState, indexName);
        assertTrue(waitForActiveShards.enoughShardsActive(clusterState, indexName));
        clusterState = startAllShards(clusterState, indexName);
        assertTrue(waitForActiveShards.enoughShardsActive(clusterState, indexName));
    }

    public void testEnoughShardsActiveLevelOne() {
        runTestForOneActiveShard(ActiveShardCount.ONE);
    }

    public void testEnoughShardsActiveLevelDefault() {
        // default is 1
        runTestForOneActiveShard(ActiveShardCount.DEFAULT);
    }

    public void testEnoughShardsActiveRandom() {
        final String indexName = "test-idx";
        final int numberOfShards = randomIntBetween(1, 5);
        final int numberOfReplicas = randomIntBetween(4, 7);
        final int activeShardCount = randomIntBetween(2, numberOfReplicas);
        final ActiveShardCount waitForActiveShards = ActiveShardCount.from(activeShardCount);
        ClusterState clusterState = initializeWithNewIndex(indexName, numberOfShards, numberOfReplicas);
        assertFalse(waitForActiveShards.enoughShardsActive(clusterState, indexName));
        clusterState = startPrimaries(clusterState, indexName);
        assertFalse(waitForActiveShards.enoughShardsActive(clusterState, indexName));
        clusterState = startLessThanWaitOnShards(clusterState, indexName, activeShardCount - 2);
        assertFalse(waitForActiveShards.enoughShardsActive(clusterState, indexName));
        clusterState = startWaitOnShards(clusterState, indexName, activeShardCount - 1);
        assertTrue(waitForActiveShards.enoughShardsActive(clusterState, indexName));
        clusterState = startAllShards(clusterState, indexName);
        assertTrue(waitForActiveShards.enoughShardsActive(clusterState, indexName));
    }

    public void testEnoughShardsActiveLevelAll() {
        final String indexName = "test-idx";
        final int numberOfShards = randomIntBetween(1, 5);
        final int numberOfReplicas = randomIntBetween(4, 7);
        // both values should represent "all"
        final ActiveShardCount waitForActiveShards = randomBoolean() ? ActiveShardCount.from(numberOfReplicas + 1) : ActiveShardCount.ALL;
        ClusterState clusterState = initializeWithNewIndex(indexName, numberOfShards, numberOfReplicas);
        assertFalse(waitForActiveShards.enoughShardsActive(clusterState, indexName));
        clusterState = startPrimaries(clusterState, indexName);
        assertFalse(waitForActiveShards.enoughShardsActive(clusterState, indexName));
        clusterState = startLessThanWaitOnShards(clusterState, indexName, numberOfReplicas - randomIntBetween(1, numberOfReplicas));
        assertFalse(waitForActiveShards.enoughShardsActive(clusterState, indexName));
        clusterState = startAllShards(clusterState, indexName);
        assertTrue(waitForActiveShards.enoughShardsActive(clusterState, indexName));
    }

    private void runTestForOneActiveShard(final ActiveShardCount activeShardCount) {
        final String indexName = "test-idx";
        final int numberOfShards = randomIntBetween(1, 5);
        final int numberOfReplicas = randomIntBetween(4, 7);
        assert activeShardCount == ActiveShardCount.ONE || activeShardCount == ActiveShardCount.DEFAULT;
        final ActiveShardCount waitForActiveShards = activeShardCount;
        ClusterState clusterState = initializeWithNewIndex(indexName, numberOfShards, numberOfReplicas);
        assertFalse(waitForActiveShards.enoughShardsActive(clusterState, indexName));
        clusterState = startPrimaries(clusterState, indexName);
        assertTrue(waitForActiveShards.enoughShardsActive(clusterState, indexName));
        clusterState = startAllShards(clusterState, indexName);
        assertTrue(waitForActiveShards.enoughShardsActive(clusterState, indexName));
    }

    private ClusterState initializeWithNewIndex(final String indexName, final int numShards, final int numReplicas) {
        // initial index creation and new routing table info
        final IndexMetaData indexMetaData = IndexMetaData.builder(indexName)
                                                .settings(settings(Version.CURRENT)
                                                              .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()))
                                                .numberOfShards(numShards)
                                                .numberOfReplicas(numReplicas)
                                                .build();
        final MetaData metaData = MetaData.builder().put(indexMetaData, true).build();
        final RoutingTable routingTable = RoutingTable.builder().addAsNew(indexMetaData).build();
        return ClusterState.builder(new ClusterName("test_cluster")).metaData(metaData).routingTable(routingTable).build();
    }

    private ClusterState startPrimaries(final ClusterState clusterState, final String indexName) {
        RoutingTable routingTable = clusterState.routingTable();
        IndexRoutingTable indexRoutingTable = routingTable.index(indexName);
        IndexRoutingTable.Builder newIndexRoutingTable = IndexRoutingTable.builder(indexRoutingTable.getIndex());
        for (final ObjectCursor<IndexShardRoutingTable> shardEntry : indexRoutingTable.getShards().values()) {
            final IndexShardRoutingTable shardRoutingTable = shardEntry.value;
            for (ShardRouting shardRouting : shardRoutingTable.getShards()) {
                if (shardRouting.primary()) {
                    shardRouting = shardRouting.initialize(randomAsciiOfLength(8), null, shardRouting.getExpectedShardSize())
                                       .moveToStarted();
                }
                newIndexRoutingTable.addShard(shardRouting);
            }
        }
        routingTable = RoutingTable.builder(routingTable).add(newIndexRoutingTable).build();
        return ClusterState.builder(clusterState).routingTable(routingTable).build();
    }

    private ClusterState startLessThanWaitOnShards(final ClusterState clusterState, final String indexName, final int numShardsToStart) {
        RoutingTable routingTable = clusterState.routingTable();
        IndexRoutingTable indexRoutingTable = routingTable.index(indexName);
        IndexRoutingTable.Builder newIndexRoutingTable = IndexRoutingTable.builder(indexRoutingTable.getIndex());
        for (final ObjectCursor<IndexShardRoutingTable> shardEntry : indexRoutingTable.getShards().values()) {
            final IndexShardRoutingTable shardRoutingTable = shardEntry.value;
            assert shardRoutingTable.getSize() > 2;
            int numToStart = numShardsToStart;
            // want less than half, and primary is already started
            for (ShardRouting shardRouting : shardRoutingTable.getShards()) {
                if (shardRouting.primary()) {
                    assertTrue(shardRouting.active());
                } else {
                    if (numToStart > 0) {
                        shardRouting = shardRouting.initialize(randomAsciiOfLength(8), null, shardRouting.getExpectedShardSize())
                                           .moveToStarted();
                        numToStart--;
                    }
                }
                newIndexRoutingTable.addShard(shardRouting);
            }
        }
        routingTable = RoutingTable.builder(routingTable).add(newIndexRoutingTable).build();
        return ClusterState.builder(clusterState).routingTable(routingTable).build();
    }

    private ClusterState startWaitOnShards(final ClusterState clusterState, final String indexName, final int numShardsToStart) {
        RoutingTable routingTable = clusterState.routingTable();
        IndexRoutingTable indexRoutingTable = routingTable.index(indexName);
        IndexRoutingTable.Builder newIndexRoutingTable = IndexRoutingTable.builder(indexRoutingTable.getIndex());
        for (final ObjectCursor<IndexShardRoutingTable> shardEntry : indexRoutingTable.getShards().values()) {
            final IndexShardRoutingTable shardRoutingTable = shardEntry.value;
            assert shardRoutingTable.getSize() > 2;
            int numToStart = numShardsToStart;
            for (ShardRouting shardRouting : shardRoutingTable.getShards()) {
                if (shardRouting.primary()) {
                    assertTrue(shardRouting.active());
                } else {
                    if (shardRouting.active() == false) {
                        if (numToStart > 0) {
                            shardRouting = shardRouting.initialize(randomAsciiOfLength(8), null, shardRouting.getExpectedShardSize())
                                               .moveToStarted();
                            numToStart--;
                        }
                    } else {
                        numToStart--;
                    }
                }
                newIndexRoutingTable.addShard(shardRouting);
            }
        }
        routingTable = RoutingTable.builder(routingTable).add(newIndexRoutingTable).build();
        return ClusterState.builder(clusterState).routingTable(routingTable).build();
    }

    private ClusterState startAllShards(final ClusterState clusterState, final String indexName) {
        RoutingTable routingTable = clusterState.routingTable();
        IndexRoutingTable indexRoutingTable = routingTable.index(indexName);
        IndexRoutingTable.Builder newIndexRoutingTable = IndexRoutingTable.builder(indexRoutingTable.getIndex());
        for (final ObjectCursor<IndexShardRoutingTable> shardEntry : indexRoutingTable.getShards().values()) {
            final IndexShardRoutingTable shardRoutingTable = shardEntry.value;
            for (ShardRouting shardRouting : shardRoutingTable.getShards()) {
                if (shardRouting.primary()) {
                    assertTrue(shardRouting.active());
                } else {
                    if (shardRouting.active() == false) {
                        shardRouting = shardRouting.initialize(randomAsciiOfLength(8), null, shardRouting.getExpectedShardSize())
                                           .moveToStarted();
                    }
                }
                newIndexRoutingTable.addShard(shardRouting);
            }
        }
        routingTable = RoutingTable.builder(routingTable).add(newIndexRoutingTable).build();
        return ClusterState.builder(clusterState).routingTable(routingTable).build();
    }

}
