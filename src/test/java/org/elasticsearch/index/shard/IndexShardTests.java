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
package org.elasticsearch.index.shard;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Simple unit-test IndexShard related operations.
 */
public class IndexShardTests extends ElasticsearchSingleNodeTest {

    public void testFlushOnDeleteSetting() throws Exception {
        boolean initValue = randomBoolean();
        createIndex("test", settingsBuilder().put(IndexShard.INDEX_FLUSH_ON_CLOSE, initValue).build());
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService("test");
        IndexShard shard = test.shard(0);
        assertEquals(initValue, shard.isFlushOnClose());
        final boolean newValue = !initValue;
        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put(IndexShard.INDEX_FLUSH_ON_CLOSE, newValue).build()));
        assertEquals(newValue, shard.isFlushOnClose());

        try {
            assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put(IndexShard.INDEX_FLUSH_ON_CLOSE, "FOOBAR").build()));
            fail("exception expected");
        } catch (ElasticsearchIllegalArgumentException ex) {

        }
        assertEquals(newValue, shard.isFlushOnClose());

    }

    public void testWriteShardState() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            ShardId id = new ShardId("foo", 1);
            long version = between(1, Integer.MAX_VALUE / 2);
            boolean primary = randomBoolean();
            ShardStateMetaData state1 = new ShardStateMetaData(version, primary, "foo");
            write(state1, env.availableShardPaths(id));
            ShardStateMetaData shardStateMetaData = load(logger, env.availableShardPaths(id));
            assertEquals(shardStateMetaData, state1);

            ShardStateMetaData state2 = new ShardStateMetaData(version, primary, "foo");
            write(state2, env.availableShardPaths(id));
            shardStateMetaData = load(logger, env.availableShardPaths(id));
            assertEquals(shardStateMetaData, state1);

            ShardStateMetaData state3 = new ShardStateMetaData(version + 1, primary, "foo");
            write(state3, env.availableShardPaths(id));
            shardStateMetaData = load(logger, env.availableShardPaths(id));
            assertEquals(shardStateMetaData, state3);
            assertEquals("foo", state3.indexUUID);
        }
    }

    public void testPersistenceStateMetadataPersistence() throws Exception {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        NodeEnvironment env = getInstanceFromNode(NodeEnvironment.class);
        IndexService test = indicesService.indexService("test");
        IndexShard shard = test.shard(0);
        ShardStateMetaData shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(getShardStateMetadata(shard), shardStateMetaData);
        ShardRouting routing = new MutableShardRouting(shard.shardRouting, shard.shardRouting.version()+1);
        shard.updateRoutingEntry(routing, true);

        shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(shardStateMetaData, getShardStateMetadata(shard));
        assertEquals(shardStateMetaData, new ShardStateMetaData(routing.version(), routing.primary(), shard.indexSettings.get(IndexMetaData.SETTING_UUID)));

        routing = new MutableShardRouting(shard.shardRouting, shard.shardRouting.version()+1);
        shard.updateRoutingEntry(routing, true);
        shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(shardStateMetaData, getShardStateMetadata(shard));
        assertEquals(shardStateMetaData, new ShardStateMetaData(routing.version(), routing.primary(), shard.indexSettings.get(IndexMetaData.SETTING_UUID)));

        routing = new MutableShardRouting(shard.shardRouting, shard.shardRouting.version()+1);
        shard.updateRoutingEntry(routing, true);
        shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(shardStateMetaData, getShardStateMetadata(shard));
        assertEquals(shardStateMetaData, new ShardStateMetaData(routing.version(), routing.primary(), shard.indexSettings.get(IndexMetaData.SETTING_UUID)));

        // test if we still write it even if the shard is not active
        MutableShardRouting inactiveRouting = new MutableShardRouting(shard.shardRouting.index(), shard.shardRouting.shardId().id(), shard.shardRouting.currentNodeId(), true, ShardRoutingState.INITIALIZING, shard.shardRouting.version() + 1);
        shard.persistMetadata(inactiveRouting, shard.shardRouting);
        shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals("inactive shard state shouldn't be persisted", shardStateMetaData, getShardStateMetadata(shard));
        assertEquals("inactive shard state shouldn't be persisted", shardStateMetaData, new ShardStateMetaData(routing.version(), routing.primary(), shard.indexSettings.get(IndexMetaData.SETTING_UUID)));


        shard.updateRoutingEntry(new MutableShardRouting(shard.shardRouting, shard.shardRouting.version()+1), false);
        shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertFalse("shard state persisted despite of persist=false", shardStateMetaData.equals(getShardStateMetadata(shard)));
        assertEquals("shard state persisted despite of persist=false", shardStateMetaData, new ShardStateMetaData(routing.version(), routing.primary(), shard.indexSettings.get(IndexMetaData.SETTING_UUID)));


        routing = new MutableShardRouting(shard.shardRouting, shard.shardRouting.version()+1);
        shard.updateRoutingEntry(routing, true);
        shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(shardStateMetaData, getShardStateMetadata(shard));
        assertEquals(shardStateMetaData, new ShardStateMetaData(routing.version(), routing.primary(), shard.indexSettings.get(IndexMetaData.SETTING_UUID)));
    }

    public void testDeleteShardState() throws IOException {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        NodeEnvironment env = getInstanceFromNode(NodeEnvironment.class);
        IndexService test = indicesService.indexService("test");
        IndexShard shard = test.shard(0);
        try {
            shard.deleteShardState();
            fail("shard is active metadata delete must fail");
        } catch (ElasticsearchIllegalStateException ex) {
            // fine - only delete if non-active
        }

        ShardRouting routing = shard.routingEntry();
        ShardStateMetaData shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(shardStateMetaData, getShardStateMetadata(shard));

        routing = new MutableShardRouting(shard.shardId.index().getName(), shard.shardId.id(), routing.currentNodeId(), routing.primary(), ShardRoutingState.INITIALIZING, shard.shardRouting.version()+1);
        shard.updateRoutingEntry(routing, true);
        shard.deleteShardState();

        assertNull("no shard state expected after delete on initializing", load(logger, env.availableShardPaths(shard.shardId)));




    }

    ShardStateMetaData getShardStateMetadata(IndexShard shard) {
        ShardRouting shardRouting = shard.routingEntry();
        if (shardRouting == null) {
            return null;
        } else {
            return new ShardStateMetaData(shardRouting.version(), shardRouting.primary(), shard.indexSettings.get(IndexMetaData.SETTING_UUID));
        }
    }

    public void testShardStateMetaHashCodeEquals() {
        ShardStateMetaData meta = new ShardStateMetaData(randomLong(), randomBoolean(), randomRealisticUnicodeOfCodepointLengthBetween(1, 10));

        assertEquals(meta, new ShardStateMetaData(meta.version, meta.primary, meta.indexUUID));
        assertEquals(meta.hashCode(), new ShardStateMetaData(meta.version, meta.primary, meta.indexUUID).hashCode());

        assertFalse(meta.equals(new ShardStateMetaData(meta.version, !meta.primary, meta.indexUUID)));
        assertFalse(meta.equals(new ShardStateMetaData(meta.version+1, meta.primary, meta.indexUUID)));
        assertFalse(meta.equals(new ShardStateMetaData(meta.version, !meta.primary, meta.indexUUID + "foo")));
        Set<Integer> hashCodes = new HashSet<>();
        for (int i = 0; i < 30; i++) { // just a sanity check that we impl hashcode
            meta = new ShardStateMetaData(randomLong(), randomBoolean(), randomRealisticUnicodeOfCodepointLengthBetween(1, 10));
            hashCodes.add(meta.hashCode());
        }
        assertTrue("more than one unique hashcode expected but got: " + hashCodes.size(), hashCodes.size() > 1);

    }

    public static ShardStateMetaData load(ESLogger logger, Path... shardPaths) throws IOException {
        return ShardStateMetaData.FORMAT.loadLatestState(logger, shardPaths);
    }

    public static void write(ShardStateMetaData shardStateMetaData,
                             Path... shardPaths) throws IOException {
        ShardStateMetaData.FORMAT.write(shardStateMetaData, shardStateMetaData.version, shardPaths);
    }
}
