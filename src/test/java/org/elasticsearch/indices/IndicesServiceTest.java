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
package org.elasticsearch.indices;

import org.apache.lucene.store.LockObtainFailedException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class IndicesServiceTest extends ElasticsearchSingleNodeTest {

    public IndicesService getIndicesService() {
        return getInstanceFromNode(IndicesService.class);
    }
    public NodeEnvironment getNodeEnvironment() {
        return getInstanceFromNode(NodeEnvironment.class);
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    public void testCanDeleteShardContent() {
        IndicesService indicesService = getIndicesService();
        IndexMetaData meta = IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(
                1).build();
        assertFalse("no shard location", indicesService.canDeleteShardContent(new ShardId("test", 0), meta));
        IndexService test = createIndex("test");
        assertTrue(test.hasShard(0));
        assertFalse("shard is allocated", indicesService.canDeleteShardContent(new ShardId("test", 0), meta));
        test.removeShard(0, "boom");
        assertTrue("shard is removed", indicesService.canDeleteShardContent(new ShardId("test", 0), meta));
    }

    public void testDeleteIndexStore() throws Exception {
        IndicesService indicesService = getIndicesService();
        IndexService test = createIndex("test");
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        IndexMetaData firstMetaData = clusterService.state().metaData().index("test");
        assertTrue(test.hasShard(0));

        try {
            indicesService.deleteIndexStore("boom", firstMetaData, clusterService.state());
            fail();
        } catch (ElasticsearchIllegalStateException ex) {
            // all good
        }

        GatewayMetaState gwMetaState = getInstanceFromNode(GatewayMetaState.class);
        MetaData meta = gwMetaState.loadMetaState();
        assertNotNull(meta);
        assertNotNull(meta.index("test"));
        assertAcked(client().admin().indices().prepareDelete("test"));

        meta = gwMetaState.loadMetaState();
        assertNotNull(meta);
        assertNull(meta.index("test"));


        test = createIndex("test");
        client().prepareIndex("test", "type", "1").setSource("field", "value").setRefresh(true).get();
        client().admin().indices().prepareFlush("test").get();
        assertHitCount(client().prepareSearch("test").get(), 1);
        IndexMetaData secondMetaData = clusterService.state().metaData().index("test");
        assertAcked(client().admin().indices().prepareClose("test"));
        ShardPath path = ShardPath.loadShardPath(logger, getNodeEnvironment(), new ShardId(test.index(), 0), test.getIndexSettings());
        assertTrue(path.exists());

        try {
            indicesService.deleteIndexStore("boom", secondMetaData, clusterService.state());
            fail();
        } catch (ElasticsearchIllegalStateException ex) {
            // all good
        }

        assertTrue(path.exists());

        // now delete the old one and make sure we resolve against the name
        try {
            indicesService.deleteIndexStore("boom", firstMetaData, clusterService.state());
            fail();
        } catch (ElasticsearchIllegalStateException ex) {
            // all good
        }
        assertAcked(client().admin().indices().prepareOpen("test"));
        ensureGreen("test");
    }

    public void testPendingTasks() throws IOException {
        IndicesService indicesService = getIndicesService();
        IndexService test = createIndex("test");

        assertTrue(test.hasShard(0));
        ShardPath path = test.shard(0).shardPath();
        assertTrue(test.shard(0).routingEntry().started());
        ShardPath shardPath = ShardPath.loadShardPath(logger, getNodeEnvironment(), new ShardId(test.index(), 0), test.getIndexSettings());
        assertEquals(shardPath, path);
        try {
            indicesService.processPendingDeletes(test.index(), test.getIndexSettings(), new TimeValue(0, TimeUnit.MILLISECONDS));
            fail("can't get lock");
        } catch (LockObtainFailedException ex) {

        }
        assertTrue(path.exists());

        int numPending = 1;
        if (randomBoolean()) {
            indicesService.addPendingDelete(new ShardId(test.index(), 0), test.getIndexSettings());
        } else {
            if (randomBoolean()) {
                numPending++;
                indicesService.addPendingDelete(new ShardId(test.index(), 0), test.getIndexSettings());
            }
            indicesService.addPendingDelete(test.index(), test.getIndexSettings());
        }
        assertAcked(client().admin().indices().prepareClose("test"));
        assertTrue(path.exists());

        assertEquals(indicesService.numPendingDeletes(test.index()), numPending);

        // shard lock released... we can now delete
        indicesService.processPendingDeletes(test.index(), test.getIndexSettings(), new TimeValue(0, TimeUnit.MILLISECONDS));
        assertEquals(indicesService.numPendingDeletes(test.index()), 0);
        assertFalse(path.exists());

        if (randomBoolean()) {
            indicesService.addPendingDelete(new ShardId(test.index(), 0), test.getIndexSettings());
            indicesService.addPendingDelete(new ShardId(test.index(), 1), test.getIndexSettings());
            indicesService.addPendingDelete(new ShardId("bogus", 1), test.getIndexSettings());
            assertEquals(indicesService.numPendingDeletes(test.index()), 2);
            // shard lock released... we can now delete
            indicesService.processPendingDeletes(test.index(),  test.getIndexSettings(), new TimeValue(0, TimeUnit.MILLISECONDS));
            assertEquals(indicesService.numPendingDeletes(test.index()), 0);
        }
        assertAcked(client().admin().indices().prepareOpen("test"));

    }
}
