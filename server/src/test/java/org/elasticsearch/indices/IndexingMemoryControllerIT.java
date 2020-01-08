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

import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardIT;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

public class IndexingMemoryControllerIT extends ESSingleNodeTestCase {

    // #10312
    public void testDeletesAloneCanTriggerRefresh() throws Exception {
        createIndex("index",
                    Settings.builder().put("index.number_of_shards", 1)
                                      .put("index.number_of_replicas", 0)
                                      .put("index.refresh_interval", -1)
                                      .build());
        ensureGreen();

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex("index"));
        IndexShard shard = indexService.getShardOrNull(0);
        assertNotNull(shard);

        for (int i = 0; i < 100; i++) {
            String id = Integer.toString(i);
            client().prepareIndex("index").setId(id).setSource("field", "value").get();
        }

        // Force merge so we know all merges are done before we start deleting:
        ForceMergeResponse r = client().admin().indices().prepareForceMerge().setMaxNumSegments(1).execute().actionGet();
        assertNoFailures(r);

        // Make a shell of an IMC to check up on indexing buffer usage:
        Settings settings = Settings.builder().put("indices.memory.index_buffer_size", "1kb").build();

        // TODO: would be cleaner if I could pass this 1kb setting to the single node this test created....
        IndexingMemoryController imc = new IndexingMemoryController(settings, null, null) {
            @Override
            protected List<IndexShard> availableShards() {
                return Collections.singletonList(shard);
            }

            @Override
            protected long getIndexBufferRAMBytesUsed(IndexShard shard) {
                return shard.getIndexBufferRAMBytesUsed();
            }

            @Override
            protected void writeIndexingBufferAsync(IndexShard shard) {
                // just do it sync'd for this test
                shard.writeIndexingBuffer();
            }

            @Override
            protected Cancellable scheduleTask(ThreadPool threadPool) {
                return null;
            }
        };

        for (int i = 0; i < 100; i++) {
            String id = Integer.toString(i);
            client().prepareDelete("index", id).get();
        }

        final long indexingBufferBytes1 = shard.getIndexBufferRAMBytesUsed();

        imc.forceCheck();

        // We must assertBusy because the writeIndexingBufferAsync is done in background (REFRESH) thread pool:
        assertBusy(() -> {
            try (Engine.Searcher s2 = shard.acquireSearcher("index")) {
                // 100 buffered deletes will easily exceed our 1 KB indexing buffer so it should trigger a write:
                final long indexingBufferBytes2 = shard.getIndexBufferRAMBytesUsed();
                assertTrue(indexingBufferBytes2 < indexingBufferBytes1);
            }
        });
    }

    public void testTranslogRecoveryWorksWithIMC() throws IOException {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = indexService.getShardOrNull(0);
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("{\"foo\" : \"bar\"}", XContentType.JSON).get();
        }

        CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrapper = directoryReader -> directoryReader;
        shard.close("simon says", false);
        AtomicReference<IndexShard> shardRef = new AtomicReference<>();
        Settings settings = Settings.builder().put("indices.memory.index_buffer_size", "50kb").build();
        Iterable<IndexShard> iterable = () -> (shardRef.get() == null) ? Collections.<IndexShard>emptyList().iterator()
            : Collections.singleton(shardRef.get()).iterator();
        AtomicInteger flushes = new AtomicInteger();
        IndexingMemoryController imc = new IndexingMemoryController(settings, client().threadPool(), iterable) {
            @Override
            protected void writeIndexingBufferAsync(IndexShard shard) {
                assertEquals(shard, shardRef.get());
                flushes.incrementAndGet();
                shard.writeIndexingBuffer();
            }
        };
        final IndexShard newShard = IndexShardIT.newIndexShard(indexService, shard, wrapper, new NoneCircuitBreakerService(), imc);
        shardRef.set(newShard);
        try {
            assertEquals(0, imc.availableShards().size());
            ShardRouting routing = newShard.routingEntry();
            DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
            newShard.markAsRecovering("store", new RecoveryState(routing, localNode, null));

            assertEquals(1, imc.availableShards().size());
            assertTrue(IndexShardTestCase.recoverFromStore(newShard));
            assertTrue("we should have flushed in IMC at least once but did: " + flushes.get(), flushes.get() >= 1);
            IndexShardTestCase.updateRoutingEntry(newShard, routing.moveToStarted());
        } finally {
            newShard.close("simon says", false);
        }
    }

}
