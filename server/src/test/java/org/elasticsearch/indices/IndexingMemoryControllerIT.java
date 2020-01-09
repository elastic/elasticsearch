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

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.greaterThan;

public class IndexingMemoryControllerIT extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings())
            // small indexing buffer so that we can trigger refresh after buffering 100 deletes
            .put("indices.memory.index_buffer_size", "1kb").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(TestEnginePlugin.class);
        return plugins;
    }

    public static class TestEnginePlugin extends Plugin implements EnginePlugin {

        EngineConfig engineConfigWithLargerIndexingMemory(EngineConfig config) {
            // We need to set a larger buffer for the IndexWriter; otherwise, it will flush before the IndexingMemoryController.
            Settings settings = Settings.builder().put(config.getIndexSettings().getSettings())
                .put("indices.memory.index_buffer_size", "10mb").build();
            IndexSettings indexSettings = new IndexSettings(config.getIndexSettings().getIndexMetaData(), settings);
            return new EngineConfig(config.getShardId(), config.getAllocationId(), config.getThreadPool(),
                indexSettings, config.getWarmer(), config.getStore(), config.getMergePolicy(), config.getAnalyzer(),
                config.getSimilarity(), new CodecService(null, LogManager.getLogger(IndexingMemoryControllerIT.class)),
                config.getEventListener(), config.getQueryCache(),
                config.getQueryCachingPolicy(), config.getTranslogConfig(), config.getFlushMergesAfter(),
                config.getExternalRefreshListener(), config.getInternalRefreshListener(), config.getIndexSort(),
                config.getCircuitBreakerService(), config.getGlobalCheckpointSupplier(), config.retentionLeasesSupplier(),
                config.getPrimaryTermSupplier(), config.getTombstoneDocSupplier());
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(config -> new InternalEngine(engineConfigWithLargerIndexingMemory(config)));
        }
    }

    // #10312
    public void testDeletesAloneCanTriggerRefresh() throws Exception {
        IndexService indexService = createIndex("index", Settings.builder().put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0).put("index.refresh_interval", -1).build());
        IndexShard shard = indexService.getShard(0);
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("index").setId(Integer.toString(i)).setSource("field", "value").get();
        }
        // Force merge so we know all merges are done before we start deleting:
        ForceMergeResponse r = client().admin().indices().prepareForceMerge().setMaxNumSegments(1).execute().actionGet();
        assertNoFailures(r);
        final RefreshStats refreshStats = shard.refreshStats();
        for (int i = 0; i < 100; i++) {
            client().prepareDelete("index", Integer.toString(i)).get();
        }
        // need to assert busily as IndexingMemoryController refreshes in background
        assertBusy(() -> assertThat(shard.refreshStats().getTotal(), greaterThan(refreshStats.getTotal() + 1)));
    }
}
