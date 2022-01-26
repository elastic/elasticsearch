/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.assertAllCancellableTasksAreCancelled;
import static org.elasticsearch.test.TaskAssertions.assertAllTasksHaveFinished;
import static org.elasticsearch.test.TaskAssertions.awaitTaskWithPrefix;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

public class ClusterStatsRestCancellationIT extends HttpSmokeTestCase {

    public static final Setting<Boolean> BLOCK_STATS_SETTING = Setting.boolSetting("index.block_stats", false, Setting.Property.IndexScope);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), ClusterStatsRestCancellationIT.StatsBlockingPlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // disable internal cluster info service to avoid internal cluster stats calls
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
            .build();
    }

    public void testClusterStateRestCancellation() throws Exception {

        createIndex("test", Settings.builder().put(BLOCK_STATS_SETTING.getKey(), true).build());
        ensureGreen("test");

        final List<Semaphore> statsBlocks = new ArrayList<>();
        for (final IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            for (final IndexService indexService : indicesService) {
                for (final IndexShard indexShard : indexService) {
                    final Engine engine = IndexShardTestCase.getEngine(indexShard);
                    if (engine instanceof StatsBlockingEngine) {
                        statsBlocks.add(((StatsBlockingEngine) engine).statsBlock);
                    }
                }
            }
        }
        assertThat(statsBlocks, not(empty()));

        final List<Releasable> releasables = new ArrayList<>();
        try {
            for (final Semaphore statsBlock : statsBlocks) {
                statsBlock.acquire();
                releasables.add(statsBlock::release);
            }

            final Request clusterStatsRequest = new Request(HttpGet.METHOD_NAME, "/_cluster/stats");

            final PlainActionFuture<Response> future = new PlainActionFuture<>();
            logger.info("--> sending cluster state request");
            final Cancellable cancellable = getRestClient().performRequestAsync(clusterStatsRequest, wrapAsRestResponseListener(future));

            awaitTaskWithPrefix(ClusterStatsAction.NAME);

            logger.info("--> waiting for at least one task to hit a block");
            assertBusy(() -> assertTrue(statsBlocks.stream().anyMatch(Semaphore::hasQueuedThreads)));

            logger.info("--> cancelling cluster stats request");
            cancellable.cancel();
            expectThrows(CancellationException.class, future::actionGet);

            assertAllCancellableTasksAreCancelled(ClusterStatsAction.NAME);
        } finally {
            Releasables.close(releasables);
        }

        assertAllTasksHaveFinished(ClusterStatsAction.NAME);
    }

    public static class StatsBlockingPlugin extends Plugin implements EnginePlugin {

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            if (BLOCK_STATS_SETTING.get(indexSettings.getSettings())) {
                return Optional.of(StatsBlockingEngine::new);
            }
            return Optional.empty();
        }

        @Override
        public List<Setting<?>> getSettings() {
            return singletonList(BLOCK_STATS_SETTING);
        }
    }

    private static class StatsBlockingEngine extends ReadOnlyEngine {

        final Semaphore statsBlock = new Semaphore(1);

        StatsBlockingEngine(EngineConfig config) {
            super(config, null, new TranslogStats(), true, Function.identity(), true, false);
        }

        @Override
        public SeqNoStats getSeqNoStats(long globalCheckpoint) {
            try {
                statsBlock.acquire();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            statsBlock.release();
            return super.getSeqNoStats(globalCheckpoint);
        }
    }
}
