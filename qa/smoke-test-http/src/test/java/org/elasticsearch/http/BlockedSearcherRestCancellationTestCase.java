/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.ReadOnlyEngine;
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

/**
 * Base class for testing that cancellation works at the REST layer for requests that need to acquire a searcher on one or more shards.
 *
 * It works by blocking searcher acquisition in order to catch the request mid-execution, and then to check that all the tasks are cancelled
 * before they complete normally.
 */
public abstract class BlockedSearcherRestCancellationTestCase extends HttpSmokeTestCase {

    private static final Setting<Boolean> BLOCK_SEARCHER_SETTING = Setting.boolSetting(
        "index.block_searcher",
        false,
        Setting.Property.IndexScope
    );

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), SearcherBlockingPlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    void runTest(Request request, String actionPrefix) throws Exception {

        createIndex("test", Settings.builder().put(BLOCK_SEARCHER_SETTING.getKey(), true).build());
        ensureGreen("test");

        final List<Semaphore> searcherBlocks = new ArrayList<>();
        for (final IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            for (final IndexService indexService : indicesService) {
                for (final IndexShard indexShard : indexService) {
                    final Engine engine = IndexShardTestCase.getEngine(indexShard);
                    if (engine instanceof SearcherBlockingEngine searcherBlockingEngine) {
                        searcherBlocks.add(searcherBlockingEngine.searcherBlock);
                    }
                }
            }
        }
        assertThat(searcherBlocks, not(empty()));
        final List<Releasable> releasables = new ArrayList<>();
        try {
            for (final Semaphore searcherBlock : searcherBlocks) {
                searcherBlock.acquire();
                releasables.add(searcherBlock::release);
            }

            final PlainActionFuture<Response> future = new PlainActionFuture<>();
            logger.info("--> sending request");
            final Cancellable cancellable = getRestClient().performRequestAsync(request, wrapAsRestResponseListener(future));

            awaitTaskWithPrefix(actionPrefix);

            logger.info("--> waiting for at least one task to hit a block");
            assertBusy(() -> assertTrue(searcherBlocks.stream().anyMatch(Semaphore::hasQueuedThreads)));

            logger.info("--> cancelling request");
            cancellable.cancel();
            expectThrows(CancellationException.class, future::actionGet);

            assertAllCancellableTasksAreCancelled(actionPrefix);
        } finally {
            Releasables.close(releasables);
        }

        assertAllTasksHaveFinished(actionPrefix);
    }

    public static class SearcherBlockingPlugin extends Plugin implements EnginePlugin {

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            if (BLOCK_SEARCHER_SETTING.get(indexSettings.getSettings())) {
                return Optional.of(SearcherBlockingEngine::new);
            }
            return Optional.empty();
        }

        @Override
        public List<Setting<?>> getSettings() {
            return singletonList(BLOCK_SEARCHER_SETTING);
        }
    }

    private static class SearcherBlockingEngine extends ReadOnlyEngine {

        final Semaphore searcherBlock = new Semaphore(1);

        SearcherBlockingEngine(EngineConfig config) {
            super(config, null, new TranslogStats(), true, Function.identity(), true, false);
        }

        @Override
        public Searcher acquireSearcher(String source, SearcherScope scope, Function<Searcher, Searcher> wrapper) throws EngineException {
            try {
                searcherBlock.acquire();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            searcherBlock.release();
            return super.acquireSearcher(source, scope, wrapper);
        }
    }

}
