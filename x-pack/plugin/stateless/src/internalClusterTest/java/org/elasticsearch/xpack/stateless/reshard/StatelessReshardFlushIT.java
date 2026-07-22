/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;
import org.elasticsearch.xpack.stateless.engine.RefreshManagerService;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.stateless.reshard.SplitSourceService.RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD;

public class StatelessReshardFlushIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(TestStatelessPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        // These tests are carefully set up and do not hit the situations that the delete unowned grace period prevents.
        return super.nodeSettings().put(RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD.getKey(), TimeValue.ZERO);
    }

    private interface FlushCallback {
        // return null to prevent actual flush from being invoked, or wrap the listener to observe its result
        Engine.FlushResultListener onFlush(boolean force, boolean waitIfOngoing, Engine.FlushResultListener listener);
    }

    private static volatile FlushCallback flushCallback = null;

    public void testSourceReleasesPermitsUponFlushFailure() {
        var indexNode = startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        final String indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 1).build());
        Index index = resolveIndex(indexName);
        ensureGreen(indexName);

        int numDocs = randomIntBetween(10, 100);
        indexDocs(indexName, numDocs);

        var splitSourceService = internalCluster().getInstance(SplitSourceService.class, indexNode);
        var setFlushFailureCountdown = new AtomicBoolean(false);
        final AtomicInteger flushFailureCountdown = new AtomicInteger(0);
        final AtomicBoolean flushFailed = new AtomicBoolean(false);
        splitSourceService.setPreHandoffHook(() -> {
            if (setFlushFailureCountdown.getAndSet(true) == false) {
                // Fail the second flush which occurs after acquiring permits
                flushFailureCountdown.set(2);
            }
        });
        var shardId = new ShardId(index, 0);
        flushCallback = (force, waitIfOngoing, listener) -> {
            if (flushFailureCountdown.getAndUpdate(val -> val == 0 ? 0 : val - 1) == 1) {
                flushFailed.set(true);
                if (randomBoolean()) {
                    listener.onFailure(new EngineException(shardId, "test failure"));
                    return null;
                } else {
                    throw new IllegalArgumentException("test flush exception");
                }
            } else {
                return listener;
            }
        };

        logger.info("starting reshard");
        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        awaitClusterState((state) -> state.getMetadata().indexMetadata(index).getReshardingMetadata() == null);
        ensureGreen(indexName);
        refresh(indexName);
        flushCallback = null;

        assertHitCount(
            client().prepareSearch(indexName)
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(10000)
                .setTrackTotalHits(true)
                .setAllowPartialSearchResults(false),
            numDocs
        );
        assertTrue(flushFailed.get());
    }

    public void testPreFlushWaitsForOngoingFlushes() {
        var indexNode = startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        final String indexName = randomIndexName();
        // Background refresh could race with the prehandoff flush being tested here
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        Index index = resolveIndex(indexName);
        ensureGreen(indexName);

        // Trigger an ongoing flush by kicking off a refresh thread, which then blocks flush until we are in the prehandoff flush.
        // Release refresh flush and validate that preflush never skips due to ongoing flush. This isn't foolproof since the
        // refresh flush is unblocked just before preflush invokes the actual flush call, but it catches regression reliably
        // on my laptop.
        var refreshLatch = new CountDownLatch(1);
        var refreshInProgress = new AtomicBoolean(false);
        var preflushResult = new AtomicReference<Engine.FlushResult>();
        flushCallback = (force, waitIfOngoing, listener) -> {
            if (force) {
                // refresh called
                refreshInProgress.set(true);
                safeAwait(refreshLatch);
            } else if (refreshInProgress.get()) {
                // preflush has now started, so refresh can be unblocked
                refreshLatch.countDown();
                refreshInProgress.set(false);

                return new Engine.FlushResultListener() {
                    @Override
                    public void onResponse(Engine.FlushResult flushResult) {
                        preflushResult.set(flushResult);
                        listener.onResponse(flushResult);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                };
            }
            return listener;
        };

        indexDocs(indexName, 200);

        var refreshThread = new Thread(() -> {
            refresh(indexName);
            refreshInProgress.set(false);
        });
        refreshThread.start();

        logger.info("starting reshard");
        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        awaitClusterState((state) -> state.getMetadata().indexMetadata(index).getReshardingMetadata() == null);
        ensureGreen(indexName);
        safeJoin(refreshThread);

        flushCallback = null;
        assertFalse("pre-handoff flush should not have been skipped", preflushResult.get().skippedDueToCollision());
    }

    public static class TestStatelessPlugin extends TestUtils.StatelessPluginWithTrialLicense {
        public TestStatelessPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected IndexEngine newIndexEngine(
            EngineConfig engineConfig,
            TranslogReplicator translogReplicator,
            Function<String, BlobContainer> translogBlobContainer,
            StatelessCommitService statelessCommitService,
            HollowShardsService hollowShardsService,
            SharedBlobCacheWarmingService sharedBlobCacheWarmingService,
            RefreshManagerService refreshManagerService,
            ReshardIndexService reshardIndexService,
            DocumentParsingProvider documentParsingProvider,
            IndexEngine.EngineMetrics engineMetrics
        ) {
            return new IndexEngine(
                engineConfig,
                translogReplicator,
                translogBlobContainer,
                statelessCommitService,
                hollowShardsService,
                sharedBlobCacheWarmingService,
                refreshManagerService,
                reshardIndexService,
                statelessCommitService.getCommitBCCResolverForShard(engineConfig.getShardId()),
                documentParsingProvider,
                engineMetrics,
                statelessCommitService.getShardLocalCommitsTracker(engineConfig.getShardId()).shardLocalReadersTracker()
            ) {
                @Override
                protected void flushHoldingLock(boolean force, boolean waitIfOngoing, FlushResultListener listener) {
                    if (flushCallback != null) {
                        listener = flushCallback.onFlush(force, waitIfOngoing, listener);
                        if (listener == null) {
                            return;
                        }
                    }
                    super.flushHoldingLock(force, waitIfOngoing, listener);
                }
            };
        }
    }
}
