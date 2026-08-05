/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.HollowShardsMetrics;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_TTL;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;

public class HollowShardsServiceUnhollowRaceIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(RemoveHollowShardInterceptPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        // the base class randomly disables hollow shards, but this test requires them
        return super.nodeSettings().put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true);
    }

    /**
     * Reproduces the race behind the {@code expect last commit hollow to be [false] but got [true]} failure of
     * https://github.com/elastic/elasticsearch/issues/154655: the unhollowing flush listener in {@link HollowShardsService}
     * asserts that the engine's last commit is not hollow. Until the listener calls {@code removeHollowShard}, the shard is in
     * the hollow shards map and the ingestion op that triggered the unhollowing holds a primary permit, so nothing can hollow
     * the engine concurrently. Once {@code removeHollowShard} has run, both protections are gone: the pending ingestion
     * completes and releases its permit, and a relocation may then acquire all permits and re-hollow the very engine instance
     * that the listener captured, via {@code IndexEngine#prepareForEngineReset} and {@code flushHollow}. The assertion must
     * therefore run before {@code removeHollowShard}; this test fails with the assertion error above if it is moved after.
     *
     * The interleaving is forced deterministically: a {@link HollowShardsService} subclass parks the unhollowing listener
     * thread right after {@code removeHollowShard}, and the test drives a re-hollowing relocation to completion before
     * releasing the listener.
     */
    public void testUnhollowFlushListenerDoesNotRaceWithRehollowingRelocation() throws Exception {
        startMasterOnlyNode();
        final var indexNodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .build();
        final String nodeA = startIndexNode(indexNodeSettings);
        final String nodeB = startIndexNode(indexNodeSettings);

        final var indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_name", nodeA).build()
        );
        ensureGreen(indexName);
        indexDocs(indexName, randomIntBetween(8, 32));
        flush(indexName);
        final var index = resolveIndex(indexName);

        // Relocate the shard to nodeB once it is hollowable, which hollows it
        final var hollowShardsServiceA = internalCluster().getInstance(HollowShardsService.class, nodeA);
        assertBusy(() -> assertTrue(hollowShardsServiceA.isHollowableIndexShard(findIndexShard(index, 0))));
        updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_name", nodeB), indexName);
        ensureGreen(indexName);
        final var shardId = findIndexShard(index, 0).shardId();
        final var hollowShardsServiceB = internalCluster().getInstance(HollowShardsService.class, nodeB);
        hollowShardsServiceB.ensureHollowShard(shardId, true);

        // Park the unhollowing flush listener right after removeHollowShard, in the middle of the window in which a
        // re-hollowing relocation can run concurrently
        final var listenerParked = new CountDownLatch(1);
        final var listenerReleased = new CountDownLatch(1);
        findPlugin(nodeB, RemoveHollowShardInterceptPlugin.class).afterUnhollowRemoveHollowShard.put(shardId, () -> {
            listenerParked.countDown();
            safeAwait(listenerReleased, TimeValue.timeValueMinutes(1));
        });

        // Trigger unhollowing. The indexing completes as soon as removeHollowShard releases the ingestion blocker, i.e.
        // while the unhollowing listener is still parked.
        indexDocs(indexName, 1);
        safeAwait(listenerParked, TimeValue.timeValueMinutes(1));
        try {
            // Relocate the shard back to nodeA. The source-side hollowing calls prepareForEngineReset -> flushHollow on the
            // very IndexEngine instance the parked listener captured, making its last commit hollow again.
            assertBusy(() -> assertTrue(hollowShardsServiceB.isHollowableIndexShard(findIndexShard(index, 0))));
            updateIndexSettings(
                Settings.builder().put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_name", nodeA),
                indexName
            );
            ensureGreen(indexName);
        } finally {
            listenerReleased.countDown();
        }

        // With the assertion misplaced after removeHollowShard, the released listener now fails its assertion on nodeB and
        // the test fails with an uncaught "expect last commit hollow to be [false] but got [true]" AssertionError.
        // As a final sanity check, unhollow the shard on nodeA again by ingesting into it.
        indexDocs(indexName, 1);
        ensureGreen(indexName);
    }

    /**
     * Installs a {@link HollowShardsService} whose {@code removeHollowShard} runs a one-shot per-shard hook after removing an
     * unhollowed shard from the hollow shards map. Hooks only fire for the unhollowing flush listener's removal (identified by
     * its reason), not for removals due to shard closure or failed unhollowing.
     */
    public static class RemoveHollowShardInterceptPlugin extends TestUtils.StatelessPluginWithTrialLicense {
        final Map<ShardId, Runnable> afterUnhollowRemoveHollowShard = new ConcurrentHashMap<>();

        public RemoveHollowShardInterceptPlugin(Settings settings) {
            super(settings);
        }

        @Override
        public Collection<Object> createComponents(Plugin.PluginServices services) {
            // The HollowShardsService subclass below is bound by its anonymous class, so it must be explicitly bound to
            // HollowShardsService for the components that get it injected by that type.
            final Collection<Object> components = super.createComponents(services);
            components.add(
                new PluginComponentBinding<>(
                    HollowShardsService.class,
                    (HollowShardsService) components.stream().filter(c -> c instanceof HollowShardsService).findFirst().orElseThrow()
                )
            );
            return components;
        }

        @Override
        protected HollowShardsService createHollowShardsService(
            Settings settings,
            ClusterService clusterService,
            IndicesService indicesService,
            ObjectStoreService objectStoreService,
            StatelessCommitService commitService,
            IndexShardCacheWarmer indexShardCacheWarmer,
            ThreadPool threadPool,
            HollowShardsMetrics metrics,
            Executor bccHeaderReadExecutor
        ) {
            return new HollowShardsService(
                settings,
                clusterService,
                indicesService,
                objectStoreService,
                commitService,
                indexShardCacheWarmer,
                threadPool,
                metrics,
                bccHeaderReadExecutor
            ) {
                @Override
                public void removeHollowShard(IndexShard indexShard, String reason) {
                    super.removeHollowShard(indexShard, reason);
                    if (reason.startsWith("unhollowing gen")) {
                        final var hook = afterUnhollowRemoveHollowShard.remove(indexShard.shardId());
                        if (hook != null) {
                            hook.run();
                        }
                    }
                }
            };
        }
    }
}
