/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.cache;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;

import static co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils.getObjectStoreMockRepository;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

public class SharedBlobCacheWarmingServiceIT extends AbstractStatelessIntegTestCase {

    private static final ByteSizeValue REGION_SIZE = ByteSizeValue.ofBytes(4096L);
    private static final ByteSizeValue CACHE_SIZE = ByteSizeValue.ofBytes(4096L * 2048L);

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(TestStateless.class);
        plugins.add(MockRepository.Plugin.class);
        plugins.add(InternalSettingsPlugin.class);
        plugins.add(ShutdownPlugin.class);
        return plugins;
    }

    public void testCacheIsWarmedBeforeIndexingShardRelocation() {
        startMasterOnlyNode();

        var cacheSettings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), CACHE_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE.getStringRep())
            .build();
        var indexNodeA = startIndexNode(cacheSettings);

        final String indexName = randomIdentifier();
        assertAcked(
            prepareCreate(
                indexName,
                Settings.builder()
                    .put(MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.getKey(), 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(EngineConfig.USE_COMPOUND_FILE, randomBoolean())
            )
        );

        indexDocs(indexName, randomIntBetween(100, 10000));
        refresh(indexName);

        var indexNodeB = startIndexNode(cacheSettings);
        ensureStableCluster(3);

        blockObjectStoreRepository(indexName, indexNodeB);

        var shutdownNodeId = client().admin().cluster().prepareState().get().getState().nodes().resolveNode(indexNodeA).getId();
        assertAcked(
            client().execute(
                PutShutdownNodeAction.INSTANCE,
                new PutShutdownNodeAction.Request(
                    shutdownNodeId,
                    SingleNodeShutdownMetadata.Type.SIGTERM,
                    "Shutdown for cache warming test",
                    null,
                    null,
                    TimeValue.timeValueMinutes(randomIntBetween(1, 5))
                )
            )
        );
        ensureGreen(indexName);
        assertThat(findIndexShard(resolveIndex(indexName), 0).routingEntry().currentNodeId(), equalTo(getNodeId(indexNodeB)));
    }

    public void testCacheIsWarmedBeforeSearchShardRecovery() {
        startMasterOnlyNode();

        var cacheSettings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), CACHE_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE.getStringRep())
            .build();
        startIndexNode(cacheSettings);

        var searchNodeA = startSearchNode(cacheSettings);
        ensureStableCluster(3);

        final String indexName = randomIdentifier();
        assertAcked(
            prepareCreate(
                indexName,
                Settings.builder()
                    .put(MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.getKey(), 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(EngineConfig.USE_COMPOUND_FILE, randomBoolean())
            )
        );

        int numDocs = randomIntBetween(100, 10000);
        indexDocs(indexName, numDocs);
        flushAndRefresh(indexName);
        ensureGreen(indexName);

        // Verify that we performed pre-warming and don't need to hit the object store on searches
        blockObjectStoreRepository(indexName, searchNodeA);

        setReplicaCount(1, indexName);
        ensureGreen(indexName);
        assertHitCount(prepareSearch(indexName), numDocs);

        var searchNodeB = startSearchNode(cacheSettings);
        ensureStableCluster(4);

        // The cache also gets pre-warmed when a shard gets relocated to a new node
        blockObjectStoreRepository(indexName, searchNodeB);
        shutdownNode(searchNodeA);
        ensureGreen(indexName);
        assertThat(findSearchShard(resolveIndex(indexName), 0).routingEntry().currentNodeId(), equalTo(getNodeId(searchNodeB)));
        assertHitCount(prepareSearch(indexName), numDocs);
    }

    private static void shutdownNode(String indexNode) {
        var shutdownNodeId = client().admin().cluster().prepareState().get().getState().nodes().resolveNode(indexNode).getId();
        assertAcked(
            client().execute(
                PutShutdownNodeAction.INSTANCE,
                new PutShutdownNodeAction.Request(
                    shutdownNodeId,
                    SingleNodeShutdownMetadata.Type.SIGTERM,
                    "Shutdown for cache warming test",
                    null,
                    null,
                    TimeValue.timeValueMinutes(randomIntBetween(1, 5))
                )
            )
        );
    }

    private void blockObjectStoreRepository(String indexName, String node) {
        final long generationToBlock = getShardEngine(findIndexShard(indexName), IndexEngine.class).getCurrentGeneration();
        final var warmingService = (BlockingSharedBlobCacheWarmingService) internalCluster().getInstance(PluginsService.class, node)
            .filterPlugins(TestStateless.class)
            .findFirst()
            .orElseThrow(() -> new AssertionError(TestStateless.class.getName() + " plugin not found"))
            .getSharedBlobCacheWarmingService();

        final var mockRepositoryB = getObjectStoreMockRepository(internalCluster().getInstance(ObjectStoreService.class, node));
        warmingService.addListener(ActionListener.running(() -> {
            logger.info("--> block object store repository after warming");
            mockRepositoryB.setRandomControlIOExceptionRate(1.0);
            mockRepositoryB.setRandomDataFileIOExceptionRate(1.0);
            mockRepositoryB.setMaximumNumberOfFailures(Long.MAX_VALUE);
            mockRepositoryB.setRandomIOExceptionPattern(".*" + StatelessCompoundCommit.blobNameFromGeneration(generationToBlock) + ".*");
        }));
    }

    public static class TestStateless extends Stateless {

        public TestStateless(Settings settings) {
            super(settings);
        }

        @Override
        protected SharedBlobCacheWarmingService createSharedBlobCacheWarmingService(
            StatelessSharedBlobCacheService cacheService,
            ThreadPool threadPool
        ) {
            return new BlockingSharedBlobCacheWarmingService(cacheService, threadPool);
        }
    }

    private static class BlockingSharedBlobCacheWarmingService extends SharedBlobCacheWarmingService {

        private final CopyOnWriteArrayList<ActionListener<Void>> listeners = new CopyOnWriteArrayList<>();

        BlockingSharedBlobCacheWarmingService(StatelessSharedBlobCacheService cacheService, ThreadPool threadPool) {
            super(cacheService, threadPool);
        }

        void addListener(ActionListener<Void> listener) {
            listeners.add(listener);
        }

        @Override
        protected void warmCache(IndexShard indexShard, StatelessCompoundCommit commit, ActionListener<Void> listener) {
            var wrappedListener = new SubscribableListener<Void>();
            for (ActionListener<Void> voidActionListener : listeners) {
                wrappedListener.addListener(voidActionListener);
            }
            wrappedListener.addListener(listener); // completed last
            super.warmCache(indexShard, commit, wrappedListener);
            safeAwait(wrappedListener);
        }
    }
}
