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

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcher;
import org.elasticsearch.xpack.stateless.cache.StatelessOnlinePrewarmingService;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.util.Collection;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.stateless.objectstore.ObjectStoreTestUtils.getObjectStoreMockRepository;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class StatelessPrefetchIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(StatelessOnlinePrewarmingService.STATELESS_ONLINE_PREWARMING_ENABLED.getKey(), false);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockRepository.Plugin.class);
    }

    public void testSearchShardsStarted() throws Exception {
        startMasterAndIndexNode();
        // create large enough cache with small enough pages on the search node to allow prefetching all data
        final String searchNode = startSearchNode(
            Settings.builder()
                .put(
                    SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(),
                    ByteSizeValue.of(randomIntBetween(10, 100), ByteSizeUnit.MB).getStringRep()
                )
                .put(
                    SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(),
                    ByteSizeValue.of(256, ByteSizeUnit.KB).getStringRep()
                )
                // The non-uploaded notification also triggers prefetchLatestCommit
                .put(SearchCommitPrefetcher.PREFETCH_NON_UPLOADED_COMMITS_SETTING.getKey(), true)
                .build()
        );
        final var indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        // Break the idle barrier. Otherwise, prefetching is skipped because the shard is considered idle.
        assertHitCount(client().prepareSearch(indexName).setQuery(new MatchAllQueryBuilder()), 0);

        final ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, searchNode);
        final long prevCompletedStats = getThreadPoolStats(threadPool, StatelessPlugin.PREWARM_THREAD_POOL).completed();

        final int rounds = randomIntBetween(1, 4);
        int totalDocs = 0;
        for (int i = 0; i < rounds; i++) {
            int cnt = randomIntBetween(10, 100);
            indexDocsAndRefresh(indexName, cnt);
            totalDocs += cnt;
            // Wait until the search shard commit generation is updated, ensuring it has received the
            // index shard's commit notification and that prefetch has been triggered for that commit.
            final long expectedGeneration = findIndexShard(indexName).commitStats().getGeneration();
            assertBusy(() -> assertThat(findSearchShard(indexName).commitStats().getGeneration(), equalTo(expectedGeneration)));
        }

        // Wait for the PREWARM pool to drain, to ensure prefetching has completed.
        assertNoRunningAndQueuedTasks(threadPool, StatelessPlugin.PREWARM_THREAD_POOL, prevCompletedStats);

        logger.info("--> blocking repository");
        final var mockRepository = getObjectStoreMockRepository(getObjectStoreService(searchNode));
        mockRepository.setBlockOnAnyFiles();
        logger.info("--> running search and verifying that the repository was not accessed because prefetching warmed the cache already");
        assertHitCount(client().prepareSearch().setQuery(new MatchAllQueryBuilder()), totalDocs);
        assertFalse(mockRepository.blocked());
    }

    private static ThreadPoolStats.Stats getThreadPoolStats(ThreadPool threadPool, String poolName) {
        return threadPool.stats().stats().stream().filter(s -> s.name().equals(poolName)).findFirst().orElseThrow();
    }

    private void assertNoRunningAndQueuedTasks(ThreadPool threadPool, String poolName, long previouslyObservedCompletedTasks)
        throws Exception {
        assertBusy(() -> {
            final var stats = getThreadPoolStats(threadPool, poolName);
            assertThat(stats.completed(), greaterThan(previouslyObservedCompletedTasks));
            assertThat(stats.active() + stats.queue(), equalTo(0));
        });
    }
}
