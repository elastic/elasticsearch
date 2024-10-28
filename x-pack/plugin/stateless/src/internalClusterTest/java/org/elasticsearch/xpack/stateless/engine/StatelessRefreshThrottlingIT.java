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

package co.elastic.elasticsearch.stateless.engine;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class StatelessRefreshThrottlingIT extends AbstractStatelessIntegTestCase {

    private RefreshNodeCreditManager getRegularIndicesCreditManager(String indexingNode) {
        var refreshThrottlingService = internalCluster().getInstance(RefreshThrottlingService.class, indexingNode);
        return refreshThrottlingService.getRegularIndicesCreditManager();
    }

    private RefreshNodeCreditManager getSystemIndicesCreditManager(String indexingNode) {
        var refreshThrottlingService = internalCluster().getInstance(RefreshThrottlingService.class, indexingNode);
        return refreshThrottlingService.getSystemIndicesCreditManager();
    }

    private RefreshThrottler getRefreshThrottler(String indexingNode, String indexName, int shard) {
        Index index = resolveIndices().entrySet().stream().filter(e -> e.getKey().getName().equals(indexName)).findAny().get().getKey();
        var indexService = internalCluster().getInstance(IndicesService.class, indexingNode).indexService(index);
        assertTrue(indexService.hasShard(shard));
        var indexShard = indexService.getShard(shard);
        assertThat(indexShard.getEngineOrNull(), instanceOf(IndexEngine.class));
        var indexEngine = ((IndexEngine) indexShard.getEngineOrNull());
        return indexEngine.getRefreshThrottler();
    }

    private RefreshBurstableThrottler getRefreshBurstableThrottler(String indexingNode, String indexName, int shard) {
        RefreshThrottler refreshThrottler = getRefreshThrottler(indexingNode, indexName, shard);
        assertThat(refreshThrottler, instanceOf(RefreshBurstableThrottler.class));
        return (RefreshBurstableThrottler) refreshThrottler;
    }

    private long getAcceptedRefreshes(RefreshBurstableThrottler refreshThrottler) {
        Long value = refreshThrottler.getAcceptedPerSourceStats().get("api");
        return value == null ? 0 : value;
    }

    private long getThrottledRefreshes(RefreshBurstableThrottler refreshThrottler) {
        Long value = refreshThrottler.getThrottledPerSourceStats().get("api");
        return value == null ? 0 : value;
    }

    public void testShardRefreshThrottling() throws Exception {
        String indexNode = startMasterAndIndexNode();
        boolean useSystemIndex = randomBoolean();
        final String indexName = useSystemIndex ? SYSTEM_INDEX_NAME : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        if (useSystemIndex) {
            createSystemIndex(indexSettings(1, 0).build());
        } else {
            createIndex(indexName, indexSettings(1, 0).build());
        }
        ensureGreen(indexName);

        RefreshBurstableThrottler refreshThrottler = getRefreshBurstableThrottler(indexNode, indexName, 0);
        indexDocs(indexName, randomIntBetween(1, 5));
        refresh(indexName);

        assertBusy(() -> {
            assertThat(getAcceptedRefreshes(refreshThrottler), equalTo(1L));
            assertThat(getThrottledRefreshes(refreshThrottler), equalTo(0L));
        });

        // Trigger throttler's credit to 0
        refreshThrottler.setCredit(0);

        indexDocs(indexName, randomIntBetween(1, 5));
        refresh(indexName);

        assertBusy(() -> {
            assertThat(getAcceptedRefreshes(refreshThrottler), equalTo(1L));
            assertThat(getThrottledRefreshes(refreshThrottler), equalTo(1L));
        });
    }

    public void testNodeRegularIndicesRefreshThrottling() throws Exception {
        String indexNode = startMasterAndIndexNode();

        int indices = randomIntBetween(1, 3);
        List<String> indicesNames = new ArrayList<>(indices);
        List<RefreshBurstableThrottler> shardThrottlers = new ArrayList<>(indices);
        for (int i = 0; i < indices; i++) {
            final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            createIndex(indexName, indexSettings(1, 0).build());
            ensureGreen(indexName);
            indicesNames.add(indexName);
            shardThrottlers.add(getRefreshBurstableThrottler(indexNode, indexName, 0));
            indexDocs(indicesNames.get(i), randomIntBetween(1, 5));
            refresh(indicesNames.get(i));
        }

        // System index refreshes should be unaffected in this test
        createSystemIndex(indexSettings(1, 0).build());
        indexDocs(SYSTEM_INDEX_NAME, randomIntBetween(1, 5));
        refresh(SYSTEM_INDEX_NAME);
        RefreshBurstableThrottler systemIndexThrottler = getRefreshBurstableThrottler(indexNode, SYSTEM_INDEX_NAME, 0);

        RefreshNodeCreditManager nodeRegularIndicesCreditManager = getRegularIndicesCreditManager(indexNode);
        // Trigger throttler's credit to number of indices - 1
        nodeRegularIndicesCreditManager.setCredit(indices - 1);

        // System index refresh should go on unhindered and should not affect later regular index refreshes
        indexDocs(SYSTEM_INDEX_NAME, randomIntBetween(1, 5));
        refresh(SYSTEM_INDEX_NAME);
        assertBusy(() -> {
            assertThat(getAcceptedRefreshes(systemIndexThrottler), equalTo(2L));
            assertThat(getThrottledRefreshes(systemIndexThrottler), equalTo(0L));
        });

        // Last regular index's refresh should be throttled
        for (int i = 0; i < indices; i++) {
            indexDocs(indicesNames.get(i), randomIntBetween(1, 5));
            refresh(indicesNames.get(i));
            final int final_i = i;
            assertBusy(() -> {
                if (final_i < indices - 1) {
                    assertThat(getAcceptedRefreshes(shardThrottlers.get(final_i)), equalTo(2L));
                    assertThat(getThrottledRefreshes(shardThrottlers.get(final_i)), equalTo(0L));
                } else {
                    assertThat(getAcceptedRefreshes(shardThrottlers.get(final_i)), equalTo(1L));
                    assertThat(getThrottledRefreshes(shardThrottlers.get(final_i)), equalTo(1L));
                }
            });
        }
    }

    public void testNodeSystemIndicesRefreshThrottling() throws Exception {
        String indexNode = startMasterAndIndexNode();

        int indices = randomIntBetween(1, 3);
        List<String> indicesNames = new ArrayList<>(indices);
        List<RefreshBurstableThrottler> shardThrottlers = new ArrayList<>(indices);
        for (int i = 0; i < indices; i++) {
            final String indexName = SYSTEM_INDEX_NAME + i;
            createIndex(indexName, indexSettings(1, 0).build());
            ensureGreen(indexName);
            indicesNames.add(indexName);
            shardThrottlers.add(getRefreshBurstableThrottler(indexNode, indexName, 0));
            indexDocs(indicesNames.get(i), randomIntBetween(1, 5));
            refresh(indicesNames.get(i));
        }

        // Regular index refreshes should be unaffected in this test
        String regularIndexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(regularIndexName, indexSettings(1, 0).build());
        indexDocs(regularIndexName, randomIntBetween(1, 5));
        refresh(regularIndexName);
        RefreshBurstableThrottler regularIndexThrottler = getRefreshBurstableThrottler(indexNode, regularIndexName, 0);

        RefreshNodeCreditManager nodeSystemIndicesCreditManager = getSystemIndicesCreditManager(indexNode);
        // Trigger throttler's credit to number of indices - 1
        nodeSystemIndicesCreditManager.setCredit(indices - 1);

        // Regular index refresh should go on unhindered and should not affect later system index refreshes
        indexDocs(regularIndexName, randomIntBetween(1, 5));
        refresh(regularIndexName);
        assertBusy(() -> {
            assertThat(getAcceptedRefreshes(regularIndexThrottler), equalTo(2L));
            assertThat(getThrottledRefreshes(regularIndexThrottler), equalTo(0L));
        });

        // Last system index's refresh should be throttled
        for (int i = 0; i < indices; i++) {
            indexDocs(indicesNames.get(i), randomIntBetween(1, 5));
            refresh(indicesNames.get(i));
            final int final_i = i;
            assertBusy(() -> {
                if (final_i < indices - 1) {
                    assertThat(getAcceptedRefreshes(shardThrottlers.get(final_i)), equalTo(2L));
                    assertThat(getThrottledRefreshes(shardThrottlers.get(final_i)), equalTo(0L));
                } else {
                    assertThat(getAcceptedRefreshes(shardThrottlers.get(final_i)), equalTo(1L));
                    assertThat(getThrottledRefreshes(shardThrottlers.get(final_i)), equalTo(1L));
                }
            });
        }
    }

    public void testSystemIndexWithFastRefreshIsNotThrottled() {
        String indexNode = startMasterAndIndexNode();
        createSystemIndex(indexSettings(1, 0).put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), true).build());
        indexDoc(SYSTEM_INDEX_NAME, "1", "field", "value");
        RefreshThrottler shardThrottler = getRefreshThrottler(indexNode, SYSTEM_INDEX_NAME, 0);
        assertThat(shardThrottler, instanceOf(RefreshThrottler.Noop.class));
    }

    public void testRunPendingRefreshAfterShardIsClosed() throws Exception {
        String indexNode = startMasterAndIndexNode();
        boolean useSystemIndex = randomBoolean();
        final String indexName = useSystemIndex ? SYSTEM_INDEX_NAME : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        if (useSystemIndex) {
            createSystemIndex(indexSettings(1, 0).build());
        } else {
            createIndex(indexName, indexSettings(1, 0).build());
        }

        var latch = new CountDownLatch(1);
        var threadPool = internalCluster().getInstance(ThreadPool.class, indexNode);
        for (int i = 0; i < threadPool.info(ThreadPool.Names.REFRESH).getMax(); i++) {
            threadPool.executor(ThreadPool.Names.REFRESH).execute(new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                    latch.await();
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }
            });
        }

        var refreshThrottler = getRefreshBurstableThrottler(indexNode, indexName, 0);
        refreshThrottler.setCredit(0);

        indexDocs(indexName, randomIntBetween(1, 5));

        var refreshFuture = new PlainActionFuture<Engine.RefreshResult>();
        var indexShard = findIndexShard(indexName);
        indexShard.externalRefresh("api", refreshFuture); // use indexShard.externalRefresh because REFRESH thread pool is blocked

        assertBusy(() -> assertThat(getThrottledRefreshes(refreshThrottler), equalTo(1L)));
        assertAcked(client().admin().indices().prepareDelete(indexName));
        assertBusy(() -> assertFalse(indexShard.store().hasReferences()));

        latch.countDown();

        expectThrows(AlreadyClosedException.class, refreshFuture::actionGet);
    }
}
