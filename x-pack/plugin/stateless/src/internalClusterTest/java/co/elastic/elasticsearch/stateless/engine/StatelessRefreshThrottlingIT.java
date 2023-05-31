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

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class StatelessRefreshThrottlingIT extends AbstractStatelessIntegTestCase {

    private RefreshNodeCreditManager getRegularIndicesCreditManager(String indexingNode) {
        var refreshThrottlingService = internalCluster().getInstance(RefreshThrottlingService.class, indexingNode);
        return refreshThrottlingService.getRegularIndicesCreditManager();
    }

    private RefreshBurstableThrottler getRefreshThrottler(String indexingNode, String indexName, int shard) {
        var indicesService = internalCluster().getInstance(IndicesService.class, indexingNode);
        var shardId = new ShardId(resolveIndex(indexName), shard);
        var indexService = indicesService.indexServiceSafe(shardId.getIndex());
        var indexShard = indexService.getShard(shardId.id());
        assertThat(indexShard.getEngineOrNull(), instanceOf(IndexEngine.class));
        var indexEngine = ((IndexEngine) indexShard.getEngineOrNull());
        assertTrue(indexEngine.getRefreshThrottler() instanceof RefreshBurstableThrottler);
        return (RefreshBurstableThrottler) indexEngine.getRefreshThrottler();
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
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        RefreshBurstableThrottler refreshThrottler = getRefreshThrottler(indexNode, indexName, 0);
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

    public void testNodeRefreshThrottling() throws Exception {
        String indexNode = startMasterAndIndexNode();

        int indices = randomIntBetween(1, 3);
        List<String> indicesNames = new ArrayList<>(indices);
        List<RefreshBurstableThrottler> shardThrottlers = new ArrayList<>(indices);
        for (int i = 0; i < indices; i++) {
            final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            createIndex(
                indexName,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                    .build()
            );
            ensureGreen(indexName);
            indicesNames.add(indexName);
            shardThrottlers.add(getRefreshThrottler(indexNode, indexName, 0));
            indexDocs(indicesNames.get(i), randomIntBetween(1, 5));
            refresh(indicesNames.get(i));
        }

        RefreshNodeCreditManager nodeCreditManager = getRegularIndicesCreditManager(indexNode);
        // Trigger throttler's credit to number of indices - 1
        nodeCreditManager.setCredit(indices - 1);

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

}
