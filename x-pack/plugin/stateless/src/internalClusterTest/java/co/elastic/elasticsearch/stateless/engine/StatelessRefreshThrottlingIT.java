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

import org.elasticsearch.index.Index;
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
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
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

    public void testNodeRefreshThrottling() throws Exception {
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

    public void testSystemIndexIsNotThrottled() throws Exception {
        String indexNode = startMasterAndIndexNode();
        createSystemIndex(indexSettings(1, 0).build());
        indexDoc(SYSTEM_INDEX_NAME, "1", "field", "value");
        RefreshThrottler shardThrottler = getRefreshThrottler(indexNode, SYSTEM_INDEX_NAME, 0);
        assertThat(shardThrottler, instanceOf(RefreshThrottler.Noop.class));
    }
}
