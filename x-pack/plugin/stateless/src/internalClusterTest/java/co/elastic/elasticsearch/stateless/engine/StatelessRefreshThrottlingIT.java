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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptorUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class StatelessRefreshThrottlingIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), List.of(SystemIndexTestPlugin.class).stream()).toList();
    }

    public static class SystemIndexTestPlugin extends Plugin implements SystemIndexPlugin {

        public static final String SYSTEM_INDEX_NAME = ".test-system-idx";

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return Collections.singletonList(
                SystemIndexDescriptorUtils.createUnmanaged(SYSTEM_INDEX_NAME + "*", "System indices for tests")
            );
        }

        @Override
        public String getFeatureName() {
            return SystemIndexTestPlugin.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "A simple test plugin";
        }
    }

    private RefreshNodeCreditManager getRegularIndicesCreditManager(String indexingNode) {
        var refreshThrottlingService = internalCluster().getInstance(RefreshThrottlingService.class, indexingNode);
        return refreshThrottlingService.getRegularIndicesCreditManager();
    }

    private RefreshThrottler getRefreshThrottler(String indexingNode, String indexName, int shard) {
        var indicesService = internalCluster().getInstance(IndicesService.class, indexingNode);
        var indexServiceIterator = indicesService.iterator();
        while (indexServiceIterator.hasNext()) {
            var indexService = indexServiceIterator.next();
            if (indexService.index().getName().equals(indexName) && indexService.hasShard(shard)) {
                var indexShard = indexService.getShard(shard);
                assertThat(indexShard.getEngineOrNull(), instanceOf(IndexEngine.class));
                var indexEngine = ((IndexEngine) indexShard.getEngineOrNull());
                return indexEngine.getRefreshThrottler();
            }
        }
        assert false : "did not find index service for " + indexName + " and shard " + shard + " on node " + indexingNode;
        return null;
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
        startSearchNode(); // only needed because system index created by the test util needs one replica shard

        indexDoc(SystemIndexTestPlugin.SYSTEM_INDEX_NAME, "1", "field", "value");
        RefreshThrottler shardThrottler = getRefreshThrottler(indexNode, SystemIndexTestPlugin.SYSTEM_INDEX_NAME, 0);
        assertThat(shardThrottler, instanceOf(RefreshThrottler.Noop.class));
    }

    public void testFastRefreshIndexIsNotThrottled() throws Exception {
        String indexNode = startMasterAndIndexNode();

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), true).build());

        RefreshThrottler shardThrottler = getRefreshThrottler(indexNode, indexName, 0);
        assertThat(shardThrottler, instanceOf(RefreshThrottler.Noop.class));
    }
}
