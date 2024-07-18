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
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils.getObjectStoreMockRepository;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

// Disabling WindowsFS because it prevents file deletions and ExtrasFS because it adds unnecessary files in Lucene index and tests in this
// class verify the content of Lucene directories
@LuceneTestCase.SuppressFileSystems(value = { "WindowsFS", "ExtrasFS" })
public class StoreStatsIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockRepository.Plugin.class);
        plugins.add(InternalSettingsPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    public void testStoreStats() throws Exception {
        startMasterOnlyNode();
        final String indexNode = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0)
                // make sure nothing triggers flushes under the hood
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.ofGb(1L))
                .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                .build()
        );

        assertBusy(() -> {
            var indexShard = shardStoreStats(indexName, ShardRouting.Role.INDEX_ONLY);
            assertThat("initial commit is uploaded and temporary files are deleted from disk", indexShard.sizeInBytes(), equalTo(0L));
            assertThat("initial commit is uploaded", indexShard.totalDataSetSizeInBytes(), greaterThan(0L));
        });

        var stats = shardStoreStats(indexName, ShardRouting.Role.INDEX_ONLY);
        final var initialDataSetSize = stats.totalDataSetSizeInBytes();

        startSearchNode();
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);

        assertBusy(() -> {
            var searchShard = shardStoreStats(indexName, ShardRouting.Role.SEARCH_ONLY);
            assertThat("search shard has no files on disk", searchShard.sizeInBytes(), equalTo(0L));
            assertThat("search shard fetched initial commit", searchShard.totalDataSetSizeInBytes(), equalTo(initialDataSetSize));
        });

        indexDocs(indexName, 1_000);

        // block index shard uploads to object store
        var mockRepository = getObjectStoreMockRepository(getObjectStoreService(indexNode));
        mockRepository.setBlockOnAnyFiles();

        stats = shardStoreStats(indexName, ShardRouting.Role.INDEX_ONLY);
        assertThat("new commit not yet uploaded, data set size unchanged", stats.totalDataSetSizeInBytes(), equalTo(initialDataSetSize));

        // now unblock uploads and flush to upload files
        mockRepository.unblock();
        flush(indexName);

        assertBusy(() -> {
            var indexShard = shardStoreStats(indexName, ShardRouting.Role.INDEX_ONLY);
            assertThat("new commit is uploaded and temporary files deleted from disk", indexShard.sizeInBytes(), equalTo(0L));
            assertThat(
                "new commit is uploaded, data set size changed",
                indexShard.totalDataSetSizeInBytes(),
                greaterThan(initialDataSetSize)
            );
        });

        stats = shardStoreStats(indexName, ShardRouting.Role.INDEX_ONLY);
        final var newDataSetSize = stats.totalDataSetSizeInBytes();

        assertBusy(() -> {
            var searchShard = shardStoreStats(indexName, ShardRouting.Role.SEARCH_ONLY);
            assertThat("search shard has no files on disk", searchShard.sizeInBytes(), equalTo(0L));
            assertThat("search shard fetched new commit", searchShard.totalDataSetSizeInBytes(), equalTo(newDataSetSize));
        });
    }

    private static StoreStats shardStoreStats(String indexName, ShardRouting.Role role) {
        var stats = client().admin().indices().prepareStats(indexName).clear().setStore(true).get();
        for (ShardStats shardStats : stats.getShards()) {
            if (shardStats.getShardRouting().role() == role) {
                return shardStats.getStats().getStore();
            }
        }
        throw new AssertionError();
    }
}
