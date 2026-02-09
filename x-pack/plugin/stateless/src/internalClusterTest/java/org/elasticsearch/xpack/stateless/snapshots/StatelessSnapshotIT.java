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

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.engine.HollowIndexEngine;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_TTL;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotShardContextFactory.STATELESS_SNAPSHOT_ENABLED_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class StatelessSnapshotIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), StatelessMockRepositoryPlugin.class);
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    public void testStatelessSnapshotReadsFromObjectStore() {
        final var indexNodeName = startMasterAndIndexNode(
            Settings.builder().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK).build()
        );

        final String indexName = randomIdentifier();
        createIndex(indexName, 1, 0);
        indexAndMaybeFlush(indexName);

        final var repoName = randomIdentifier();
        createRepository(repoName, "fs");

        final var snapshotReadSeen = new AtomicBoolean(false);
        setNodeRepositoryStrategy(indexNodeName, new StatelessMockRepositoryStrategy() {
            @Override
            public InputStream blobContainerReadBlob(
                CheckedSupplier<InputStream, IOException> originalSupplier,
                OperationPurpose purpose,
                String blobName
            ) throws IOException {
                if (purpose == OperationPurpose.SNAPSHOT_DATA) {
                    snapshotReadSeen.set(true);
                }
                return super.blobContainerReadBlob(originalSupplier, purpose, blobName);
            }

            @Override
            public InputStream blobContainerReadBlob(
                CheckedSupplier<InputStream, IOException> originalSupplier,
                OperationPurpose purpose,
                String blobName,
                long position,
                long length
            ) throws IOException {
                if (purpose == OperationPurpose.SNAPSHOT_DATA) {
                    snapshotReadSeen.set(true);
                }
                return super.blobContainerReadBlob(originalSupplier, purpose, blobName, position, length);
            }
        });

        // 1. Stateless snapshot is disabled by default so that indices reads are from local primary shard
        // The object store should not see any read with SNAPSHOT_DATA operation purpose
        createSnapshot(repoName, "snap-1", List.of(indexName), List.of());
        assertFalse(snapshotReadSeen.get());

        // 2. Enable stateless snapshot and take another snapshot. The object store should see reads with SNAPSHOT_DATA operation purpose
        indexAndMaybeFlush(indexName);
        updateClusterSettings(Settings.builder().put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "read_from_object_store"));
        createSnapshot(repoName, "snap-2", List.of(indexName), List.of());
        assertTrue(snapshotReadSeen.get());

        // 3. Disable stateless snapshot and take yet another snapshot.
        // The object store should no longer see any new read with SNAPSHOT_DATA operation purpose
        snapshotReadSeen.set(false);
        indexAndMaybeFlush(indexName);
        updateClusterSettings(Settings.builder().put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "disabled"));
        createSnapshot(repoName, "snap-3", List.of(indexName), List.of());
        assertFalse(snapshotReadSeen.get());
    }

    public void testStatelessSnapshotBasic() {
        final var settings = Settings.builder().put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "read_from_object_store").build();
        startMasterAndIndexNode(settings);
        startSearchNode(settings);
        ensureStableCluster(2);

        final String indexName = randomIdentifier();
        final int numberOfShards = between(1, 5);
        createIndex(indexName, numberOfShards, 1);
        ensureGreen(indexName);
        final var nDocs = indexAndMaybeFlush(indexName);

        final var repoName = randomIdentifier();
        createRepository(repoName, "fs");
        final var snapshotName = randomIdentifier();
        createSnapshot(repoName, snapshotName, List.of(indexName), List.of());

        safeGet(client().admin().indices().prepareDelete(indexName).execute());
        final var restoreSnapshotResponse = safeGet(
            client().admin()
                .cluster()
                .prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
                .setIndices(indexName)
                .setWaitForCompletion(true)
                .execute()
        );
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(numberOfShards));

        ensureGreen(indexName);
        assertHitCount(prepareSearch(indexName), nDocs);
    }

    public void testSnapshotHollowShard() {
        final Settings settings = Settings.builder()
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "read_from_object_store")
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true)
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0)
            .build();
        final var node0 = startMasterAndIndexNode(settings);
        final var node1 = startMasterAndIndexNode(settings);
        ensureStableCluster(2);

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.exclude._name", node1).build());
        ensureGreen(indexName);
        final int nDocs = indexAndMaybeFlush(indexName);

        // Ensure that the shard becomes hollow
        safeSleep(100); //

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", node0));
        ensureGreen(indexName);

        final var indexShard = findIndexShard(indexName);
        assertThat(indexShard.getEngineOrNull(), instanceOf(HollowIndexEngine.class));

        final var repoName = randomIdentifier();
        createRepository(repoName, "fs");
        final var snapshotInfo = createSnapshot(repoName, "snap-1", List.of(indexName), List.of());
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));

        final String restoredIndexName = "restored-" + indexName;
        client().admin()
            .cluster()
            .prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap-1")
            .setIndices(indexName)
            .setRenamePattern(indexName)
            .setRenameReplacement(restoredIndexName)
            .setWaitForCompletion(true)
            .get();

        final var indicesStatsResponse = safeGet(client().admin().indices().prepareStats(restoredIndexName).setDocs(true).execute());
        final long count = indicesStatsResponse.getIndices().get(restoredIndexName).getTotal().getDocs().getCount();
        assertThat(count, equalTo((long) nDocs));
    }

    private int indexAndMaybeFlush(String indexName) {
        final int nDocs = between(50, 100);
        indexDocs(indexName, nDocs);
        if (randomBoolean()) {
            flush(indexName);
        }
        return nDocs;
    }
}
