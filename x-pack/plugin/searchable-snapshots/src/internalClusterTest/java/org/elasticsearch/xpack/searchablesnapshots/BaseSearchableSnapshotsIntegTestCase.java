/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.blobstore.cache.BlobStoreCacheService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.FrozenCacheService;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.license.LicenseService.SELF_GENERATED_LICENSE_TYPE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public abstract class BaseSearchableSnapshotsIntegTestCase extends AbstractSnapshotIntegTestCase {
    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateSearchableSnapshots.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        if (randomBoolean()) {
            builder.put(
                CacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(),
                rarely()
                    ? randomBoolean()
                        ? new ByteSizeValue(randomIntBetween(0, 10), ByteSizeUnit.KB)
                        : new ByteSizeValue(randomIntBetween(0, 1000), ByteSizeUnit.BYTES)
                    : new ByteSizeValue(randomIntBetween(1, 10), ByteSizeUnit.MB)
            );
        }
        if (randomBoolean()) {
            builder.put(
                CacheService.SNAPSHOT_CACHE_RANGE_SIZE_SETTING.getKey(),
                rarely()
                    ? new ByteSizeValue(randomIntBetween(4, 1024), ByteSizeUnit.KB)
                    : new ByteSizeValue(randomIntBetween(1, 10), ByteSizeUnit.MB)
            );
        }
        builder.put(
            FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(),
            rarely()
                ? randomBoolean()
                    ? new ByteSizeValue(randomIntBetween(0, 10), ByteSizeUnit.KB)
                    : new ByteSizeValue(randomIntBetween(0, 1000), ByteSizeUnit.BYTES)
                : new ByteSizeValue(randomIntBetween(1, 10), ByteSizeUnit.MB)
        );
        builder.put(
            FrozenCacheService.SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(),
            rarely()
                ? new ByteSizeValue(randomIntBetween(4, 1024), ByteSizeUnit.KB)
                : new ByteSizeValue(randomIntBetween(1, 10), ByteSizeUnit.MB)
        );
        if (randomBoolean()) {
            builder.put(
                FrozenCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(),
                rarely()
                    ? new ByteSizeValue(randomIntBetween(4, 1024), ByteSizeUnit.KB)
                    : new ByteSizeValue(randomIntBetween(1, 10), ByteSizeUnit.MB)
            );
        }
        if (randomBoolean()) {
            builder.put(
                FrozenCacheService.FROZEN_CACHE_RECOVERY_RANGE_SIZE_SETTING.getKey(),
                new ByteSizeValue(randomIntBetween(4, 1024), ByteSizeUnit.KB)
            );
        }
        return builder.build();
    }

    @After
    public void waitForBlobCacheFillsToComplete() {
        for (BlobStoreCacheService blobStoreCacheService : internalCluster().getDataNodeInstances(BlobStoreCacheService.class)) {
            assertTrue(blobStoreCacheService.waitForInFlightCacheFillsToComplete(30L, TimeUnit.SECONDS));
        }
    }

    protected String mountSnapshot(String repositoryName, String snapshotName, String indexName, Settings restoredIndexSettings)
        throws Exception {
        final String restoredIndexName = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        mountSnapshot(repositoryName, snapshotName, indexName, restoredIndexName, restoredIndexSettings);
        return restoredIndexName;
    }

    protected void mountSnapshot(
        String repositoryName,
        String snapshotName,
        String indexName,
        String restoredIndexName,
        Settings restoredIndexSettings
    ) throws Exception {
        mountSnapshot(repositoryName, snapshotName, indexName, restoredIndexName, restoredIndexSettings, Storage.FULL_COPY);
    }

    protected void mountSnapshot(
        String repositoryName,
        String snapshotName,
        String indexName,
        String restoredIndexName,
        Settings restoredIndexSettings,
        final Storage storage
    ) throws Exception {
        final MountSearchableSnapshotRequest mountRequest = new MountSearchableSnapshotRequest(
            restoredIndexName,
            repositoryName,
            snapshotName,
            indexName,
            Settings.builder()
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), Boolean.FALSE.toString())
                .put(restoredIndexSettings)
                .build(),
            Strings.EMPTY_ARRAY,
            true,
            storage
        );

        final RestoreSnapshotResponse restoreResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, mountRequest).actionGet();
        assertThat(restoreResponse.getRestoreInfo().successfulShards(), equalTo(getNumShards(restoredIndexName).numPrimaries));
        assertThat(restoreResponse.getRestoreInfo().failedShards(), equalTo(0));
    }

    protected void createAndPopulateIndex(String indexName, Settings.Builder settings) throws InterruptedException {
        assertAcked(prepareCreate(indexName, settings));
        ensureGreen(indexName);
        populateIndex(indexName, 100);
    }

    protected void populateIndex(String indexName, int maxIndexRequests) throws InterruptedException {
        final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        // This index does not permit dynamic fields, so we can only use defined field names
        final String key = indexName.equals(SearchableSnapshotsConstants.SNAPSHOT_BLOB_CACHE_INDEX) ? "type" : "foo";
        for (int i = between(10, maxIndexRequests); i >= 0; i--) {
            indexRequestBuilders.add(client().prepareIndex(indexName).setSource(key, randomBoolean() ? "bar" : "baz"));
        }
        indexRandom(true, true, indexRequestBuilders);
        refresh(indexName);
        if (randomBoolean()) {
            assertThat(
                client().admin().indices().prepareForceMerge(indexName).setOnlyExpungeDeletes(true).setFlush(true).get().getFailedShards(),
                equalTo(0)
            );
        }
    }
}
