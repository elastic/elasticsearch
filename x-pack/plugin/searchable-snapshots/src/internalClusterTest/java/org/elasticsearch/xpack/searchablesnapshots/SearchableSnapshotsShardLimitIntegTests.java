/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.util.Locale;

import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(maxNumDataNodes = 1)
public class SearchableSnapshotsShardLimitIntegTests extends BaseFrozenSearchableSnapshotsIntegTestCase {

    private static final int MAX_NORMAL = 3;
    private static final int MAX_FROZEN = 20;

    private final String fsRepoName = randomAlphaOfLength(10);
    private final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
    private final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), MAX_NORMAL)
            .put(ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE_FROZEN.getKey(), MAX_FROZEN)
            .build();
    }

    @Override
    protected int numberOfShards() {
        return 1;
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    public void testFrozenAndNormalIndependent() throws Exception {
        createRepository(fsRepoName, "fs");

        assertAcked(prepareCreate(indexName, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true)));

        createFullSnapshot(fsRepoName, snapshotName);

        final Settings.Builder indexSettingsBuilder = Settings.builder();
        final int initialCopies = between(1, MAX_FROZEN - 1);
        indexSettingsBuilder.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, initialCopies - 1);
        mount(indexSettingsBuilder, MountSearchableSnapshotRequest.Storage.SHARED_CACHE);

        // one above limit.
        indexSettingsBuilder.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, (MAX_FROZEN + 1) - initialCopies - 1);
        expectLimitThrows(() -> mount(indexSettingsBuilder, MountSearchableSnapshotRequest.Storage.SHARED_CACHE));

        // mount just at limit.
        indexSettingsBuilder.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, MAX_FROZEN - initialCopies - 1);
        mount(indexSettingsBuilder, MountSearchableSnapshotRequest.Storage.SHARED_CACHE);

        // cannot mount one more shard.
        expectLimitThrows(() -> mount(Settings.EMPTY, MountSearchableSnapshotRequest.Storage.SHARED_CACHE));

        // can still do full copy
        mount(Settings.EMPTY, MountSearchableSnapshotRequest.Storage.FULL_COPY);

        // and normal index
        createIndex();

        // but now we have 3 normal shards, so must fail
        expectLimitThrows(() -> mount(Settings.EMPTY, MountSearchableSnapshotRequest.Storage.FULL_COPY));
        expectLimitThrows(this::createIndex);
    }

    private void expectLimitThrows(ThrowingRunnable runnable) {
        expectThrows(IllegalArgumentException.class, runnable);
    }

    public void createIndex() {
        createIndex(randomAlphaOfLength(10).toLowerCase(Locale.ROOT));
    }

    private void mount(Settings.Builder settings, MountSearchableSnapshotRequest.Storage storage) throws Exception {
        mount(settings.build(), storage);
    }

    private void mount(Settings settings, MountSearchableSnapshotRequest.Storage storage) throws Exception {
        mountSnapshot(fsRepoName, snapshotName, indexName, randomAlphaOfLength(11).toLowerCase(Locale.ROOT), settings, storage);
    }
}
