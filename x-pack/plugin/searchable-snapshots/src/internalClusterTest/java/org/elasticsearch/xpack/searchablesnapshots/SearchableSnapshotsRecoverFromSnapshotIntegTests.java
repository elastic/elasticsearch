/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.recovery.plan.ShardSnapshotsService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.util.Collections;
import java.util.Locale;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class SearchableSnapshotsRecoverFromSnapshotIntegTests extends BaseSearchableSnapshotsIntegTestCase {
    public void testSearchableSnapshotRelocationDoNotUseSnapshotBasedRecoveries() throws Exception {
        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final Settings.Builder repositorySettings = randomRepositorySettings();
        repositorySettings.put(BlobStoreRepository.USE_FOR_PEER_RECOVERY_SETTING.getKey(), true);
        createRepository(repositoryName, FsRepository.TYPE, repositorySettings);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createAndPopulateIndex(
            indexName,
            Settings.builder()
                .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
        );

        final TotalHits totalHits = internalCluster().client()
            .prepareSearch(indexName)
            .setTrackTotalHits(true)
            .get()
            .getHits()
            .getTotalHits();

        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createSnapshot(repositoryName, snapshotName, Collections.singletonList(indexName));
        assertAcked(client().admin().indices().prepareDelete(indexName));

        final String restoredIndexName = "restored-" + indexName;
        mountSnapshot(
            repositoryName,
            snapshotName,
            indexName,
            restoredIndexName,
            Settings.EMPTY,
            MountSearchableSnapshotRequest.Storage.FULL_COPY
        );

        createSnapshot(repositoryName, randomAlphaOfLength(10).toLowerCase(Locale.ROOT), Collections.singletonList(restoredIndexName));

        final String newNode = internalCluster().startDataOnlyNode();

        final MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
            new MockLogAppender.UnseenEventExpectation(
                "Error fetching segments file",
                ShardSnapshotsService.class.getCanonicalName(),
                Level.WARN,
                "Unable to fetch shard snapshot files for*"
            )
        );

        final Logger logger = LogManager.getLogger(ShardSnapshotsService.class);
        Loggers.addAppender(logger, mockAppender);

        // Relocate the searchable snapshot shard to the new node
        client().admin()
            .indices()
            .prepareUpdateSettings(restoredIndexName)
            .setSettings(Settings.builder().put("index.routing.allocation.require._name", newNode).build())
            .get();

        ensureGreen(restoredIndexName);

        assertHitCount(client().prepareSearch(restoredIndexName).setTrackTotalHits(true).get(), totalHits.value);

        mockAppender.assertAllExpectationsMatched();
        Loggers.removeAppender(logger, mockAppender);
        mockAppender.stop();
    }
}
