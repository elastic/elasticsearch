/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.cluster.util.Version;

import java.util.List;

import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class RollingUpgradeLuceneIndexCompatibilityTestCase extends RollingUpgradeIndexCompatibilityTestCase {

    static {
        clusterConfig = config -> config.setting("xpack.license.self_generated.type", "trial");
    }

    public RollingUpgradeLuceneIndexCompatibilityTestCase(List<Version> nodesVersions) {
        super(nodesVersions);
    }

    /**
     * Creates an index on N-2, upgrades to N -1 and marks as read-only, then remains searchable during rolling upgrades.
     */
    public void testIndexUpgrade() throws Exception {
        final String index = suffix("index-rolling-upgraded");
        final int numDocs = 2543;

        if (isFullyUpgradedTo(VERSION_MINUS_2)) {
            logger.debug("--> creating index [{}]", index);
            createIndex(
                client(),
                index,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                    .build()
            );

            logger.debug("--> indexing [{}] docs in [{}]", numDocs, index);
            indexDocs(index, numDocs);
            return;
        }

        ensureGreen(index);

        if (isFullyUpgradedTo(VERSION_MINUS_1)) {
            assertThat(indexVersion(index), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), index, numDocs);

            logger.debug("--> flushing [{}]", index);
            flush(index, true);

            logger.debug("--> applying write block on [{}]", index);
            addIndexWriteBlock(index);

            logger.debug("--> applying verified read-only setting on [{}]", index);
            updateIndexSettings(index, Settings.builder().put(VERIFIED_READ_ONLY_SETTING.getKey(), true));
            return;
        }

        if (nodesVersions().values().stream().anyMatch(v -> v.onOrAfter(VERSION_CURRENT))) {
            var indexSettings = getIndexSettingsAsMap(index);
            assertThat(indexSettings.get(IndexMetadata.APIBlock.WRITE.settingName()), equalTo(Boolean.TRUE.toString()));
            assertThat(indexSettings.get(VERIFIED_READ_ONLY_SETTING.getKey()), equalTo(Boolean.TRUE.toString()));

            if (isIndexClosed(index)) {
                logger.debug("--> re-opening index [{}] after upgrade", index);
                openIndex(index);
                ensureGreen(index);
            }

            assertThat(indexVersion(index), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), index, numDocs);

            updateRandomIndexSettings(index);
            updateRandomMappings(index);

            if (randomBoolean()) {
                logger.debug("--> random closing of index [{}] before upgrade", index);
                closeIndex(index);
                ensureGreen(index);
            }
        }
    }

    /**
     * Creates an index on N-2, marks as read-only on N-1 and creates a snapshot, then restores the snapshot during rolling upgrades to N.
     */
    public void testRestoreIndex() throws Exception {
        final String repository = suffix("repository");
        final String snapshot = suffix("snapshot");
        final String index = suffix("index");
        final int numDocs = 1234;

        if (isFullyUpgradedTo(VERSION_MINUS_2)) {
            logger.debug("--> registering repository [{}]", repository);
            registerRepository(client(), repository, FsRepository.TYPE, true, repositorySettings());

            logger.debug("--> creating index [{}]", index);
            createIndex(
                client(),
                index,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                    .build()
            );

            logger.debug("--> indexing [{}] docs in [{}]", numDocs, index);
            indexDocs(index, numDocs);
            return;
        }

        if (isFullyUpgradedTo(VERSION_MINUS_1)) {
            ensureGreen(index);

            assertThat(indexVersion(index), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), index, numDocs);

            logger.debug("--> flushing [{}]", index);
            flush(index, true);

            logger.debug("--> applying write block on [{}]", index);
            addIndexWriteBlock(index);

            logger.debug("--> applying verified read-only setting on [{}]", index);
            updateIndexSettings(index, Settings.builder().put(VERIFIED_READ_ONLY_SETTING.getKey(), true));

            logger.debug("--> creating snapshot [{}]", snapshot);
            createSnapshot(client(), repository, snapshot, true);

            logger.debug("--> deleting index [{}]", index);
            deleteIndex(index);
            return;
        }
        if (nodesVersions().values().stream().anyMatch(v -> v.onOrAfter(VERSION_CURRENT))) {
            var restoredIndex = suffix("index-restored-rolling");
            boolean success = false;
            try {

                logger.debug("--> restoring index [{}] as [{}]", index, restoredIndex);
                restoreIndex(repository, snapshot, index, restoredIndex);
                ensureGreen(restoredIndex);

                assertThat(indexVersion(restoredIndex), equalTo(VERSION_MINUS_2));
                assertDocCount(client(), restoredIndex, numDocs);

                updateRandomIndexSettings(restoredIndex);
                updateRandomMappings(restoredIndex);

                logger.debug("--> closing restored index [{}]", restoredIndex);
                closeIndex(restoredIndex);
                ensureGreen(restoredIndex);

                logger.debug("--> re-opening restored index [{}]", restoredIndex);
                openIndex(restoredIndex);
                ensureGreen(restoredIndex);

                assertDocCount(client(), restoredIndex, numDocs);

                logger.debug("--> deleting restored index [{}]", restoredIndex);
                deleteIndex(restoredIndex);

                success = true;
            } finally {
                if (success == false) {
                    try {
                        client().performRequest(new Request("DELETE", "/" + restoredIndex));
                    } catch (ResponseException e) {
                        logger.warn("Failed to delete restored index [" + restoredIndex + ']', e);
                    }
                }
            }
        }
    }
}
