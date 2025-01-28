/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.cluster.util.Version;

import static org.hamcrest.Matchers.equalTo;

public class FullClusterRestartLuceneIndexCompatibilityIT extends FullClusterRestartIndexCompatibilityTestCase {

    static {
        clusterConfig = config -> config.setting("xpack.license.self_generated.type", "trial");
    }

    public FullClusterRestartLuceneIndexCompatibilityIT(Version version) {
        super(version);
    }

    /**
     * Creates an index on N-2, upgrades to N -1 and marks as read-only, then upgrades to N.
     */
    public void testIndexUpgrade() throws Exception {
        final String index = suffix("index");
        final int numDocs = 2431;

        if (isFullyUpgradedTo(VERSION_MINUS_2)) {
            logger.debug("--> creating index [{}]", index);
            createIndex(
                client(),
                index,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomInt(2))
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

            addIndexBlock(index, IndexMetadata.APIBlock.WRITE);
            return;
        }

        if (isFullyUpgradedTo(VERSION_CURRENT)) {
            ensureGreen(index);

            assertThat(indexVersion(index), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), index, numDocs);

            assertThatIndexBlock(index, IndexMetadata.APIBlock.WRITE);

            var numberOfReplicas = getNumberOfReplicas(index);
            if (0 < numberOfReplicas) {
                logger.debug("--> resetting number of replicas [{}] to [0]", numberOfReplicas);
                updateIndexSettings(index, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
            }

            updateRandomIndexSettings(index);
            updateRandomMappings(index);

            logger.debug("--> adding replica to test peer-recovery");
            updateIndexSettings(index, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
            ensureGreen(index);

            logger.debug("--> closing restored index [{}]", index);
            closeIndex(index);
            ensureGreen(index);

            logger.debug("--> adding replica to test peer-recovery for closed shards");
            updateIndexSettings(index, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2));
            ensureGreen(index);

            logger.debug("--> re-opening restored index [{}]", index);
            openIndex(index);
            ensureGreen(index);

            assertDocCount(client(), index, numDocs);

            logger.debug("--> deleting index [{}]", index);
            deleteIndex(index);
        }
    }

    /**
     * Similar to {@link #testIndexUpgrade()} but with a read_only block.
     */
    public void testIndexUpgradeReadOnlyBlock() throws Exception {
        final String index = suffix("index");
        final int numDocs = 2531;

        if (isFullyUpgradedTo(VERSION_MINUS_2)) {
            logger.debug("--> creating index [{}]", index);
            createIndex(
                client(),
                index,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomInt(2))
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

            addIndexBlock(index, IndexMetadata.APIBlock.READ_ONLY);
            return;
        }

        if (isFullyUpgradedTo(VERSION_CURRENT)) {
            ensureGreen(index);

            assertThat(indexVersion(index), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), index, numDocs);

            assertThatIndexBlock(index, IndexMetadata.APIBlock.READ_ONLY);
        }
    }

    /**
     * Creates an index on N-2, marks as read-only on N-1 and creates a snapshot, then restores the snapshot on N.
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

            addIndexBlock(index, IndexMetadata.APIBlock.WRITE);

            logger.debug("--> creating snapshot [{}]", snapshot);
            createSnapshot(client(), repository, snapshot, true);

            logger.debug("--> deleting index [{}]", index);
            deleteIndex(index);
            return;
        }

        if (isFullyUpgradedTo(VERSION_CURRENT)) {
            var restoredIndex = suffix("index-restored");
            logger.debug("--> restoring index [{}] as [{}]", index, restoredIndex);
            restoreIndex(repository, snapshot, index, restoredIndex);
            ensureGreen(restoredIndex);

            assertThatIndexBlock(restoredIndex, IndexMetadata.APIBlock.WRITE);
            assertThat(indexVersion(restoredIndex), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), restoredIndex, numDocs);

            updateRandomIndexSettings(restoredIndex);
            updateRandomMappings(restoredIndex);

            logger.debug("--> adding replica to test peer-recovery");
            updateIndexSettings(restoredIndex, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
            ensureGreen(restoredIndex);

            logger.debug("--> closing restored index [{}]", restoredIndex);
            closeIndex(restoredIndex);
            ensureGreen(restoredIndex);

            logger.debug("--> adding replica to test peer-recovery for closed shards");
            updateIndexSettings(restoredIndex, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2));
            ensureGreen(restoredIndex);

            logger.debug("--> re-opening restored index [{}]", restoredIndex);
            openIndex(restoredIndex);
            ensureGreen(restoredIndex);

            assertDocCount(client(), restoredIndex, numDocs);

            logger.debug("--> deleting restored index [{}]", restoredIndex);
            deleteIndex(restoredIndex);
        }
    }

    /**
     * Creates an index on N-2, marks as read-only on N-1 and creates a snapshot and then closes the index, then restores the snapshot on N.
     */
    public void testRestoreIndexOverClosedIndex() throws Exception {
        final String repository = suffix("repository");
        final String snapshot = suffix("snapshot");
        final String index = suffix("index");
        final int numDocs = 2134;

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

            addIndexBlock(index, IndexMetadata.APIBlock.WRITE);

            logger.debug("--> creating snapshot [{}]", snapshot);
            createSnapshot(client(), repository, snapshot, true);

            logger.debug("--> force-merge index [{}] to 1 segment", index);
            forceMerge(index, 1);

            logger.debug("--> closing index [{}]", index);
            closeIndex(index);
            ensureGreen(index);
            return;
        }

        if (isFullyUpgradedTo(VERSION_CURRENT)) {
            assertThat(isIndexClosed(index), equalTo(true));
            assertThatIndexBlock(index, IndexMetadata.APIBlock.WRITE);

            logger.debug("--> restoring index [{}] over existing closed index", index);
            restoreIndex(repository, snapshot, index, index);
            ensureGreen(index);

            assertThat(indexVersion(index), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), index, numDocs);

            logger.debug("--> deleting index [{}]", index);
            deleteIndex(index);
        }
    }
}
