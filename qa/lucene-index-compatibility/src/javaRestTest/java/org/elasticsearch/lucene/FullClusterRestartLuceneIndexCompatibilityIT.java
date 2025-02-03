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
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.cluster.util.Version;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_READ_ONLY_BLOCK;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_WRITE_BLOCK;
import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.INDEX_CLOSED_BLOCK;
import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING;
import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class FullClusterRestartLuceneIndexCompatibilityIT extends FullClusterRestartIndexCompatibilityTestCase {

    static {
        clusterConfig = config -> config.setting("xpack.license.self_generated.type", "trial");
    }

    public FullClusterRestartLuceneIndexCompatibilityIT(Version version) {
        super(version);
    }

    /**
     * Creates an index on N-2, upgrades to N-1 and marks as read-only, then upgrades to N.
     */
    public void testIndexUpgrade() throws Exception {
        final String index = suffix("index");
        final int numDocs = 2431;

        if (isFullyUpgradedTo(VERSION_MINUS_2)) {
            createIndex(
                client(),
                index,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomInt(2))
                    .build()
            );
            indexDocs(index, numDocs);
            return;
        }

        assertThat(indexVersion(index), equalTo(VERSION_MINUS_2));
        ensureGreen(index);

        if (isIndexClosed(index) == false) {
            assertDocCount(client(), index, numDocs);
        }

        if (isFullyUpgradedTo(VERSION_MINUS_1)) {
            final boolean maybeClose = randomBoolean();
            if (maybeClose) {
                logger.debug("--> closing index [{}] before upgrade", index);
                closeIndex(index);
            }

            final var block = randomFrom(IndexMetadata.APIBlock.WRITE, IndexMetadata.APIBlock.READ_ONLY);
            addIndexBlock(index, block);

            assertThat(indexBlocks(index), maybeClose ? contains(INDEX_CLOSED_BLOCK, block.getBlock()) : contains(block.getBlock()));
            assertIndexSetting(index, VERIFIED_BEFORE_CLOSE_SETTING, is(maybeClose));
            assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));
            return;
        }

        if (isFullyUpgradedTo(VERSION_CURRENT)) {
            final var isClosed = isIndexClosed(index);
            logger.debug("--> upgraded index [{}] is in [{}] state", index, isClosed ? "closed" : "open");
            assertThat(
                indexBlocks(index),
                isClosed
                    ? either(contains(INDEX_CLOSED_BLOCK, INDEX_WRITE_BLOCK)).or(contains(INDEX_CLOSED_BLOCK, INDEX_READ_ONLY_BLOCK))
                    : either(contains(INDEX_WRITE_BLOCK)).or(contains(INDEX_READ_ONLY_BLOCK))
            );
            assertIndexSetting(index, VERIFIED_BEFORE_CLOSE_SETTING, is(isClosed));
            assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));

            if (isClosed == false) {
                logger.debug("--> write/read_only API blocks cannot be removed on an opened index");
                var ex = expectUpdateIndexSettingsThrows(
                    index,
                    Settings.builder()
                        .putNull(IndexMetadata.APIBlock.WRITE.settingName())
                        .putNull(IndexMetadata.APIBlock.READ_ONLY.settingName())
                );
                assertThat(ex.getMessage(), containsStringCannotRemoveBlockOnReadOnlyIndex(index));

            } else if (randomBoolean()) {
                logger.debug("--> write/read_only API blocks can be removed on a closed index: INDEX_CLOSED_BLOCK already blocks writes");
                updateIndexSettings(
                    index,
                    Settings.builder()
                        .putNull(IndexMetadata.APIBlock.WRITE.settingName())
                        .putNull(IndexMetadata.APIBlock.READ_ONLY.settingName())
                );

                assertThat(indexBlocks(index), contains(INDEX_CLOSED_BLOCK));
                assertIndexSetting(index, VERIFIED_BEFORE_CLOSE_SETTING, is(true));
                assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));
            }

            var block = indexBlocks(index).stream().filter(c -> c.equals(INDEX_WRITE_BLOCK) || c.equals(INDEX_READ_ONLY_BLOCK)).findFirst();
            if (block.isPresent() && block.get().equals(INDEX_READ_ONLY_BLOCK)) {
                logger.debug("--> read_only API block can be replaced by a write block (required for the remaining tests)");
                updateIndexSettings(
                    index,
                    Settings.builder()
                        .putNull(IndexMetadata.APIBlock.READ_ONLY.settingName())
                        .put(IndexMetadata.APIBlock.WRITE.settingName(), true)
                );

                assertThat(indexBlocks(index), isClosed ? contains(INDEX_CLOSED_BLOCK, INDEX_WRITE_BLOCK) : contains(INDEX_WRITE_BLOCK));
                assertIndexSetting(index, VERIFIED_BEFORE_CLOSE_SETTING, is(isClosed));
                assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));
            }

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

            if (isClosed) {
                logger.debug("--> re-opening index [{}]", index);
                openIndex(index);
                ensureGreen(index);

                assertDocCount(client(), index, numDocs);
            } else {
                logger.debug("--> closing index [{}]", index);
                closeIndex(index);
                ensureGreen(index);
            }

            logger.debug("--> adding more replicas to test peer-recovery");
            updateIndexSettings(index, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2));
            ensureGreen(index);

            assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));
            assertThat(
                indexBlocks(index),
                isIndexClosed(index) ? contains(INDEX_CLOSED_BLOCK, INDEX_WRITE_BLOCK) : contains(INDEX_WRITE_BLOCK)
            );

            deleteIndex(index);
        }
    }

    /**
     * Creates an index on N-2, closes it on N-1 (without marking it as read-only), then upgrades to N.
     */
    public void testClosedIndexUpgrade() throws Exception {
        final String index = suffix("index");
        final int numDocs = 2437;

        if (isFullyUpgradedTo(VERSION_MINUS_2)) {
            createIndex(
                client(),
                index,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomInt(2))
                    .build()
            );
            indexDocs(index, numDocs);
            return;
        }

        assertThat(indexVersion(index), equalTo(VERSION_MINUS_2));
        ensureGreen(index);

        if (isIndexClosed(index) == false) {
            assertDocCount(client(), index, numDocs);
        }

        if (isFullyUpgradedTo(VERSION_MINUS_1)) {
            logger.debug("--> [{}] closing index before upgrade without adding a read_only/write block", index);
            closeIndex(index);

            assertThat(indexBlocks(index), contains(INDEX_CLOSED_BLOCK));
            assertThat(indexBlocks(index), not(contains(INDEX_WRITE_BLOCK)));
            assertIndexSetting(index, VERIFIED_BEFORE_CLOSE_SETTING, is(true));
            assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(false));
            return;
        }

        if (isFullyUpgradedTo(VERSION_CURRENT)) {
            assertThat(indexBlocks(index), contains(INDEX_CLOSED_BLOCK));
            assertIndexSetting(index, VERIFIED_BEFORE_CLOSE_SETTING, is(true));
            assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(false));

            logger.debug("--> re-opening index [{}] will add a write block", index);
            openIndex(index);
            ensureGreen(index);

            assertThat(indexBlocks(index), contains(INDEX_WRITE_BLOCK));
            assertIndexSetting(index, VERIFIED_BEFORE_CLOSE_SETTING, is(false));
            assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));
            assertDocCount(client(), index, numDocs);

            logger.debug("--> closing index [{}]", index);
            closeIndex(index);
            ensureGreen(index);

            assertThat(indexBlocks(index), contains(INDEX_CLOSED_BLOCK, INDEX_WRITE_BLOCK));
            assertIndexSetting(index, VERIFIED_BEFORE_CLOSE_SETTING, is(true));
            assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));

            deleteIndex(index);
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
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
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

            assertIndexSetting(restoredIndex, VERIFIED_READ_ONLY_SETTING, is(true));
            assertThat(indexBlocks(restoredIndex), contains(INDEX_WRITE_BLOCK));
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
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
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
            assertThat(indexBlocks(index), contains(INDEX_CLOSED_BLOCK, INDEX_WRITE_BLOCK));
            assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));

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
