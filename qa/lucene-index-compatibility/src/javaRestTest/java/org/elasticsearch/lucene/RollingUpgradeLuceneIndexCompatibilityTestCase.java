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

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_READ_ONLY_BLOCK;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_WRITE_BLOCK;
import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.INDEX_CLOSED_BLOCK;
import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING;
import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

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
            createIndex(
                client(),
                index,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
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
            final var maybeClose = randomBoolean();
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

        if (nodesVersions().values().stream().anyMatch(v -> v.onOrAfter(VERSION_CURRENT))) {
            final var isClosed = isIndexClosed(index);
            logger.debug("--> upgraded index [{}] is now in [{}] state", index, isClosed ? "closed" : "open");
            assertThat(
                indexBlocks(index),
                isClosed
                    ? either(contains(INDEX_CLOSED_BLOCK, INDEX_WRITE_BLOCK)).or(contains(INDEX_CLOSED_BLOCK, INDEX_READ_ONLY_BLOCK))
                    : either(contains(INDEX_WRITE_BLOCK)).or(contains(INDEX_READ_ONLY_BLOCK))
            );
            assertIndexSetting(index, VERIFIED_BEFORE_CLOSE_SETTING, is(isClosed));
            assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));

            var block = indexBlocks(index).stream()
                .filter(c -> c.equals(INDEX_WRITE_BLOCK) || c.equals(INDEX_READ_ONLY_BLOCK))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Block not found"));
            if (block.equals(INDEX_READ_ONLY_BLOCK)) {
                logger.debug("--> read_only API block can be replaced by a write block (required for the remaining tests)");
                updateIndexSettings(
                    index,
                    Settings.builder()
                        .putNull(IndexMetadata.APIBlock.READ_ONLY.settingName())
                        .put(IndexMetadata.APIBlock.WRITE.settingName(), true)
                );
            }

            assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));
            assertIndexSetting(index, VERIFIED_BEFORE_CLOSE_SETTING, is(isClosed));
            assertThat(indexBlocks(index), isClosed ? contains(INDEX_CLOSED_BLOCK, INDEX_WRITE_BLOCK) : contains(INDEX_WRITE_BLOCK));

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
     * Similar to {@link #testIndexUpgrade()} but with a read_only block.
     */
    public void testIndexUpgradeReadOnlyBlock() throws Exception {
        final String index = suffix("index-");
        final int numDocs = 2573;

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

            addIndexBlock(index, IndexMetadata.APIBlock.READ_ONLY);
            return;
        }

        if (nodesVersions().values().stream().anyMatch(v -> v.onOrAfter(VERSION_CURRENT))) {
            assertThat(indexBlocks(index), contains(INDEX_READ_ONLY_BLOCK));
            assertIndexSetting(index, VERIFIED_READ_ONLY_SETTING, is(true));

            assertThat(indexVersion(index), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), index, numDocs);
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

            addIndexBlock(index, IndexMetadata.APIBlock.WRITE);

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

                assertThat(indexBlocks(restoredIndex), contains(INDEX_WRITE_BLOCK));
                assertIndexSetting(restoredIndex, VERIFIED_READ_ONLY_SETTING, is(true));

                var ex = expectUpdateIndexSettingsThrows(
                    restoredIndex,
                    Settings.builder().putNull(IndexMetadata.APIBlock.WRITE.settingName())
                );
                assertThat(ex.getMessage(), containsStringCannotRemoveBlockOnReadOnlyIndex(restoredIndex));

                assertThat(indexVersion(restoredIndex), equalTo(VERSION_MINUS_2));
                assertDocCount(client(), restoredIndex, numDocs);

                updateRandomIndexSettings(restoredIndex);
                updateRandomMappings(restoredIndex);

                logger.debug("--> closing restored index [{}]", restoredIndex);
                closeIndex(restoredIndex);
                ensureGreen(restoredIndex);

                logger.debug("--> write API block can be removed on a closed index: INDEX_CLOSED_BLOCK already blocks writes");
                updateIndexSettings(restoredIndex, Settings.builder().putNull(IndexMetadata.APIBlock.WRITE.settingName()));

                logger.debug("--> but attempts to re-opening [{}] should fail due to the missing block", restoredIndex);
                ex = expectThrows(ResponseException.class, () -> openIndex(restoredIndex));
                assertThat(ex.getMessage(), containsString("must be marked as read-only"));

                addIndexBlock(restoredIndex, IndexMetadata.APIBlock.WRITE);

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

        if (isFullyUpgradedTo(VERSION_CURRENT)) {
            var exception = expectThrows(
                ResponseException.class,
                () -> restoreIndex(
                    repository,
                    snapshot,
                    index,
                    suffix("unrestorable"),
                    Settings.builder().put(IndexMetadata.APIBlock.WRITE.settingName(), false).build()
                )
            );
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(500));
            assertThat(exception.getMessage(), containsString("must be marked as read-only using the setting"));
        }
    }
}
