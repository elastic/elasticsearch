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

import static org.hamcrest.Matchers.equalTo;

public class FullClusterRestartSearchableSnapshotIndexCompatibilityIT extends FullClusterRestartIndexCompatibilityTestCase {

    static {
        clusterConfig = config -> config.setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
            .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB");
    }

    public FullClusterRestartSearchableSnapshotIndexCompatibilityIT(Version version) {
        super(version);
    }

    /**
     * Creates an index and a snapshot on N-2, then mounts the snapshot on N.
     */
    public void testSearchableSnapshot() throws Exception {
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

            logger.debug("--> creating snapshot [{}]", snapshot);
            createSnapshot(client(), repository, snapshot, true);
            return;
        }

        if (isFullyUpgradedTo(VERSION_MINUS_1)) {
            ensureGreen(index);

            assertThat(indexVersion(index), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), index, numDocs);

            logger.debug("--> deleting index [{}]", index);
            deleteIndex(index);
            return;
        }

        if (isFullyUpgradedTo(VERSION_CURRENT)) {
            var mountedIndex = suffix("index-mounted");
            logger.debug("--> mounting index [{}] as [{}]", index, mountedIndex);
            mountIndex(repository, snapshot, index, randomBoolean(), mountedIndex);
            ensureGreen(mountedIndex);

            assertThat(indexVersion(mountedIndex), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), mountedIndex, numDocs);

            updateRandomIndexSettings(mountedIndex);
            updateRandomMappings(mountedIndex);

            logger.debug("--> adding replica to test peer-recovery");
            updateIndexSettings(mountedIndex, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
            ensureGreen(mountedIndex);

            logger.debug("--> closing index [{}]", mountedIndex);
            closeIndex(mountedIndex);
            ensureGreen(mountedIndex);

            logger.debug("--> adding replica to test peer-recovery for closed shards");
            updateIndexSettings(mountedIndex, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2));
            ensureGreen(mountedIndex);

            logger.debug("--> re-opening index [{}]", mountedIndex);
            openIndex(mountedIndex);
            ensureGreen(mountedIndex);

            assertDocCount(client(), mountedIndex, numDocs);

            logger.debug("--> deleting index [{}]", mountedIndex);
            deleteIndex(mountedIndex);
        }
    }

    /**
     * Creates an index and a snapshot on N-2, mounts the snapshot on N -1 and then upgrades to N.
     */
    public void testSearchableSnapshotUpgrade() throws Exception {
        final String mountedIndex = suffix("index-mounted");
        final String repository = suffix("repository");
        final String snapshot = suffix("snapshot");
        final String index = suffix("index");
        final int numDocs = 4321;

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

            logger.debug("--> creating snapshot [{}]", snapshot);
            createSnapshot(client(), repository, snapshot, true);

            logger.debug("--> deleting index [{}]", index);
            deleteIndex(index);
            return;
        }

        if (isFullyUpgradedTo(VERSION_MINUS_1)) {
            logger.debug("--> mounting index [{}] as [{}]", index, mountedIndex);
            mountIndex(repository, snapshot, index, randomBoolean(), mountedIndex);

            ensureGreen(mountedIndex);

            updateRandomIndexSettings(mountedIndex);
            updateRandomMappings(mountedIndex);

            assertThat(indexVersion(mountedIndex), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), mountedIndex, numDocs);

            if (randomBoolean()) {
                logger.debug("--> adding replica to test upgrade with replica");
                updateIndexSettings(mountedIndex, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
                ensureGreen(mountedIndex);
            }

            if (randomBoolean()) {
                logger.debug("--> random closing of index [{}] before upgrade", mountedIndex);
                closeIndex(mountedIndex);
                ensureGreen(mountedIndex);
            }
            return;
        }

        if (isFullyUpgradedTo(VERSION_CURRENT)) {
            ensureGreen(mountedIndex);

            if (isIndexClosed(mountedIndex)) {
                logger.debug("--> re-opening index [{}] after upgrade", mountedIndex);
                openIndex(mountedIndex);
                ensureGreen(mountedIndex);
            }

            assertThat(indexVersion(mountedIndex), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), mountedIndex, numDocs);

            updateRandomIndexSettings(mountedIndex);
            updateRandomMappings(mountedIndex);

            logger.debug("--> adding replica to test peer-recovery");
            updateIndexSettings(mountedIndex, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2));
            ensureGreen(mountedIndex);
        }
    }
}
