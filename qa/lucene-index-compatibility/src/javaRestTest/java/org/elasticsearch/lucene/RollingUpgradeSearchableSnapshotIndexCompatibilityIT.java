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
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.cluster.util.Version;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class RollingUpgradeSearchableSnapshotIndexCompatibilityIT extends RollingUpgradeIndexCompatibilityTestCase {

    static {
        clusterConfig = config -> config.setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
            .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB");
    }

    public RollingUpgradeSearchableSnapshotIndexCompatibilityIT(List<Version> nodesVersion) {
        super(nodesVersion);
    }

    /**
     * Creates an index and a snapshot on N-2, then mounts the snapshot during rolling upgrades.
     */
    public void testMountSearchableSnapshot() throws Exception {
        final String repository = suffix("repository");
        final String snapshot = suffix("snapshot");
        final String index = suffix("index-rolling-upgrade");
        final var mountedIndex = suffix("index-rolling-upgrade-mounted");
        final int numDocs = 3145;

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

        boolean success = false;
        try {
            logger.debug("--> mounting index [{}] as [{}]", index, mountedIndex);
            mountIndex(repository, snapshot, index, randomBoolean(), mountedIndex);
            ensureGreen(mountedIndex);

            updateRandomIndexSettings(mountedIndex);
            updateRandomMappings(mountedIndex);

            assertThat(indexVersion(mountedIndex), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), mountedIndex, numDocs);

            logger.debug("--> closing mounted index [{}]", mountedIndex);
            closeIndex(mountedIndex);
            ensureGreen(mountedIndex);

            logger.debug("--> re-opening index [{}]", mountedIndex);
            openIndex(mountedIndex);
            ensureGreen(mountedIndex);

            logger.debug("--> deleting mounted index [{}]", mountedIndex);
            deleteIndex(mountedIndex);

            success = true;
        } finally {
            if (success == false) {
                try {
                    client().performRequest(new Request("DELETE", "/" + mountedIndex));
                } catch (ResponseException e) {
                    logger.warn("Failed to delete mounted index [" + mountedIndex + ']', e);
                }
            }
        }
    }

    /**
     * Creates an index and a snapshot on N-2, mounts the snapshot and ensures it remains searchable during rolling upgrades.
     */
    public void testSearchableSnapshotUpgrade() throws Exception {
        final String mountedIndex = suffix("index-rolling-upgraded-mounted");
        final String repository = suffix("repository");
        final String snapshot = suffix("snapshot");
        final String index = suffix("index-rolling-upgraded");
        final int numDocs = 2143;

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

            logger.debug("--> mounting index [{}] as [{}]", index, mountedIndex);
            mountIndex(repository, snapshot, index, randomBoolean(), mountedIndex);
        }

        ensureGreen(mountedIndex);

        if (isIndexClosed(mountedIndex)) {
            logger.debug("--> re-opening index [{}] after upgrade", mountedIndex);
            openIndex(mountedIndex);
            ensureGreen(mountedIndex);
        }

        updateRandomIndexSettings(mountedIndex);
        updateRandomMappings(mountedIndex);

        assertThat(indexVersion(mountedIndex), equalTo(VERSION_MINUS_2));
        assertDocCount(client(), mountedIndex, numDocs);

        if (randomBoolean()) {
            logger.debug("--> random closing of index [{}] before upgrade", mountedIndex);
            closeIndex(mountedIndex);
            ensureGreen(mountedIndex);
        }
    }
}
