/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.blobstore;

import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryConflictException;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_DISK_READ_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_DISK_WRITE_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_NETWORK_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class BlobStoreDynamicSettingsIT extends AbstractSnapshotIntegTestCase {

    public void testUpdateRateLimitsDynamically() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();

        final boolean largeSnapshotPool = randomBoolean();
        final String dataNode;
        if (largeSnapshotPool) {
            dataNode = startDataNodeWithLargeSnapshotPool();
        } else {
            dataNode = internalCluster().startDataOnlyNode();
        }

        final String repoName = "test-repo";
        // use a small chunk size so the rate limiter does not overshoot to far and get blocked a very long time below
        createRepository(repoName, "mock", randomRepositorySettings().put("chunk_size", "100b"));

        if (randomBoolean()) {
            createFullSnapshot(repoName, "snapshot-1");
        }

        final String indexName = "test-idx";
        createIndexWithContent(indexName);

        final Repository repoOnMaster = getRepositoryOnNode(repoName, masterNode);
        final Repository repoOnDataNode = getRepositoryOnNode(repoName, dataNode);

        final Settings currentSettings = repoOnMaster.getMetadata().settings();
        assertNull(currentSettings.get(BlobStoreRepository.MAX_SNAPSHOT_BYTES_PER_SEC.getKey()));
        assertNull(currentSettings.get(BlobStoreRepository.MAX_RESTORE_BYTES_PER_SEC.getKey()));

        createRepository(
            repoName,
            "mock",
            Settings.builder().put(currentSettings).put(BlobStoreRepository.MAX_SNAPSHOT_BYTES_PER_SEC.getKey(), "1b"),
            randomBoolean()
        );

        assertSame(repoOnMaster, getRepositoryOnNode(repoName, masterNode));
        assertSame(repoOnDataNode, getRepositoryOnNode(repoName, dataNode));

        final Settings updatedSettings = repoOnMaster.getMetadata().settings();
        assertEquals(
            ByteSizeValue.ofBytes(1L),
            updatedSettings.getAsBytesSize(BlobStoreRepository.MAX_SNAPSHOT_BYTES_PER_SEC.getKey(), ByteSizeValue.ZERO)
        );
        assertNull(currentSettings.get(BlobStoreRepository.MAX_RESTORE_BYTES_PER_SEC.getKey()));

        final ActionFuture<CreateSnapshotResponse> snapshot1 = startFullSnapshotBlockedOnDataNode("snapshot-2", repoName, dataNode);

        // we only run concurrent verification when we have a large SNAPSHOT pool on the data node because otherwise the verification would
        // deadlock since the small pool is already blocked by the snapshot on the data node
        createRepository(
            repoName,
            "mock",
            Settings.builder().put(updatedSettings).put(BlobStoreRepository.MAX_SNAPSHOT_BYTES_PER_SEC.getKey(), "1024b"),
            largeSnapshotPool && randomBoolean()
        );
        assertSame(repoOnMaster, getRepositoryOnNode(repoName, masterNode));
        assertSame(repoOnDataNode, getRepositoryOnNode(repoName, dataNode));

        logger.info("--> verify that we can't update [location] dynamically");
        try {
            // this setting update will fail so we can set the verification parameter randomly even if the SNAPSHOT pool is already blocked
            // since we will never actually get to the verification step
            createRepository(
                repoName,
                "mock",
                Settings.builder().put(repoOnMaster.getMetadata().settings()).put("location", randomRepoPath()),
                randomBoolean()
            );
        } catch (Exception e) {
            final Throwable ise = ExceptionsHelper.unwrap(e, RepositoryConflictException.class);
            assertThat(ise, instanceOf(RepositoryConflictException.class));
            assertEquals(
                ise.getMessage(),
                "[test-repo] trying to modify or unregister repository that is currently used (snapshot is in progress)"
            );
        }

        logger.info("--> verify that we can update [{}] dynamically", MockRepository.DUMMY_UPDATABLE_SETTING_NAME);
        final String dummySettingValue = randomUnicodeOfCodepointLength(10);
        // we only run concurrent verification when we have a large SNAPSHOT pool on the data node because otherwise the verification would
        // deadlock since the small pool is already blocked by the snapshot on the data node
        createRepository(
            repoName,
            "mock",
            Settings.builder()
                .put(repoOnMaster.getMetadata().settings())
                .put(MockRepository.DUMMY_UPDATABLE_SETTING_NAME, dummySettingValue),
            largeSnapshotPool && randomBoolean()
        );
        final Repository newRepoOnMaster = getRepositoryOnNode(repoName, masterNode);
        assertSame(repoOnMaster, newRepoOnMaster);
        assertSame(repoOnDataNode, getRepositoryOnNode(repoName, dataNode));
        assertEquals(dummySettingValue, newRepoOnMaster.getMetadata().settings().get(MockRepository.DUMMY_UPDATABLE_SETTING_NAME));

        unblockNode(repoName, dataNode);
        assertSuccessful(snapshot1);
    }

    public void testDefaultRateLimits() throws Exception {
        boolean nodeBandwidthSettingsSet = randomBoolean();
        Settings.Builder nodeSettings = Settings.builder();
        if (randomBoolean()) {
            nodeSettings = nodeSettings.put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "100m");
        }
        if (nodeBandwidthSettingsSet) {
            nodeSettings = nodeSettings.put(NODE_BANDWIDTH_RECOVERY_NETWORK_SETTING.getKey(), "100m")
                .put(NODE_BANDWIDTH_RECOVERY_DISK_READ_SETTING.getKey(), "100m")
                .put(NODE_BANDWIDTH_RECOVERY_DISK_WRITE_SETTING.getKey(), "100m");
        }
        final String node = internalCluster().startMasterOnlyNode(nodeSettings.build());

        String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepositorySettings());
        final BlobStoreRepository repository = getRepositoryOnNode("test-repo", node);

        RateLimiter snapshotRateLimiter = repository.getSnapshotRateLimiter();
        if (nodeBandwidthSettingsSet) {
            assertNull("default snapshot rate limiter should be null", snapshotRateLimiter);
        } else {
            assertThat("default snapshot speed should be 40mb/s", snapshotRateLimiter.getMBPerSec(), equalTo(40.0));
        }
        RateLimiter restoreRateLimiter = repository.getRestoreRateLimiter();
        assertNull("default restore rate limiter should be null", restoreRateLimiter);

        deleteRepository("test-repo");
    }
}
