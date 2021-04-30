/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.instanceOf;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class BlobStoreDynamicSettingsIT extends AbstractSnapshotIntegTestCase {

    public void testUpdateRateLimitsDynamically() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        // use a small chunk size so the rate limiter does not overshoot to far and get blocked a very long time below
        createRepository(repoName, "mock", randomRepositorySettings().put("chunk_size", "100b"));
        final String indexName = "test-idx";
        createIndexWithContent(indexName);

        final Repository repoOnMaster = getRepositoryOnNode(repoName, masterNode);
        final Repository repoOnDataNode = getRepositoryOnNode(repoName, dataNode);

        final Settings currentSettings = repoOnMaster.getMetadata().settings();
        assertNull(currentSettings.get(BlobStoreRepository.MAX_SNAPSHOT_BYTES_PER_SEC.getKey()));
        assertNull(currentSettings.get(BlobStoreRepository.MAX_RESTORE_BYTES_PER_SEC.getKey()));

        // allow one chunk per second so we get slow snapshots but without the rate limiter completely locking up
        createRepository(repoName, "mock", Settings.builder().put(currentSettings)
                .put(BlobStoreRepository.MAX_SNAPSHOT_BYTES_PER_SEC.getKey(), "1b"));

        assertSame(repoOnMaster, getRepositoryOnNode(repoName, masterNode));
        assertSame(repoOnDataNode, getRepositoryOnNode(repoName, dataNode));

        final Settings updatedSettings = repoOnMaster.getMetadata().settings();
        assertEquals(ByteSizeValue.ofBytes(1L),
                updatedSettings.getAsBytesSize(BlobStoreRepository.MAX_SNAPSHOT_BYTES_PER_SEC.getKey(), ByteSizeValue.ZERO));
        assertNull(currentSettings.get(BlobStoreRepository.MAX_RESTORE_BYTES_PER_SEC.getKey()));

        final ActionFuture<CreateSnapshotResponse> snapshot1 = startFullSnapshotBlockedOnDataNode("snapshot-1", repoName, dataNode);

        createRepositoryNoVerify(repoName, "mock", Settings.builder().put(updatedSettings)
                .put(BlobStoreRepository.MAX_SNAPSHOT_BYTES_PER_SEC.getKey(), "1024b"));
        assertSame(repoOnMaster, getRepositoryOnNode(repoName, masterNode));
        assertSame(repoOnDataNode, getRepositoryOnNode(repoName, dataNode));

        logger.info("--> verify that we can't update [location] dynamically");
        try {
            createRepositoryNoVerify(repoName, "mock", Settings.builder().put(repoOnMaster.getMetadata().settings())
                    .put("location", randomRepoPath()));
        } catch (Exception e) {
            final Throwable ise = ExceptionsHelper.unwrap(e, IllegalStateException.class);
            assertThat(ise, instanceOf(IllegalStateException.class));
            assertEquals(ise.getMessage(), "trying to modify or unregister repository that is currently used");
        }

        logger.info("--> verify that we can update [{}] dynamically", MockRepository.DUMMY_UPDATABLE_SETTING_NAME);
        final String dummySettingValue = randomUnicodeOfCodepointLength(10);
        createRepositoryNoVerify(repoName, "mock", Settings.builder().put(repoOnMaster.getMetadata().settings())
                .put(MockRepository.DUMMY_UPDATABLE_SETTING_NAME, dummySettingValue));
        final Repository newRepoOnMaster = getRepositoryOnNode(repoName, masterNode);
        assertSame(repoOnMaster, newRepoOnMaster);
        assertSame(repoOnDataNode, getRepositoryOnNode(repoName, dataNode));
        assertEquals(dummySettingValue, newRepoOnMaster.getMetadata().settings().get(MockRepository.DUMMY_UPDATABLE_SETTING_NAME));

        unblockNode(repoName, dataNode);
        assertSuccessful(snapshot1);
    }
}
