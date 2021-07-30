/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.hamcrest.Matchers;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class BlobStoreSizeLimitIT extends AbstractSnapshotIntegTestCase {

    public void testBlobStoreSizeIsLimited() throws Exception {
        final String repoName = "test-repo";
        final int maxSnapshots = randomIntBetween(1, 10);
        createRepository(
            repoName,
            FsRepository.TYPE,
            Settings.builder().put(BlobStoreRepository.MAX_SNAPSHOTS_SETTING.getKey(), maxSnapshots).put("location", randomRepoPath())
        );
        final List<String> snapshotNames = createNSnapshots(repoName, maxSnapshots);
        final ActionFuture<CreateSnapshotResponse> failingSnapshotFuture = startFullSnapshot(repoName, "failing-snapshot");
        final RepositoryException repositoryException = expectThrows(RepositoryException.class, failingSnapshotFuture::actionGet);
        assertThat(
            repositoryException.getMessage(),
            Matchers.endsWith(
                "Cannot add another snapshot to this repository as it already contains ["
                    + maxSnapshots
                    + "] snapshots and is configured to hold up to ["
                    + maxSnapshots
                    + "] snapshots only."
            )
        );
        assertEquals(repositoryException.repository(), repoName);
        assertAcked(startDeleteSnapshot(repoName, randomFrom(snapshotNames)).get());
        createFullSnapshot(repoName, "last-snapshot");
    }
}
