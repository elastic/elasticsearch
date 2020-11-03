/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        createRepository(repoName, FsRepository.TYPE, Settings.builder()
                .put(BlobStoreRepository.MAX_SNAPSHOTS_SETTING.getKey(), maxSnapshots).put("location", randomRepoPath()));
        final List<String> snapshotNames = createNSnapshots(repoName, maxSnapshots);
        final ActionFuture<CreateSnapshotResponse> failingSnapshotFuture = startFullSnapshot(repoName, "failing-snapshot");
        final RepositoryException repositoryException = expectThrows(RepositoryException.class, failingSnapshotFuture::actionGet);
        assertThat(repositoryException.getMessage(), Matchers.endsWith(
                "Cannot add another snapshot to this repository as it already contains [" + maxSnapshots +
                        "] snapshots and is configured to hold up to [" + maxSnapshots + "] snapshots only."));
        assertEquals(repositoryException.repository(), repoName);
        assertAcked(startDeleteSnapshot(repoName, randomFrom(snapshotNames)).get());
        createFullSnapshot(repoName, "last-snapshot");
    }
}
