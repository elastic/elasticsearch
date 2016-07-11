/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.bwcompat;

import org.elasticsearch.common.io.FileTestUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ESIntegTestCase;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Tests that a repository can handle both snapshots of previous version formats and new version formats,
 * as blob names and repository blob formats have changed between the snapshot versions.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class RepositoryUpgradabilityIT extends AbstractSnapshotIntegTestCase {

    /**
     * This tests that a repository can inter-operate with snapshots that both have and don't have a UUID,
     * namely when a repository was created in an older version with snapshots created in the old format
     * (only snapshot name, no UUID) and then the repository is loaded into newer versions where subsequent
     * snapshots have a name and a UUID.
     */
    public void testRepositoryWorksWithCrossVersions() throws Exception {
        final List<String> repoVersions = listRepoVersions();
        // run the test for each supported version
        for (final String version : repoVersions) {
            final String repoName = "test-repo-" + version;
            logger.info("-->  creating repository [{}] for version [{}]", repoName, version);
            createRepository(version, repoName);

            logger.info("--> get the snapshots");
            final String originalIndex = "index-" + version;
            final Set<String> indices = Sets.newHashSet(originalIndex);
            final Set<SnapshotInfo> snapshotInfos = Sets.newHashSet(getSnapshots(repoName));
            assertThat(snapshotInfos.size(), equalTo(1));
            SnapshotInfo originalSnapshot = snapshotInfos.iterator().next();
            assertThat(originalSnapshot.snapshotId(), equalTo(new SnapshotId("test_1", SnapshotId.UNASSIGNED_UUID)));
            assertThat(Sets.newHashSet(originalSnapshot.indices()), equalTo(indices));

            logger.info("--> restore the original snapshot");
            final Set<String> restoredIndices = Sets.newHashSet(
                restoreSnapshot(repoName, originalSnapshot.snapshotId().getName())
            );
            assertThat(restoredIndices, equalTo(indices));
            // make sure it has documents
            for (final String searchIdx : restoredIndices) {
                assertThat(client().prepareSearch(searchIdx).setSize(0).get().getHits().totalHits(), greaterThan(0L));
            }
            deleteIndices(restoredIndices); // delete so we can restore again later

            final String snapshotName2 = "test_2";
            logger.info("--> take a new snapshot of the old index");
            final int addedDocSize = 10;
            for (int i = 0; i < addedDocSize; i++) {
                index(originalIndex, "doc", Integer.toString(i), "foo", "new-bar-" + i);
            }
            refresh();
            snapshotInfos.add(createSnapshot(repoName, snapshotName2));

            logger.info("--> get the snapshots with the newly created snapshot [{}]", snapshotName2);
            Set<SnapshotInfo> snapshotInfosFromRepo = Sets.newHashSet(getSnapshots(repoName));
            assertThat(snapshotInfosFromRepo, equalTo(snapshotInfos));
            snapshotInfosFromRepo.forEach(snapshotInfo -> {
                assertThat(Sets.newHashSet(snapshotInfo.indices()), equalTo(indices));
            });

            final String snapshotName3 = "test_3";
            final String indexName2 = "index2";
            logger.info("--> take a new snapshot with a new index");
            createIndex(indexName2);
            indices.add(indexName2);
            for (int i = 0; i < addedDocSize; i++) {
                index(indexName2, "doc", Integer.toString(i), "foo", "new-bar-" + i);
            }
            refresh();
            snapshotInfos.add(createSnapshot(repoName, snapshotName3));

            logger.info("--> get the snapshots with the newly created snapshot [{}]", snapshotName3);
            snapshotInfosFromRepo = Sets.newHashSet(getSnapshots(repoName));
            assertThat(snapshotInfosFromRepo, equalTo(snapshotInfos));
            snapshotInfosFromRepo.forEach(snapshotInfo -> {
                if (snapshotInfo.snapshotId().getName().equals(snapshotName3)) {
                    // only the last snapshot has all the indices
                    assertThat(Sets.newHashSet(snapshotInfo.indices()), equalTo(indices));
                } else {
                    assertThat(Sets.newHashSet(snapshotInfo.indices()), equalTo(Sets.newHashSet(originalIndex)));
                }
            });
            deleteIndices(indices); // clean up indices

            logger.info("--> restore the old snapshot again");
            Set<String> oldRestoredIndices = Sets.newHashSet(restoreSnapshot(repoName, originalSnapshot.snapshotId().getName()));
            assertThat(oldRestoredIndices, equalTo(Sets.newHashSet(originalIndex)));
            for (final String searchIdx : oldRestoredIndices) {
                assertThat(client().prepareSearch(searchIdx).setSize(0).get().getHits().totalHits(),
                           greaterThanOrEqualTo((long)addedDocSize));
            }
            deleteIndices(oldRestoredIndices);

            logger.info("--> restore the new snapshot");
            Set<String> newSnapshotIndices = Sets.newHashSet(restoreSnapshot(repoName, snapshotName3));
            assertThat(newSnapshotIndices, equalTo(Sets.newHashSet(originalIndex, indexName2)));
            for (final String searchIdx : newSnapshotIndices) {
                assertThat(client().prepareSearch(searchIdx).setSize(0).get().getHits().totalHits(),
                           greaterThanOrEqualTo((long)addedDocSize));
            }
            deleteIndices(newSnapshotIndices); // clean up indices before starting again
        }
    }

    private List<String> listRepoVersions() throws Exception {
        final String prefix = "repo";
        final List<String> repoVersions = new ArrayList<>();
        final Path repoFiles = getBwcIndicesPath();
        try (final DirectoryStream<Path> dirStream = Files.newDirectoryStream(repoFiles, prefix + "-*.zip")) {
            for (final Path entry : dirStream) {
                final String fileName = entry.getFileName().toString();
                String version = fileName.substring(prefix.length() + 1);
                version = version.substring(0, version.length() - ".zip".length());
                repoVersions.add(version);
            }
        }
        return Collections.unmodifiableList(repoVersions);
    }

    private void createRepository(final String version, final String repoName) throws Exception {
        final String prefix = "repo";
        final Path repoFile = getBwcIndicesPath().resolve(prefix + "-" + version + ".zip");
        final Path repoPath = randomRepoPath();
        FileTestUtils.unzip(repoFile, repoPath, "repo/");
        assertAcked(client().admin().cluster().preparePutRepository(repoName)
                                              .setType("fs")
                                              .setSettings(Settings.builder().put("location", repoPath)));
    }

    private List<SnapshotInfo> getSnapshots(final String repoName) throws Exception {
        return client().admin().cluster().prepareGetSnapshots(repoName)
                                         .addSnapshots("_all")
                                         .get()
                                         .getSnapshots();
    }

    private SnapshotInfo createSnapshot(final String repoName, final String snapshotName) throws Exception {
        return client().admin().cluster().prepareCreateSnapshot(repoName, snapshotName)
                                         .setWaitForCompletion(true)
                                         .get()
                                         .getSnapshotInfo();
    }

    private List<String> restoreSnapshot(final String repoName, final String snapshotName) throws Exception {
        return client().admin().cluster().prepareRestoreSnapshot(repoName, snapshotName)
                                         .setWaitForCompletion(true)
                                         .get()
                                         .getRestoreInfo()
                                         .indices();
    }

    private void deleteIndices(final Set<String> indices) throws Exception {
        client().admin().indices().prepareDelete(indices.toArray(new String[indices.size()])).get();
    }

}
