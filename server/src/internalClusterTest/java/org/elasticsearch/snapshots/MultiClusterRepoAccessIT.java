/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.MockHttpTransport;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.nio.MockNioTransportPlugin;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Function;

import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.READONLY_SETTING_KEY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class MultiClusterRepoAccessIT extends AbstractSnapshotIntegTestCase {

    private InternalTestCluster secondCluster;
    private Path repoPath;

    @Before
    public void startSecondCluster() throws IOException, InterruptedException {
        repoPath = randomRepoPath();
        secondCluster = new InternalTestCluster(
            randomLong(),
            createTempDir(),
            true,
            true,
            0,
            0,
            "second_cluster",
            new NodeConfigurationSource() {
                @Override
                public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
                    return Settings.builder()
                        .put(MultiClusterRepoAccessIT.this.nodeSettings(nodeOrdinal, otherSettings))
                        .put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType())
                        .put(Environment.PATH_REPO_SETTING.getKey(), repoPath)
                        .build();
                }

                @Override
                public Path nodeConfigPath(int nodeOrdinal) {
                    return null;
                }
            },
            0,
            "leader",
            Arrays.asList(
                ESIntegTestCase.TestSeedPlugin.class,
                MockHttpTransport.TestPlugin.class,
                MockTransportService.TestPlugin.class,
                MockNioTransportPlugin.class,
                InternalSettingsPlugin.class,
                MockRepository.Plugin.class
            ),
            Function.identity()
        );
        secondCluster.beforeTest(random());
    }

    @After
    public void stopSecondCluster() throws IOException {
        IOUtils.close(secondCluster);
    }

    public void testConcurrentDeleteFromOtherCluster() throws InterruptedException {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoNameOnFirstCluster = "test-repo";
        final String repoNameOnSecondCluster = randomBoolean() ? "test-repo" : "other-repo";
        createRepository(repoNameOnFirstCluster, "fs", repoPath);

        secondCluster.startMasterOnlyNode();
        secondCluster.startDataOnlyNode();

        createIndexWithRandomDocs("test-idx-1", randomIntBetween(1, 100));
        createFullSnapshot(repoNameOnFirstCluster, "snap-1");
        createIndexWithRandomDocs("test-idx-2", randomIntBetween(1, 100));
        createFullSnapshot(repoNameOnFirstCluster, "snap-2");
        createIndexWithRandomDocs("test-idx-3", randomIntBetween(1, 100));
        createFullSnapshot(repoNameOnFirstCluster, "snap-3");

        secondCluster.client()
            .admin()
            .cluster()
            .preparePutRepository(repoNameOnSecondCluster)
            .setType("fs")
            .setSettings(Settings.builder().put("location", repoPath))
            .get();
        secondCluster.client().admin().cluster().prepareDeleteSnapshot(repoNameOnSecondCluster, "snap-1").get();
        secondCluster.client().admin().cluster().prepareDeleteSnapshot(repoNameOnSecondCluster, "snap-2").get();

        final SnapshotException sne = expectThrows(
            SnapshotException.class,
            () -> client().admin()
                .cluster()
                .prepareCreateSnapshot(repoNameOnFirstCluster, "snap-4")
                .setWaitForCompletion(true)
                .execute()
                .actionGet()
        );
        assertThat(sne.getMessage(), containsString("failed to update snapshot in repository"));
        final RepositoryException cause = (RepositoryException) sne.getCause();
        assertThat(
            cause.getMessage(),
            containsString(
                "["
                    + repoNameOnFirstCluster
                    + "] concurrent modification of the index-N file, expected current generation [2] but it was not found in "
                    + "the repository. The last cluster to write to this repository was ["
                    + secondCluster.client().admin().cluster().prepareState().get().getState().metadata().clusterUUID()
                    + "] at generation [4]."
            )
        );
        assertAcked(client().admin().cluster().prepareDeleteRepository(repoNameOnFirstCluster).get());
        createRepository(repoNameOnFirstCluster, "fs", repoPath);
        createFullSnapshot(repoNameOnFirstCluster, "snap-5");
    }

    public void testConcurrentWipeAndRecreateFromOtherCluster() throws InterruptedException, IOException {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "fs", repoPath);

        createIndexWithRandomDocs("test-idx-1", randomIntBetween(1, 100));
        createFullSnapshot(repoName, "snap-1");
        final String repoUuid = client().admin()
            .cluster()
            .prepareGetRepositories(repoName)
            .get()
            .repositories()
            .stream()
            .filter(r -> r.name().equals(repoName))
            .findFirst()
            .orElseThrow()
            .uuid();

        secondCluster.startMasterOnlyNode();
        secondCluster.startDataOnlyNode();
        assertAcked(
            secondCluster.client()
                .admin()
                .cluster()
                .preparePutRepository(repoName)
                .setType("fs")
                .setSettings(Settings.builder().put("location", repoPath).put(READONLY_SETTING_KEY, true))
        );
        assertThat(
            secondCluster.client()
                .admin()
                .cluster()
                .prepareGetRepositories(repoName)
                .get()
                .repositories()
                .stream()
                .filter(r -> r.name().equals(repoName))
                .findFirst()
                .orElseThrow()
                .uuid(),
            equalTo(repoUuid)
        );

        assertAcked(client().admin().cluster().prepareDeleteRepository(repoName));
        IOUtils.rm(internalCluster().getCurrentMasterNodeInstance(Environment.class).resolveRepoFile(repoPath.toString()));
        createRepository(repoName, "fs", repoPath);
        createFullSnapshot(repoName, "snap-1");

        final String newRepoUuid = client().admin()
            .cluster()
            .prepareGetRepositories(repoName)
            .get()
            .repositories()
            .stream()
            .filter(r -> r.name().equals(repoName))
            .findFirst()
            .orElseThrow()
            .uuid();
        assertThat(newRepoUuid, not(equalTo((repoUuid))));

        secondCluster.client().admin().cluster().prepareGetSnapshots(repoName).get(); // force another read of the repo data
        assertThat(
            secondCluster.client()
                .admin()
                .cluster()
                .prepareGetRepositories(repoName)
                .get()
                .repositories()
                .stream()
                .filter(r -> r.name().equals(repoName))
                .findFirst()
                .orElseThrow()
                .uuid(),
            equalTo(newRepoUuid)
        );
    }
}
