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

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

public class MultiClusterRepoAccessIT extends AbstractSnapshotIntegTestCase {

    private InternalTestCluster secondCluster;
    private Path repoPath;

    @Before
    public void startSecondCluster() throws IOException, InterruptedException {
        repoPath = randomRepoPath();
        secondCluster = new InternalTestCluster(randomLong(), createTempDir(), true, true, 0,
                0, "second_cluster", new NodeConfigurationSource() {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                return Settings.builder().put(MultiClusterRepoAccessIT.this.nodeSettings(nodeOrdinal))
                        .put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType())
                        .put(Environment.PATH_REPO_SETTING.getKey(), repoPath).build();
            }

            @Override
            public Path nodeConfigPath(int nodeOrdinal) {
                return null;
            }
        }, 0, "leader", Arrays.asList(ESIntegTestCase.TestSeedPlugin.class,
                MockHttpTransport.TestPlugin.class, MockTransportService.TestPlugin.class,
                MockNioTransportPlugin.class, InternalSettingsPlugin.class, MockRepository.Plugin.class), Function.identity());
        secondCluster.beforeTest(random(), 0);
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
        secondCluster.client().admin().cluster().preparePutRepository(repoNameOnSecondCluster).setType("fs")
                .setSettings(Settings.builder().put("location", repoPath)).get();

        createIndexWithRandomDocs("test-idx-1", randomIntBetween(1, 100));
        createFullSnapshot(repoNameOnFirstCluster, "snap-1");
        createIndexWithRandomDocs("test-idx-2", randomIntBetween(1, 100));
        createFullSnapshot(repoNameOnFirstCluster, "snap-2");
        createIndexWithRandomDocs("test-idx-3", randomIntBetween(1, 100));
        createFullSnapshot(repoNameOnFirstCluster, "snap-3");

        secondCluster.client().admin().cluster().prepareDeleteSnapshot(repoNameOnSecondCluster, "snap-1").get();
        secondCluster.client().admin().cluster().prepareDeleteSnapshot(repoNameOnSecondCluster, "snap-2").get();

        final SnapshotException sne = expectThrows(SnapshotException.class, () ->
                client().admin().cluster().prepareCreateSnapshot(repoNameOnFirstCluster, "snap-4").setWaitForCompletion(true)
                        .execute().actionGet());
        assertThat(sne.getMessage(), containsString("failed to update snapshot in repository"));
        final RepositoryException cause = (RepositoryException) sne.getCause();
        assertThat(cause.getMessage(), containsString("[" + repoNameOnFirstCluster +
                "] concurrent modification of the index-N file, expected current generation [2] but it was not found in the repository"));
        assertAcked(client().admin().cluster().prepareDeleteRepository(repoNameOnFirstCluster).get());
        createRepository(repoNameOnFirstCluster, "fs", repoPath);
        createFullSnapshot(repoNameOnFirstCluster, "snap-5");
    }
}
