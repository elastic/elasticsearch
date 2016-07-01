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
package org.elasticsearch.snapshots;

import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.test.ESIntegTestCase;

import java.nio.file.Path;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 */
@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class RepositoriesIT extends AbstractSnapshotIntegTestCase {
    public void testRepositoryCreation() throws Exception {
        Client client = client();

        Path location = randomRepoPath();

        logger.info("-->  creating repository");
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo-1")
                .setType("fs").setSettings(Settings.builder()
                                .put("location", location)
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> verify the repository");
        int numberOfFiles = FileSystemUtils.files(location).length;
        VerifyRepositoryResponse verifyRepositoryResponse = client.admin().cluster().prepareVerifyRepository("test-repo-1").get();
        assertThat(verifyRepositoryResponse.getNodes().length, equalTo(cluster().numDataAndMasterNodes()));

        logger.info("--> verify that we didn't leave any files as a result of verification");
        assertThat(FileSystemUtils.files(location).length, equalTo(numberOfFiles));

        logger.info("--> check that repository is really there");
        ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState().clear().setMetaData(true).get();
        MetaData metaData = clusterStateResponse.getState().getMetaData();
        RepositoriesMetaData repositoriesMetaData = metaData.custom(RepositoriesMetaData.TYPE);
        assertThat(repositoriesMetaData, notNullValue());
        assertThat(repositoriesMetaData.repository("test-repo-1"), notNullValue());
        assertThat(repositoriesMetaData.repository("test-repo-1").type(), equalTo("fs"));

        logger.info("-->  creating another repository");
        putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo-2")
                .setType("fs").setSettings(Settings.builder()
                                .put("location", randomRepoPath())
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> check that both repositories are in cluster state");
        clusterStateResponse = client.admin().cluster().prepareState().clear().setMetaData(true).get();
        metaData = clusterStateResponse.getState().getMetaData();
        repositoriesMetaData = metaData.custom(RepositoriesMetaData.TYPE);
        assertThat(repositoriesMetaData, notNullValue());
        assertThat(repositoriesMetaData.repositories().size(), equalTo(2));
        assertThat(repositoriesMetaData.repository("test-repo-1"), notNullValue());
        assertThat(repositoriesMetaData.repository("test-repo-1").type(), equalTo("fs"));
        assertThat(repositoriesMetaData.repository("test-repo-2"), notNullValue());
        assertThat(repositoriesMetaData.repository("test-repo-2").type(), equalTo("fs"));

        logger.info("--> check that both repositories can be retrieved by getRepositories query");
        GetRepositoriesResponse repositoriesResponse = client.admin().cluster()
            .prepareGetRepositories(randomFrom("_all", "*", "test-repo-*")).get();
        assertThat(repositoriesResponse.repositories().size(), equalTo(2));
        assertThat(findRepository(repositoriesResponse.repositories(), "test-repo-1"), notNullValue());
        assertThat(findRepository(repositoriesResponse.repositories(), "test-repo-2"), notNullValue());

        logger.info("--> delete repository test-repo-1");
        client.admin().cluster().prepareDeleteRepository("test-repo-1").get();
        repositoriesResponse = client.admin().cluster().prepareGetRepositories().get();
        assertThat(repositoriesResponse.repositories().size(), equalTo(1));
        assertThat(findRepository(repositoriesResponse.repositories(), "test-repo-2"), notNullValue());

        logger.info("--> delete repository test-repo-2");
        client.admin().cluster().prepareDeleteRepository("test-repo-2").get();
        repositoriesResponse = client.admin().cluster().prepareGetRepositories().get();
        assertThat(repositoriesResponse.repositories().size(), equalTo(0));
    }

    private RepositoryMetaData findRepository(List<RepositoryMetaData> repositories, String name) {
        for (RepositoryMetaData repository : repositories) {
            if (repository.name().equals(name)) {
                return repository;
            }
        }
        return null;
    }

    public void testMisconfiguredRepository() throws Exception {
        Client client = client();

        logger.info("--> trying creating repository with incorrect settings");
        try {
            client.admin().cluster().preparePutRepository("test-repo").setType("fs").get();
            fail("Shouldn't be here");
        } catch (RepositoryException ex) {
            assertThat(ex.toString(), containsString("missing location"));
        }

        logger.info("--> trying creating fs repository with location that is not registered in path.repo setting");
        Path invalidRepoPath = createTempDir().toAbsolutePath();
        String location = invalidRepoPath.toString();
        try {
            client().admin().cluster().preparePutRepository("test-repo")
                    .setType("fs").setSettings(Settings.builder().put("location", location))
                    .get();
            fail("Shouldn't be here");
        } catch (RepositoryException ex) {
            assertThat(ex.toString(), containsString("location [" + location + "] doesn't match any of the locations specified by path.repo"));
        }

        String repoUrl = invalidRepoPath.toAbsolutePath().toUri().toURL().toString();
        String unsupportedUrl = repoUrl.replace("file:/", "netdoc:/");
        logger.info("--> trying creating url repository with unsupported url protocol");
        try {
            client().admin().cluster().preparePutRepository("test-repo")
                    .setType("url").setSettings(Settings.builder().put("url", unsupportedUrl))
                    .get();
            fail("Shouldn't be here");
        } catch (RepositoryException ex) {
            assertThat(ex.toString(), containsString("unsupported url protocol [netdoc]"));
        }

        logger.info("--> trying creating url repository with location that is not registered in path.repo setting");
        try {
            client().admin().cluster().preparePutRepository("test-repo")
                    .setType("url").setSettings(Settings.builder().put("url", invalidRepoPath.toUri().toURL()))
                    .get();
            fail("Shouldn't be here");
        } catch (RepositoryException ex) {
            assertThat(ex.toString(), containsString("doesn't match any of the locations specified by path.repo"));
        }
    }

    public void testRepositoryAckTimeout() throws Exception {
        logger.info("-->  creating repository test-repo-1 with 0s timeout - shouldn't ack");
        PutRepositoryResponse putRepositoryResponse = client().admin().cluster().preparePutRepository("test-repo-1")
                .setType("fs").setSettings(Settings.builder()
                                .put("location", randomRepoPath())
                                .put("compress", randomBoolean())
                                .put("chunk_size", randomIntBetween(5, 100), ByteSizeUnit.BYTES)
                )
                .setTimeout("0s").get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(false));

        logger.info("-->  creating repository test-repo-2 with standard timeout - should ack");
        putRepositoryResponse = client().admin().cluster().preparePutRepository("test-repo-2")
                .setType("fs").setSettings(Settings.builder()
                                .put("location", randomRepoPath())
                                .put("compress", randomBoolean())
                                .put("chunk_size", randomIntBetween(5, 100), ByteSizeUnit.BYTES)
                ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("-->  deleting repository test-repo-2 with 0s timeout - shouldn't ack");
        DeleteRepositoryResponse deleteRepositoryResponse = client().admin().cluster().prepareDeleteRepository("test-repo-2")
                .setTimeout("0s").get();
        assertThat(deleteRepositoryResponse.isAcknowledged(), equalTo(false));

        logger.info("-->  deleting repository test-repo-1 with standard timeout - should ack");
        deleteRepositoryResponse = client().admin().cluster().prepareDeleteRepository("test-repo-1").get();
        assertThat(deleteRepositoryResponse.isAcknowledged(), equalTo(true));
    }

    public void testRepositoryVerification() throws Exception {
        Client client = client();

        Settings settings = Settings.builder()
                .put("location", randomRepoPath())
                .put("random_control_io_exception_rate", 1.0).build();
        logger.info("-->  creating repository that cannot write any files - should fail");
        assertThrows(client.admin().cluster().preparePutRepository("test-repo-1")
                        .setType("mock").setSettings(settings),
                RepositoryVerificationException.class);

        logger.info("-->  creating repository that cannot write any files, but suppress verification - should be acked");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo-1")
                .setType("mock").setSettings(settings).setVerify(false));

        logger.info("-->  verifying repository");
        assertThrows(client.admin().cluster().prepareVerifyRepository("test-repo-1"), RepositoryVerificationException.class);

        Path location = randomRepoPath();

        logger.info("-->  creating repository");
        try {
            client.admin().cluster().preparePutRepository("test-repo-1")
                    .setType("mock")
                    .setSettings(Settings.builder()
                                    .put("location", location)
                                    .put("localize_location", true)
                    ).get();
            fail("RepositoryVerificationException wasn't generated");
        } catch (RepositoryVerificationException ex) {
            assertThat(ex.getMessage(), containsString("is not shared"));
        }
    }

    public void testRepositoryVerificationTimeout() throws Exception {
        Client client = client();

        Settings settings = Settings.builder()
                .put("location", randomRepoPath())
                .put("random_control_io_exception_rate", 1.0).build();
        logger.info("-->  creating repository that cannot write any files - should fail");
        assertThrows(client.admin().cluster().preparePutRepository("test-repo-1")
                        .setType("mock").setSettings(settings),
                RepositoryVerificationException.class);

        logger.info("-->  creating repository that cannot write any files, but suppress verification - should be acked");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo-1")
                .setType("mock").setSettings(settings).setVerify(false));

        logger.info("-->  verifying repository");
        assertThrows(client.admin().cluster().prepareVerifyRepository("test-repo-1"), RepositoryVerificationException.class);

        Path location = randomRepoPath();

        logger.info("-->  creating repository");
        try {
            client.admin().cluster().preparePutRepository("test-repo-1")
                    .setType("mock")
                    .setSettings(Settings.builder()
                                    .put("location", location)
                                    .put("localize_location", true)
                    ).get();
            fail("RepositoryVerificationException wasn't generated");
        } catch (RepositoryVerificationException ex) {
            assertThat(ex.getMessage(), containsString("is not shared"));
        }
    }

}
