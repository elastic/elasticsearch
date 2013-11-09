/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.carrotsearch.randomizedtesting.LifecycleScope;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.repositories.RepositoryException;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 */
public class RepositoriesTests extends AbstractSnapshotTests {

    @Before
    public final void beforeSnapshots() {
        wipeRepositories();
    }

    @Test
    public void testRepositoryCreation() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        PutRepositoryResponse putRepositoryResponse = run(client.admin().cluster().preparePutRepository("test-repo-1")
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder()
                        .put("location", newTempDir(LifecycleScope.SUITE))
                ));
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> check that repository is really there");
        ClusterStateResponse clusterStateResponse = run(client.admin().cluster().prepareState().setFilterAll().setFilterMetaData(false));
        MetaData metaData = clusterStateResponse.getState().getMetaData();
        RepositoriesMetaData repositoriesMetaData = metaData.custom(RepositoriesMetaData.TYPE);
        assertThat(repositoriesMetaData, notNullValue());
        assertThat(repositoriesMetaData.repository("test-repo-1"), notNullValue());
        assertThat(repositoriesMetaData.repository("test-repo-1").type(), equalTo("fs"));

        logger.info("-->  creating anoter repository");
        putRepositoryResponse = run(client.admin().cluster().preparePutRepository("test-repo-2")
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder()
                        .put("location", newTempDir(LifecycleScope.SUITE))
                ));
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> check that both repositories are in cluster state");
        clusterStateResponse = run(client.admin().cluster().prepareState().setFilterAll().setFilterMetaData(false));
        metaData = clusterStateResponse.getState().getMetaData();
        repositoriesMetaData = metaData.custom(RepositoriesMetaData.TYPE);
        assertThat(repositoriesMetaData, notNullValue());
        assertThat(repositoriesMetaData.repositories().size(), equalTo(2));
        assertThat(repositoriesMetaData.repository("test-repo-1"), notNullValue());
        assertThat(repositoriesMetaData.repository("test-repo-1").type(), equalTo("fs"));
        assertThat(repositoriesMetaData.repository("test-repo-2"), notNullValue());
        assertThat(repositoriesMetaData.repository("test-repo-2").type(), equalTo("fs"));

        logger.info("--> check that both repositories can be retrieved by getRepositories query");
        GetRepositoriesResponse repositoriesResponse = run(client.admin().cluster().prepareGetRepositories());
        assertThat(repositoriesResponse.repositories().size(), equalTo(2));
        assertThat(findRepository(repositoriesResponse.repositories(), "test-repo-1"), notNullValue());
        assertThat(findRepository(repositoriesResponse.repositories(), "test-repo-2"), notNullValue());

        logger.info("--> delete repository test-repo-1");
        run(client.admin().cluster().prepareDeleteRepository("test-repo-1"));
        repositoriesResponse = run(client.admin().cluster().prepareGetRepositories());
        assertThat(repositoriesResponse.repositories().size(), equalTo(1));
        assertThat(findRepository(repositoriesResponse.repositories(), "test-repo-2"), notNullValue());

        logger.info("--> delete repository test-repo-2");
        run(client.admin().cluster().prepareDeleteRepository("test-repo-2"));
        repositoriesResponse = run(client.admin().cluster().prepareGetRepositories());
        assertThat(repositoriesResponse.repositories().size(), equalTo(0));
    }

    private RepositoryMetaData findRepository(ImmutableList<RepositoryMetaData> repositories, String name) {
        for (RepositoryMetaData repository : repositories) {
            if (repository.name().equals(name)) {
                return repository;
            }
        }
        return null;
    }

    @Test
    public void testMisconfiguredRepository() throws Exception {
        Client client = client();

        logger.info("--> trying creating repository with incorrect settings");
        try {
            run(client.admin().cluster().preparePutRepository("test-repo").setType("fs"));
            fail("Shouldn't be here");
        } catch (RepositoryException ex) {
            // Expected
        }
    }

    @Test
    public void repositoryAckTimeoutTest() throws Exception {

        logger.info("-->  creating repository test-repo-1 with 0s timeout - shouldn't ack");
        PutRepositoryResponse putRepositoryResponse = run(client().admin().cluster().preparePutRepository("test-repo-1")
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder()
                        .put("location", newTempDir(LifecycleScope.SUITE))
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(5, 100))
                )
                .setTimeout("0s"));
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(false));

        logger.info("-->  creating repository test-repo-2 with standard timeout - should ack");
        putRepositoryResponse = run(client().admin().cluster().preparePutRepository("test-repo-2")
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder()
                        .put("location", newTempDir(LifecycleScope.SUITE))
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(5, 100))
                ));
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("-->  deleting repository test-repo-2 with 0s timeout - shouldn't ack");
        DeleteRepositoryResponse deleteRepositoryResponse = run(client().admin().cluster().prepareDeleteRepository("test-repo-2")
                .setTimeout("0s"));
        assertThat(deleteRepositoryResponse.isAcknowledged(), equalTo(false));

        logger.info("-->  deleting repository test-repo-1 with standard timeout - should ack");
        deleteRepositoryResponse = run(client().admin().cluster().prepareDeleteRepository("test-repo-1"));
        assertThat(deleteRepositoryResponse.isAcknowledged(), equalTo(true));
    }


}
