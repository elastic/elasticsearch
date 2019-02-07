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

package org.elasticsearch.action.admin.cluster.repositories;

import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * This class tests that repository operations (Put, Delete, Verify) are blocked when the cluster is read-only.
 *
 * The @NodeScope TEST is needed because this class updates the cluster setting "cluster.blocks.read_only".
 */
@ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class RepositoryBlocksIT extends ESIntegTestCase {
    public void testPutRepositoryWithBlocks() {
        logger.info("-->  registering a repository is blocked when the cluster is read only");
        try {
            setClusterReadOnly(true);
            assertBlocked(client().admin().cluster().preparePutRepository("test-repo-blocks")
                    .setType("fs")
                    .setVerify(false)
                    .setSettings(Settings.builder().put("location",  randomRepoPath())), MetaData.CLUSTER_READ_ONLY_BLOCK);
        } finally {
            setClusterReadOnly(false);
        }

        logger.info("-->  registering a repository is allowed when the cluster is not read only");
        assertAcked(client().admin().cluster().preparePutRepository("test-repo-blocks")
                .setType("fs")
                .setVerify(false)
                .setSettings(Settings.builder().put("location",  randomRepoPath())));
    }

    public void testVerifyRepositoryWithBlocks() {
        assertAcked(client().admin().cluster().preparePutRepository("test-repo-blocks")
                .setType("fs")
                .setVerify(false)
                .setSettings(Settings.builder().put("location",  randomRepoPath())));

        // This test checks that the Get Repository operation is never blocked, even if the cluster is read only.
        try {
            setClusterReadOnly(true);
            VerifyRepositoryResponse response = client().admin().cluster()
                .prepareVerifyRepository("test-repo-blocks").execute().actionGet();
            assertThat(response.getNodes().size(), equalTo(cluster().numDataAndMasterNodes()));
        } finally {
            setClusterReadOnly(false);
        }
    }

    public void testDeleteRepositoryWithBlocks() {
        assertAcked(client().admin().cluster().preparePutRepository("test-repo-blocks")
                .setType("fs")
                .setVerify(false)
                .setSettings(Settings.builder().put("location",  randomRepoPath())));

        logger.info("-->  deleting a repository is blocked when the cluster is read only");
        try {
            setClusterReadOnly(true);
            assertBlocked(client().admin().cluster().prepareDeleteRepository("test-repo-blocks"), MetaData.CLUSTER_READ_ONLY_BLOCK);
        } finally {
            setClusterReadOnly(false);
        }

        logger.info("-->  deleting a repository is allowed when the cluster is not read only");
        assertAcked(client().admin().cluster().prepareDeleteRepository("test-repo-blocks"));
    }

    public void testGetRepositoryWithBlocks() {
        assertAcked(client().admin().cluster().preparePutRepository("test-repo-blocks")
                .setType("fs")
                .setVerify(false)
                .setSettings(Settings.builder().put("location",  randomRepoPath())));

        // This test checks that the Get Repository operation is never blocked, even if the cluster is read only.
        try {
            setClusterReadOnly(true);
            GetRepositoriesResponse response = client().admin().cluster().prepareGetRepositories("test-repo-blocks").execute().actionGet();
            assertThat(response.repositories(), hasSize(1));
        } finally {
            setClusterReadOnly(false);
        }
    }
}
