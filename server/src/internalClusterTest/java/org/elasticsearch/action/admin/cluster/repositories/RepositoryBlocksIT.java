/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.repositories;

import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.cluster.metadata.Metadata;
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
            assertBlocked(
                clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo-blocks")
                    .setType("fs")
                    .setVerify(false)
                    .setSettings(Settings.builder().put("location", randomRepoPath())),
                Metadata.CLUSTER_READ_ONLY_BLOCK
            );
        } finally {
            setClusterReadOnly(false);
        }

        logger.info("-->  registering a repository is allowed when the cluster is not read only");
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo-blocks")
                .setType("fs")
                .setVerify(false)
                .setSettings(Settings.builder().put("location", randomRepoPath()))
        );
    }

    public void testVerifyRepositoryWithBlocks() {
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo-blocks")
                .setType("fs")
                .setVerify(false)
                .setSettings(Settings.builder().put("location", randomRepoPath()))
        );

        // This test checks that the Get Repository operation is never blocked, even if the cluster is read only.
        try {
            setClusterReadOnly(true);
            VerifyRepositoryResponse response = clusterAdmin().prepareVerifyRepository(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                "test-repo-blocks"
            ).get();
            assertThat(response.getNodes().size(), equalTo(cluster().numDataAndMasterNodes()));
        } finally {
            setClusterReadOnly(false);
        }
    }

    public void testDeleteRepositoryWithBlocks() {
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo-blocks")
                .setType("fs")
                .setVerify(false)
                .setSettings(Settings.builder().put("location", randomRepoPath()))
        );

        logger.info("-->  deleting a repository is blocked when the cluster is read only");
        try {
            setClusterReadOnly(true);
            assertBlocked(
                clusterAdmin().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo-blocks"),
                Metadata.CLUSTER_READ_ONLY_BLOCK
            );
        } finally {
            setClusterReadOnly(false);
        }

        logger.info("-->  deleting a repository is allowed when the cluster is not read only");
        assertAcked(clusterAdmin().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo-blocks"));
    }

    public void testGetRepositoryWithBlocks() {
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo-blocks")
                .setType("fs")
                .setVerify(false)
                .setSettings(Settings.builder().put("location", randomRepoPath()))
        );

        // This test checks that the Get Repository operation is never blocked, even if the cluster is read only.
        try {
            setClusterReadOnly(true);
            GetRepositoriesResponse response = clusterAdmin().prepareGetRepositories(TEST_REQUEST_TIMEOUT, "test-repo-blocks").get();
            assertThat(response.repositories(), hasSize(1));
        } finally {
            setClusterReadOnly(false);
        }
    }
}
