/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.repositories.stats.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.repositories.RepositoryInfo;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.RepositoryStatsSnapshot;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class RepositoriesStatsResponseTests extends ESTestCase {
    public void testSerializationRoundtrip() throws Exception {
        final RepositoriesStatsResponse repositoriesStatsResponse = createResponse();
        final RepositoriesStatsResponse deserializedResponse = copyWriteable(
            repositoriesStatsResponse,
            writableRegistry(),
            RepositoriesStatsResponse::new,
            Version.CURRENT
        );
        assertResponsesAreEqual(repositoriesStatsResponse, deserializedResponse);
    }

    private void assertResponsesAreEqual(RepositoriesStatsResponse response, RepositoriesStatsResponse otherResponse) {
        List<RepositoriesNodeStatsResponse> nodeResponses = response.getNodes();
        List<RepositoriesNodeStatsResponse> otherNodeResponses = otherResponse.getNodes();
        assertThat(nodeResponses.size(), equalTo(otherNodeResponses.size()));
        for (int i = 0; i < nodeResponses.size(); i++) {
            RepositoriesNodeStatsResponse nodeResponse = nodeResponses.get(i);
            RepositoriesNodeStatsResponse otherNodeResponse = otherNodeResponses.get(i);
            assertThat(nodeResponse.repositoryStatsSnapshots, equalTo(otherNodeResponse.repositoryStatsSnapshots));
        }

        List<FailedNodeException> failures = response.failures();
        List<FailedNodeException> otherFailures = otherResponse.failures();
        assertThat(failures.size(), equalTo(otherFailures.size()));
        for (int i = 0; i < failures.size(); i++) {
            FailedNodeException failure = failures.get(i);
            FailedNodeException otherFailure = otherFailures.get(i);
            assertThat(failure.nodeId(), equalTo(otherFailure.nodeId()));
            assertThat(failure.getMessage(), equalTo(otherFailure.getMessage()));
        }
    }

    private RepositoriesStatsResponse createResponse() {
        ClusterName clusterName = new ClusterName("test");
        int nodes = randomIntBetween(1, 10);
        List<RepositoriesNodeStatsResponse> nodeResponses = new ArrayList<>(nodes);
        for (int i = 0; i < nodes; i++) {
            DiscoveryNode node = new DiscoveryNode("nodeId" + i, buildNewFakeTransportAddress(), Version.CURRENT);
            int numberOfRepos = randomInt(10);
            List<RepositoryStatsSnapshot> nodeRepoStats = new ArrayList<>(numberOfRepos);

            for (int j = 0; j < numberOfRepos; j++) {
                String repoId = randomAlphaOfLength(10);
                String repoName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
                String repoType = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
                String repoLocation = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
                Long startedAt = System.currentTimeMillis() - 1;
                Long stoppedAt = randomBoolean() ? System.currentTimeMillis() : null;
                RepositoryInfo repositoryInfo = new RepositoryInfo(repoId, repoName, repoType, repoLocation, startedAt, stoppedAt);
                RepositoryStatsSnapshot statsSnapshot = new RepositoryStatsSnapshot(
                    repositoryInfo,
                    new RepositoryStats(Map.of("GET", randomLongBetween(0, 2000))),
                    null
                );
                nodeRepoStats.add(statsSnapshot);
            }

            nodeResponses.add(new RepositoriesNodeStatsResponse(node, nodeRepoStats));
        }

        int numberOfFailures = randomInt(20);
        List<FailedNodeException> failures = new ArrayList<>(numberOfFailures);
        for (int i = nodes; i < numberOfFailures + nodes; i++) {
            FailedNodeException failedNodeException = new FailedNodeException(
                "nodeId" + i,
                "error",
                randomBoolean() ? new RuntimeException("boom") : null
            );
            failures.add(failedNodeException);
        }

        return new RepositoriesStatsResponse(clusterName, nodeResponses, failures);
    }
}
