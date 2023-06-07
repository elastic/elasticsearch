/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.repositories.metering.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.repositories.RepositoryInfo;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.RepositoryStatsSnapshot;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class RepositoriesMeteringResponseTests extends ESTestCase {
    public void testSerializationRoundtrip() throws Exception {
        final RepositoriesMeteringResponse repositoriesMeteringResponse = createResponse();
        final RepositoriesMeteringResponse deserializedResponse = copyWriteable(
            repositoriesMeteringResponse,
            writableRegistry(),
            RepositoriesMeteringResponse::new,
                TransportVersion.current()
        );
        assertResponsesAreEqual(repositoriesMeteringResponse, deserializedResponse);
    }

    private void assertResponsesAreEqual(RepositoriesMeteringResponse response, RepositoriesMeteringResponse otherResponse) {
        List<RepositoriesNodeMeteringResponse> nodeResponses = response.getNodes();
        List<RepositoriesNodeMeteringResponse> otherNodeResponses = otherResponse.getNodes();
        assertThat(nodeResponses.size(), equalTo(otherNodeResponses.size()));
        for (int i = 0; i < nodeResponses.size(); i++) {
            RepositoriesNodeMeteringResponse nodeResponse = nodeResponses.get(i);
            RepositoriesNodeMeteringResponse otherNodeResponse = otherNodeResponses.get(i);
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

    private RepositoriesMeteringResponse createResponse() {
        ClusterName clusterName = new ClusterName("test");
        int nodes = randomIntBetween(1, 10);
        List<RepositoriesNodeMeteringResponse> nodeResponses = new ArrayList<>(nodes);
        for (int nodeId = 0; nodeId < nodes; nodeId++) {
            DiscoveryNode node = DiscoveryNodeUtils.create("nodeId" + nodeId);
            int numberOfRepos = randomInt(10);
            List<RepositoryStatsSnapshot> nodeRepoStats = new ArrayList<>(numberOfRepos);

            for (int clusterVersion = 0; clusterVersion < numberOfRepos; clusterVersion++) {
                String repoId = randomAlphaOfLength(10);
                String repoName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
                String repoType = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
                Map<String, String> repoLocation = Map.of("bucket", randomAlphaOfLength(10).toLowerCase(Locale.ROOT));
                long startedAt = System.currentTimeMillis() - 1;
                Long stoppedAt = randomBoolean() ? System.currentTimeMillis() : null;
                RepositoryInfo repositoryInfo = new RepositoryInfo(repoId, repoName, repoType, repoLocation, startedAt, stoppedAt);
                boolean archived = randomBoolean();
                RepositoryStatsSnapshot statsSnapshot = new RepositoryStatsSnapshot(
                    repositoryInfo,
                    new RepositoryStats(Map.of("GET", randomLongBetween(0, 2000))),
                    archived ? clusterVersion : RepositoryStatsSnapshot.UNKNOWN_CLUSTER_VERSION,
                    archived
                );
                nodeRepoStats.add(statsSnapshot);
            }

            nodeResponses.add(new RepositoriesNodeMeteringResponse(node, nodeRepoStats));
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

        return new RepositoriesMeteringResponse(clusterName, nodeResponses, failures);
    }
}
