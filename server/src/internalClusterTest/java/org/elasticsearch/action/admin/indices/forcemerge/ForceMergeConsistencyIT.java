/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.forcemerge;

import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Arrays;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Demonstrates that the issue in https://github.com/elastic/elasticsearch/issues/13719 also affects simple rolling restart.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ForceMergeConsistencyIT extends ESIntegTestCase {

    private static final String INDEX = "test";

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/13719")
    public void testUnavailableCopy() throws Exception {
        internalCluster().startMasterOnlyNode();
        String primaryNode = internalCluster().startDataOnlyNode();
        createIndex(
            INDEX,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0)
                .build()
        );

        assertThat(clusterAdmin().prepareHealth(INDEX).get().getStatus(), is(ClusterHealthStatus.YELLOW));

        String replicaNode = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX);

        IntStream.range(0, 50)
            .forEach(
                i -> client().prepareIndex(INDEX)
                    .setSource("field", randomAlphaOfLength(5))
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .get()
            );

        client().admin().indices().prepareFlush(INDEX);
        String restartNode = randomFrom(primaryNode, replicaNode);
        boolean stopOtherNode = randomBoolean();
        String otherNode = restartNode.equals(primaryNode) ? replicaNode : primaryNode;

        logger.info("--> restarting [{}], stopping other [{}]", restartNode, stopOtherNode);
        internalCluster().restartNode(restartNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ensureYellowAndNoInitializingShards(INDEX);
                ensureClusterStateConsistency();
                ForceMergeResponse forceMergeResponse = client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();
                assertThat(forceMergeResponse.getFailedShards(), equalTo(0));
                assertOneSegment(1);
                if (stopOtherNode) {
                    internalCluster().stopNode(otherNode);
                }
                return super.onNodeStopped(nodeName);
            }
        });

        if (stopOtherNode) {
            internalCluster().startDataOnlyNode();
        }
        ensureGreen(INDEX);
        assertOneSegment(2);
    }

    private void assertOneSegment(int expectedShardCopies) {
        IndicesSegmentResponse indicesSegmentResponse = client().admin().indices().prepareSegments(INDEX).get();
        assertThat(indicesSegmentResponse.getFailedShards(), equalTo(0));
        indicesSegmentResponse.getIndices()
            .get(INDEX)
            .getShards()
            .values()
            .stream()
            .peek(shard -> assertThat(shard.getShards().length, equalTo(expectedShardCopies)))
            .flatMap(shard -> Arrays.stream(shard.getShards()))
            .forEach(segments -> {
                assertThat(segments.getNumberOfCommitted(), equalTo(1));
                assertThat(segments.getNumberOfSearch(), equalTo(1));
            });
    }
}
