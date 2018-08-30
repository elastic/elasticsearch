/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ccr.action.DeleteAutoFollowPatternAction.Request;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class TransportDeleteAutoFollowPatternActionTests extends ESTestCase {

    public void testInnerDelete() {
        Map<String, List<String>> existingAlreadyFollowedIndexUUIDS = new HashMap<>();
        Map<String, AutoFollowMetadata.AutoFollowPattern> existingAutoFollowPatterns = new HashMap<>();
        {
            List<String> existingPatterns = new ArrayList<>();
            existingPatterns.add("transactions-*");
            existingAutoFollowPatterns.put("eu_cluster",
                new AutoFollowMetadata.AutoFollowPattern(existingPatterns, null, null, null, null, null, null, null, null));

            List<String> existingUUIDS = new ArrayList<>();
            existingUUIDS.add("_val");
            existingAlreadyFollowedIndexUUIDS.put("eu_cluster", existingUUIDS);
        }
        {
            List<String> existingPatterns = new ArrayList<>();
            existingPatterns.add("logs-*");
            existingAutoFollowPatterns.put("asia_cluster",
                new AutoFollowMetadata.AutoFollowPattern(existingPatterns, null, null, null, null, null, null, null, null));

            List<String> existingUUIDS = new ArrayList<>();
            existingUUIDS.add("_val");
            existingAlreadyFollowedIndexUUIDS.put("asia_cluster", existingUUIDS);
        }
        ClusterState clusterState = ClusterState.builder(new ClusterName("us_cluster"))
            .metaData(MetaData.builder().putCustom(AutoFollowMetadata.TYPE,
                new AutoFollowMetadata(existingAutoFollowPatterns, existingAlreadyFollowedIndexUUIDS)))
            .build();

        Request request = new Request();
        request.setLeaderClusterAlias("eu_cluster");
        AutoFollowMetadata result = TransportDeleteAutoFollowPatternAction.innerDelete(request, clusterState)
            .getMetaData()
            .custom(AutoFollowMetadata.TYPE);
        assertThat(result.getPatterns().size(), equalTo(1));
        assertThat(result.getPatterns().get("asia_cluster"), notNullValue());
        assertThat(result.getFollowedLeaderIndexUUIDs().size(), equalTo(1));
        assertThat(result.getFollowedLeaderIndexUUIDs().get("asia_cluster"), notNullValue());
    }

    public void testInnerDeleteDoesNotExist() {
        Map<String, List<String>> existingAlreadyFollowedIndexUUIDS = new HashMap<>();
        Map<String, AutoFollowMetadata.AutoFollowPattern> existingAutoFollowPatterns = new HashMap<>();
        {
            List<String> existingPatterns = new ArrayList<>();
            existingPatterns.add("transactions-*");
            existingAutoFollowPatterns.put("eu_cluster",
                new AutoFollowMetadata.AutoFollowPattern(existingPatterns, null, null, null, null, null, null, null, null));
        }
        ClusterState clusterState = ClusterState.builder(new ClusterName("us_cluster"))
            .metaData(MetaData.builder().putCustom(AutoFollowMetadata.TYPE,
                new AutoFollowMetadata(existingAutoFollowPatterns, existingAlreadyFollowedIndexUUIDS)))
            .build();

        Request request = new Request();
        request.setLeaderClusterAlias("asia_cluster");
        Exception e = expectThrows(ResourceNotFoundException.class,
            () -> TransportDeleteAutoFollowPatternAction.innerDelete(request, clusterState));
        assertThat(e.getMessage(), equalTo("no auto-follow patterns for cluster alias [asia_cluster] found"));
    }

    public void testInnerDeleteNoAutoFollowMetadata() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("us_cluster"))
            .metaData(MetaData.builder())
            .build();

        Request request = new Request();
        request.setLeaderClusterAlias("asia_cluster");
        Exception e = expectThrows(ResourceNotFoundException.class,
            () -> TransportDeleteAutoFollowPatternAction.innerDelete(request, clusterState));
        assertThat(e.getMessage(), equalTo("no auto-follow patterns for cluster alias [asia_cluster] found"));
    }

}
