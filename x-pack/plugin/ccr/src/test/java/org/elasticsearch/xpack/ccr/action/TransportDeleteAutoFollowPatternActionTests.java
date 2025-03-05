/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;
import org.elasticsearch.xpack.core.ccr.action.DeleteAutoFollowPatternAction.Request;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class TransportDeleteAutoFollowPatternActionTests extends ESTestCase {

    public void testInnerDelete() {
        Map<String, List<String>> existingAlreadyFollowedIndexUUIDS = new HashMap<>();
        Map<String, Map<String, String>> existingHeaders = new HashMap<>();
        Map<String, AutoFollowPattern> existingAutoFollowPatterns = new HashMap<>();
        {
            List<String> existingPatterns = new ArrayList<>();
            existingPatterns.add("transactions-*");
            existingAutoFollowPatterns.put(
                "name1",
                new AutoFollowPattern(
                    "eu_cluster",
                    existingPatterns,
                    Collections.emptyList(),
                    null,
                    Settings.EMPTY,
                    true,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            );

            List<String> existingUUIDS = new ArrayList<>();
            existingUUIDS.add("_val");
            existingAlreadyFollowedIndexUUIDS.put("name1", existingUUIDS);
            existingHeaders.put("name1", Collections.singletonMap("key", "val"));
        }
        {
            List<String> existingPatterns = new ArrayList<>();
            existingPatterns.add("logs-*");
            existingAutoFollowPatterns.put(
                "name2",
                new AutoFollowPattern(
                    "asia_cluster",
                    existingPatterns,
                    Collections.emptyList(),
                    null,
                    Settings.EMPTY,
                    true,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            );

            List<String> existingUUIDS = new ArrayList<>();
            existingUUIDS.add("_val");
            existingAlreadyFollowedIndexUUIDS.put("name2", existingUUIDS);
            existingHeaders.put("name2", Collections.singletonMap("key", "val"));
        }
        final var projectId = randomProjectIdOrDefault();
        ClusterState clusterState = ClusterState.builder(new ClusterName("us_cluster"))
            .putProjectMetadata(
                ProjectMetadata.builder(projectId)
                    .putCustom(
                        AutoFollowMetadata.TYPE,
                        new AutoFollowMetadata(existingAutoFollowPatterns, existingAlreadyFollowedIndexUUIDS, existingHeaders)
                    )
            )
            .build();

        Request request = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "name1");
        AutoFollowMetadata result = TransportDeleteAutoFollowPatternAction.innerDelete(request, clusterState.projectState(projectId))
            .getMetadata()
            .getProject(projectId)
            .custom(AutoFollowMetadata.TYPE);
        assertThat(result.getPatterns().size(), equalTo(1));
        assertThat(result.getPatterns().get("name2"), notNullValue());
        assertThat(result.getPatterns().get("name2").getRemoteCluster(), equalTo("asia_cluster"));
        assertThat(result.getFollowedLeaderIndexUUIDs().size(), equalTo(1));
        assertThat(result.getFollowedLeaderIndexUUIDs().get("name2"), notNullValue());
        assertThat(result.getHeaders().size(), equalTo(1));
        assertThat(result.getHeaders().get("name2"), notNullValue());
    }

    public void testInnerDeleteDoesNotExist() {
        Map<String, List<String>> existingAlreadyFollowedIndexUUIDS = new HashMap<>();
        Map<String, AutoFollowPattern> existingAutoFollowPatterns = new HashMap<>();
        Map<String, Map<String, String>> existingHeaders = new HashMap<>();
        {
            List<String> existingPatterns = new ArrayList<>();
            existingPatterns.add("transactions-*");
            existingAutoFollowPatterns.put(
                "name1",
                new AutoFollowPattern(
                    "eu_cluster",
                    existingPatterns,
                    Collections.emptyList(),
                    null,
                    Settings.EMPTY,
                    true,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            );
            existingHeaders.put("key", Collections.singletonMap("key", "val"));
        }
        final var projectId = randomProjectIdOrDefault();
        ClusterState clusterState = ClusterState.builder(new ClusterName("us_cluster"))
            .putProjectMetadata(
                ProjectMetadata.builder(projectId)
                    .putCustom(
                        AutoFollowMetadata.TYPE,
                        new AutoFollowMetadata(existingAutoFollowPatterns, existingAlreadyFollowedIndexUUIDS, existingHeaders)
                    )
            )
            .build();

        Request request = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "name2");
        Exception e = expectThrows(
            ResourceNotFoundException.class,
            () -> TransportDeleteAutoFollowPatternAction.innerDelete(request, clusterState.projectState(projectId))
        );
        assertThat(e.getMessage(), equalTo("auto-follow pattern [name2] is missing"));
    }

    public void testInnerDeleteNoAutoFollowMetadata() {
        final var projectId = randomProjectIdOrDefault();
        ClusterState clusterState = ClusterState.builder(new ClusterName("us_cluster"))
            .putProjectMetadata(ProjectMetadata.builder(projectId))
            .build();

        Request request = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "name1");
        Exception e = expectThrows(
            ResourceNotFoundException.class,
            () -> TransportDeleteAutoFollowPatternAction.innerDelete(request, clusterState.projectState(projectId))
        );
        assertThat(e.getMessage(), equalTo("auto-follow pattern [name1] is missing"));
    }

}
