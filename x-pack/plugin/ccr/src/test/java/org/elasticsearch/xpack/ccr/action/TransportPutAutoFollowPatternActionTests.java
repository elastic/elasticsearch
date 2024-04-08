/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;

public class TransportPutAutoFollowPatternActionTests extends ESTestCase {

    public void testInnerPut() {
        PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
        request.setName("name1");
        request.setRemoteCluster("eu_cluster");
        request.setLeaderIndexPatterns(Collections.singletonList("logs-*"));
        final boolean withExclusionPatterns = randomBoolean();
        if (withExclusionPatterns) {
            request.setLeaderIndexExclusionPatterns(Collections.singletonList("logs-excluded-*"));
        }
        final int numberOfReplicas = randomIntBetween(0, 4);
        request.setSettings(Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), numberOfReplicas).build());

        ClusterState localState = ClusterState.builder(new ClusterName("us_cluster")).metadata(Metadata.builder()).build();

        ClusterState remoteState = ClusterState.builder(new ClusterName("eu_cluster")).metadata(Metadata.builder()).build();

        ClusterState result = TransportPutAutoFollowPatternAction.innerPut(request, null, localState, remoteState);
        AutoFollowMetadata autoFollowMetadata = result.metadata().custom(AutoFollowMetadata.TYPE);
        assertThat(autoFollowMetadata, notNullValue());
        assertThat(autoFollowMetadata.getPatterns().size(), equalTo(1));
        assertThat(autoFollowMetadata.getPatterns().get("name1").getRemoteCluster(), equalTo("eu_cluster"));
        assertThat(autoFollowMetadata.getPatterns().get("name1").getLeaderIndexPatterns().size(), equalTo(1));
        assertThat(autoFollowMetadata.getPatterns().get("name1").getLeaderIndexPatterns().get(0), equalTo("logs-*"));
        final List<String> exclusionPatterns = autoFollowMetadata.getPatterns().get("name1").getLeaderIndexExclusionPatterns();
        if (withExclusionPatterns) {
            assertThat(exclusionPatterns.size(), equalTo(1));
            assertThat(exclusionPatterns.get(0), equalTo("logs-excluded-*"));
        } else {
            assertThat(exclusionPatterns.size(), equalTo(0));
        }
        assertThat(
            autoFollowMetadata.getPatterns().get("name1").getSettings().keySet(),
            hasItem(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey())
        );
        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(autoFollowMetadata.getPatterns().get("name1").getSettings()),
            equalTo(numberOfReplicas)
        );
        assertThat(autoFollowMetadata.getFollowedLeaderIndexUUIDs().size(), equalTo(1));
        assertThat(autoFollowMetadata.getFollowedLeaderIndexUUIDs().get("name1").size(), equalTo(0));
    }

    public void testInnerPut_existingLeaderIndices() {
        PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
        request.setName("name1");
        request.setRemoteCluster("eu_cluster");
        request.setLeaderIndexPatterns(Collections.singletonList("logs-*"));
        boolean withExclusionPatterns = randomBoolean();
        if (withExclusionPatterns) {
            request.setLeaderIndexExclusionPatterns(Collections.singletonList("logs-excluded-*"));
        }

        ClusterState localState = ClusterState.builder(new ClusterName("us_cluster")).metadata(Metadata.builder()).build();

        int numLeaderIndices = randomIntBetween(1, 8);
        int numMatchingLeaderIndices = randomIntBetween(1, 8);
        int numExcludedLeaderIndices = randomIntBetween(1, 8);
        Metadata.Builder mdBuilder = Metadata.builder();
        for (int i = 0; i < numLeaderIndices; i++) {
            mdBuilder.put(
                IndexMetadata.builder("transactions-" + i).settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(0)
            );
        }
        for (int i = 0; i < numMatchingLeaderIndices; i++) {
            mdBuilder.put(
                IndexMetadata.builder("logs-" + i).settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(0)
            );
        }
        for (int i = 0; i < numExcludedLeaderIndices; i++) {
            mdBuilder.put(
                IndexMetadata.builder("logs-excluded-" + i).settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(0)
            );
        }

        ClusterState remoteState = ClusterState.builder(new ClusterName("eu_cluster")).metadata(mdBuilder).build();

        ClusterState result = TransportPutAutoFollowPatternAction.innerPut(request, null, localState, remoteState);
        AutoFollowMetadata autoFollowMetadata = result.metadata().custom(AutoFollowMetadata.TYPE);
        assertThat(autoFollowMetadata, notNullValue());
        assertThat(autoFollowMetadata.getPatterns().size(), equalTo(1));
        assertThat(autoFollowMetadata.getPatterns().get("name1").getRemoteCluster(), equalTo("eu_cluster"));
        assertThat(autoFollowMetadata.getPatterns().get("name1").getLeaderIndexPatterns().size(), equalTo(1));
        assertThat(autoFollowMetadata.getPatterns().get("name1").getLeaderIndexPatterns().get(0), equalTo("logs-*"));
        final List<String> exclusionPatterns = autoFollowMetadata.getPatterns().get("name1").getLeaderIndexExclusionPatterns();
        if (withExclusionPatterns) {
            assertThat(exclusionPatterns.size(), equalTo(1));
            assertThat(exclusionPatterns.get(0), equalTo("logs-excluded-*"));
        } else {
            assertThat(exclusionPatterns.size(), equalTo(0));
        }

        final int expectedAutoFollowIndexCount = withExclusionPatterns
            ? numMatchingLeaderIndices
            : numMatchingLeaderIndices + numExcludedLeaderIndices;
        assertThat(autoFollowMetadata.getFollowedLeaderIndexUUIDs().size(), equalTo(1));
        assertThat(autoFollowMetadata.getFollowedLeaderIndexUUIDs().get("name1").size(), equalTo(expectedAutoFollowIndexCount));
    }

    public void testInnerPut_existingLeaderIndicesAndAutoFollowMetadata() {
        PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
        request.setName("name1");
        request.setRemoteCluster("eu_cluster");
        request.setLeaderIndexPatterns(Arrays.asList("logs-*", "transactions-*"));
        boolean withExclusionPatterns = randomBoolean();
        if (withExclusionPatterns) {
            request.setLeaderIndexExclusionPatterns(Collections.singletonList("logs-excluded-*"));
        }

        Map<String, AutoFollowPattern> existingAutoFollowPatterns = new HashMap<>();
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
        Map<String, List<String>> existingAlreadyFollowedIndexUUIDS = new HashMap<>();
        List<String> existingUUIDS = new ArrayList<>();
        existingUUIDS.add("_val");
        existingAlreadyFollowedIndexUUIDS.put("name1", existingUUIDS);
        Map<String, Map<String, String>> existingHeaders = new HashMap<>();
        existingHeaders.put("name1", Collections.singletonMap("key", "val"));

        ClusterState localState = ClusterState.builder(new ClusterName("us_cluster"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        AutoFollowMetadata.TYPE,
                        new AutoFollowMetadata(existingAutoFollowPatterns, existingAlreadyFollowedIndexUUIDS, existingHeaders)
                    )
            )
            .build();

        int numLeaderIndices = randomIntBetween(1, 8);
        Metadata.Builder mdBuilder = Metadata.builder();
        for (int i = 0; i < numLeaderIndices; i++) {
            mdBuilder.put(
                IndexMetadata.builder("logs-" + i).settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(0)
            );
        }
        int numExcludedLeaderIndices = randomIntBetween(1, 8);
        for (int i = 0; i < numExcludedLeaderIndices; i++) {
            mdBuilder.put(
                IndexMetadata.builder("logs-excluded-" + i).settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(0)
            );
        }

        ClusterState remoteState = ClusterState.builder(new ClusterName("eu_cluster")).metadata(mdBuilder).build();

        ClusterState result = TransportPutAutoFollowPatternAction.innerPut(request, null, localState, remoteState);
        AutoFollowMetadata autoFollowMetadata = result.metadata().custom(AutoFollowMetadata.TYPE);
        assertThat(autoFollowMetadata, notNullValue());
        assertThat(autoFollowMetadata.getPatterns().size(), equalTo(1));
        assertThat(autoFollowMetadata.getPatterns().get("name1").getRemoteCluster(), equalTo("eu_cluster"));
        assertThat(autoFollowMetadata.getPatterns().get("name1").getLeaderIndexPatterns().size(), equalTo(2));
        assertThat(autoFollowMetadata.getPatterns().get("name1").getLeaderIndexPatterns().get(0), equalTo("logs-*"));
        assertThat(autoFollowMetadata.getPatterns().get("name1").getLeaderIndexPatterns().get(1), equalTo("transactions-*"));
        final List<String> exclusionPatterns = autoFollowMetadata.getPatterns().get("name1").getLeaderIndexExclusionPatterns();
        if (withExclusionPatterns) {
            assertThat(exclusionPatterns.size(), equalTo(1));
            assertThat(exclusionPatterns.get(0), equalTo("logs-excluded-*"));
        } else {
            assertThat(exclusionPatterns.size(), equalTo(0));
        }
        final int expectedAutoFollowIndexCount = withExclusionPatterns ? numLeaderIndices : numLeaderIndices + numExcludedLeaderIndices;
        assertThat(autoFollowMetadata.getFollowedLeaderIndexUUIDs().size(), equalTo(1));
        assertThat(autoFollowMetadata.getFollowedLeaderIndexUUIDs().get("name1").size(), equalTo(expectedAutoFollowIndexCount + 1));
        assertThat(autoFollowMetadata.getHeaders().size(), equalTo(1));
        assertThat(autoFollowMetadata.getHeaders().get("name1"), notNullValue());
    }
}
