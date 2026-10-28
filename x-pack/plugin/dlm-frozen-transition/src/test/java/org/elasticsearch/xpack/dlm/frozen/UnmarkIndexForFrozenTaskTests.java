/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class UnmarkIndexForFrozenTaskTests extends ESTestCase {
    final ProjectId PROJECT_ID = randomProjectIdOrDefault();

    public void testUnmark() {
        IndexMetadata vanilla = IndexMetadata.builder(DataStream.getDefaultBackingIndexName("vanilla", 1))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .creationDate(randomNonNegativeLong())
            .build();

        ClusterState state;
        state = unmarkAndCheck(vanilla);
        assertThat(getCustomData(state, vanilla.getIndex().getName()), equalTo(Map.of()));

        IndexMetadata onlyCustom = IndexMetadata.builder(DataStream.getDefaultBackingIndexName("withCustom", 1))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .creationDate(randomNonNegativeLong())
            .putCustom(DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, Map.of("other", "thing"))
            .build();

        state = unmarkAndCheck(onlyCustom);
        assertThat(getCustomData(state, onlyCustom.getIndex().getName()), equalTo(Map.of("other", "thing")));

        IndexMetadata markedIndex = IndexMetadata.builder(DataStream.getDefaultBackingIndexName("myds", 1))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .creationDate(randomNonNegativeLong())
            .putCustom(
                DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY,
                Map.of(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY, "repo")
            )
            .build();

        state = unmarkAndCheck(markedIndex);
        assertThat(getCustomData(state, markedIndex.getIndex().getName()), equalTo(Map.of()));

        IndexMetadata withOtherCustom = IndexMetadata.builder(DataStream.getDefaultBackingIndexName("withCustom", 1))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .creationDate(randomNonNegativeLong())
            .putCustom(
                DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY,
                Map.of(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY, "repo", "other", "thing")
            )
            .build();

        state = unmarkAndCheck(withOtherCustom);
        assertThat(getCustomData(state, withOtherCustom.getIndex().getName()), equalTo(Map.of("other", "thing")));

        UnmarkIndexForFrozenTask task = new UnmarkIndexForFrozenTask(PROJECT_ID, "missing-index", ActionListener.noop());
        var newState = task.execute(state);
        assertThat(newState, sameInstance(state));
    }

    private ClusterState unmarkAndCheck(IndexMetadata indexMetadata) {
        ProjectMetadata projectMetadata = ProjectMetadata.builder(PROJECT_ID).put(indexMetadata, true).build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(projectMetadata).build();
        UnmarkIndexForFrozenTask task = new UnmarkIndexForFrozenTask(PROJECT_ID, indexMetadata.getIndex().getName(), ActionListener.noop());

        ClusterState newState = task.execute(state);
        assertIndexUnmarked(newState, indexMetadata.getIndex().getName());
        var anotherState = task.execute(newState);
        // Check that doing it twice is a no-op
        assertThat(anotherState, sameInstance(newState));
        return newState;
    }

    private void assertIndexUnmarked(ClusterState state, String indexName) {
        assertTrue(
            "expected " + indexName + " to be unmarked as ready for frozen, but its custom data was: " + getCustomData(state, indexName),
            Optional.ofNullable(state.metadata().getProject(PROJECT_ID).index(indexName))
                .map(im -> DataStreamLifecycleService.indexMarkedForFrozen(im) == false)
                .orElse(false)
        );
    }

    private Map<String, String> getCustomData(ClusterState state, String indexName) {
        return Optional.ofNullable(state.metadata().getProject(PROJECT_ID).index(indexName))
            .map(im -> im.getCustomData(DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY))
            .orElse(Map.of());
    }
}
