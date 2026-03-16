/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class MarkIndicesForFrozenTaskTests extends ESTestCase {
    final ProjectId PROJECT_ID = randomProjectIdOrDefault();
    final String REPO = randomAlphaOfLength(10);

    public void testMarkIndicesForFrozen() throws Exception {
        IndexMetadata vanilla = IndexMetadata.builder(DataStream.getDefaultBackingIndexName("vanilla", 1))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .creationDate(randomNonNegativeLong())
            .build();
        IndexMetadata withOtherCustom = IndexMetadata.builder(DataStream.getDefaultBackingIndexName("withCustom", 1))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .creationDate(randomNonNegativeLong())
            .putCustom(DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, Map.of("other", "thing"))
            .build();
        IndexMetadata alreadyMarkedDifferentRepo = IndexMetadata.builder(DataStream.getDefaultBackingIndexName("diffRepo", 1))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .creationDate(randomNonNegativeLong())
            .putCustom(
                DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY,
                Map.of(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY, "other-repo")
            )
            .build();
        IndexMetadata alreadyMarkedSameRepo = IndexMetadata.builder(DataStream.getDefaultBackingIndexName("alreadyMarked", 1))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .creationDate(randomNonNegativeLong())
            .putCustom(
                DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY,
                Map.of(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY, REPO)
            )
            .build();
        Set<Index> allIndices = Set.of(
            vanilla.getIndex(),
            withOtherCustom.getIndex(),
            alreadyMarkedDifferentRepo.getIndex(),
            alreadyMarkedSameRepo.getIndex()
        );

        ProjectMetadata projectMetadata = ProjectMetadata.builder(PROJECT_ID)
            .put(vanilla, true)
            .put(withOtherCustom, true)
            .put(alreadyMarkedDifferentRepo, true)
            .put(alreadyMarkedSameRepo, true)
            .build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(projectMetadata).build();
        MarkIndicesForFrozenTask task = new MarkIndicesForFrozenTask(PROJECT_ID, allIndices, ActionListener.noop());

        ClusterState newState = task.execute(state);
        // No change, there is no default repo
        assertThat(newState, sameInstance(state));

        // Add the repo to the settings
        state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .persistentSettings(Settings.builder().put(RepositoriesService.DEFAULT_REPOSITORY_SETTING.getKey(), REPO).build())
                    .build()
            )
            .putProjectMetadata(projectMetadata)
            .build();

        newState = task.execute(state);
        assertIndexMarked(newState, vanilla.getIndex().getName());
        assertIndexMarked(newState, withOtherCustom.getIndex().getName());
        assertIndexMarked(newState, alreadyMarkedDifferentRepo.getIndex().getName());
        assertIndexMarked(newState, alreadyMarkedSameRepo.getIndex().getName());

        assertThat(
            getCustomData(newState, withOtherCustom.getIndex().getName()),
            equalTo(Map.of("other", "thing", DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY, REPO))
        );
    }

    private void assertIndexMarked(ClusterState state, String indexName) {
        assertTrue(
            "expected " + indexName + " to be marked as ready for frozen, but its custom data was: " + getCustomData(state, indexName),
            Optional.ofNullable(state.metadata().getProject(PROJECT_ID).index(indexName))
                .map(DataStreamLifecycleService::indexMarkedForFrozen)
                .orElse(false)
        );
        assertThat(
            "expected " + indexName + " to have " + REPO + " as its repository, but it has: " + getCustomData(state, indexName),
            Optional.ofNullable(state.metadata().getProject(PROJECT_ID).index(indexName))
                .map(im -> im.getCustomData(DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY))
                .map(custom -> custom.get(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY))
                .orElse("_unset_"),
            equalTo(REPO)
        );
    }

    private Map<String, String> getCustomData(ClusterState state, String indexName) {
        return Optional.ofNullable(state.metadata().getProject(PROJECT_ID).index(indexName))
            .map(im -> im.getCustomData(DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY))
            .orElse(Map.of());
    }
}
