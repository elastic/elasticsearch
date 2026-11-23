/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class MarkIndicesForFrozenTaskTests extends ESTestCase {
    final ProjectId PROJECT_ID = randomProjectIdOrDefault();
    final String REPO = randomAlphaOfLength(10);

    @TestLogging(value = "org.elasticsearch.datastreams.lifecycle.MarkIndicesForFrozenTask:INFO", reason = "asserts the success log")
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

        // A cluster state value, which can change after the task is queued, takes precedence over the node-level value.
        // An empty value means no default repository.
        String effectiveRepository = randomFrom("", REPO);
        boolean repositoryInClusterState = randomBoolean();
        String nodeRepository = repositoryInClusterState
            ? randomFrom("", randomValueOtherThan(REPO, () -> randomAlphaOfLength(10)))
            : effectiveRepository;
        Metadata.Builder metadata = Metadata.builder().put(projectMetadata);
        if (repositoryInClusterState) {
            metadata.persistentSettings(
                Settings.builder().put(RepositoriesService.DEFAULT_REPOSITORY_SETTING.getKey(), effectiveRepository).build()
            );
        }
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        MarkIndicesForFrozenTask task = new MarkIndicesForFrozenTask(PROJECT_ID, allIndices, nodeRepository, ActionListener.noop());

        ClusterState newState;
        try (var mockLog = MockLog.capture(MarkIndicesForFrozenTask.class)) {
            String loggerName = MarkIndicesForFrozenTask.class.getName();
            mockLog.addExpectation(
                effectiveRepository.isEmpty()
                    ? new MockLog.UnseenEventExpectation("no success log", loggerName, Level.INFO, "DLM successfully marked*")
                    : new MockLog.SeenEventExpectation(
                        "success log lists only newly marked indices",
                        loggerName,
                        Level.INFO,
                        "DLM successfully marked 3 indices as ready to be frozen: *"
                    )
            );
            newState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
                state,
                new DataStreamLifecycleService.MarkIndicesForFrozenExecutor(),
                List.of(task)
            );
            mockLog.assertAllExpectationsMatched();
        }
        if (effectiveRepository.isEmpty()) {
            assertThat(newState, sameInstance(state));
        } else {
            assertIndexMarked(newState, vanilla.getIndex().getName());
            assertIndexMarked(newState, withOtherCustom.getIndex().getName());
            assertIndexMarked(newState, alreadyMarkedDifferentRepo.getIndex().getName());
            assertIndexMarked(newState, alreadyMarkedSameRepo.getIndex().getName());
            assertThat(
                getCustomData(newState, withOtherCustom.getIndex().getName()),
                equalTo(Map.of("other", "thing", DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY, REPO))
            );
        }
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
