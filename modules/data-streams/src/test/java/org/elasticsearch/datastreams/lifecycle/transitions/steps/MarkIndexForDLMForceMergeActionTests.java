/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.transitions.steps;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.datastreams.DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY;
import static org.elasticsearch.datastreams.lifecycle.transitions.steps.MarkIndexForDLMForceMergeAction.DLM_INDEX_FOR_FORCE_MERGE_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class MarkIndexForDLMForceMergeActionTests extends ESTestCase {

    private void assertValidationError(
        MarkIndexForDLMForceMergeAction.Request request,
        int expectedErrorCount,
        String expectedErrorMessage
    ) {
        ActionRequestValidationException exception = request.validate();
        assertThat(exception, is(notNullValue()));
        assertThat(exception.validationErrors(), hasSize(expectedErrorCount));
        if (expectedErrorMessage != null) {
            assertThat(exception.validationErrors().get(0), containsString(expectedErrorMessage));
        }
    }

    public void testRequestValidation() {
        ProjectId projectId = randomProjectIdOrDefault();
        String sourceIndex = "test-index";
        String targetIndex = "clone-index";

        // Valid request
        MarkIndexForDLMForceMergeAction.Request validRequest = new MarkIndexForDLMForceMergeAction.Request(
            projectId,
            sourceIndex,
            targetIndex
        );
        assertThat(validRequest.validate(), is(nullValue()));

        // Null project ID
        assertValidationError(
            new MarkIndexForDLMForceMergeAction.Request(null, sourceIndex, targetIndex),
            1,
            "project id must not be null"
        );

        // Null source index
        assertValidationError(
            new MarkIndexForDLMForceMergeAction.Request(projectId, null, targetIndex),
            1,
            "source index must not be null or empty"
        );

        // Empty source index
        assertValidationError(
            new MarkIndexForDLMForceMergeAction.Request(projectId, "", targetIndex),
            1,
            "source index must not be null or empty"
        );

        // Null target index
        assertValidationError(
            new MarkIndexForDLMForceMergeAction.Request(projectId, sourceIndex, null),
            1,
            "index to be force merged must not be null or empty"
        );

        // Empty target index
        assertValidationError(
            new MarkIndexForDLMForceMergeAction.Request(projectId, sourceIndex, ""),
            1,
            "index to be force merged must not be null or empty"
        );

        // Multiple validation errors
        assertValidationError(new MarkIndexForDLMForceMergeAction.Request(null, null, null), 3, null);
    }

    public void testUpdateTaskWithMissingIndex() {
        // Test scenario: source index doesn't exist in cluster state
        ProjectId projectId = randomProjectIdOrDefault();
        String sourceIndex = "non-existent-index";
        String targetIndex = "clone-index";

        ClusterState initialState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(projectId))
            .build();

        AtomicReference<AcknowledgedResponse> responseRef = new AtomicReference<>();
        MarkIndexForDLMForceMergeAction.UpdateTask task = new MarkIndexForDLMForceMergeAction.UpdateTask(
            ActionListener.wrap(responseRef::set, e -> fail("Should not fail when index is missing")),
            projectId,
            sourceIndex,
            targetIndex
        );

        ClusterState resultState = task.execute(initialState);

        // When index doesn't exist, cluster state should remain unchanged
        assertThat(resultState, sameInstance(initialState));
    }

    public void testUpdateTaskWithExistingIndex() {
        // Test scenario: mark an index that exists but has no custom metadata yet
        ProjectId projectId = randomProjectIdOrDefault();
        String sourceIndex = "test-index";
        String targetIndex = "clone-index";

        IndexMetadata indexMetadata = IndexMetadata.builder(sourceIndex)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, randomAlphaOfLength(10))
            )
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        ClusterState initialState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(indexMetadata, false))
            .build();

        AtomicReference<AcknowledgedResponse> responseRef = new AtomicReference<>();
        MarkIndexForDLMForceMergeAction.UpdateTask task = new MarkIndexForDLMForceMergeAction.UpdateTask(
            ActionListener.wrap(responseRef::set, e -> fail("Should not fail: " + e.getMessage())),
            projectId,
            sourceIndex,
            targetIndex
        );

        ClusterState resultState = task.execute(initialState);

        // Verify the custom metadata was added
        IndexMetadata updatedIndexMetadata = resultState.metadata().getProject(projectId).index(sourceIndex);
        assertThat(updatedIndexMetadata, is(notNullValue()));
        Map<String, String> customMetadata = updatedIndexMetadata.getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY);
        assertThat(customMetadata, is(notNullValue()));
        assertThat(customMetadata.get(DLM_INDEX_FOR_FORCE_MERGE_KEY), equalTo(targetIndex));
    }

    public void testUpdateTaskMergesExistingCustomMetadata() {
        // Test scenario: index already has some custom metadata, we need to preserve it
        ProjectId projectId = randomProjectIdOrDefault();
        String sourceIndex = "test-index";
        String targetIndex = "clone-index";

        Map<String, String> existingCustomMetadata = new HashMap<>();
        existingCustomMetadata.put("other_key", "other_value");
        existingCustomMetadata.put("another_key", "another_value");

        IndexMetadata indexMetadata = IndexMetadata.builder(sourceIndex)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, randomAlphaOfLength(10))
            )
            .numberOfShards(1)
            .numberOfReplicas(1)
            .putCustom(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, existingCustomMetadata)
            .build();

        ClusterState initialState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(indexMetadata, false))
            .build();

        AtomicReference<AcknowledgedResponse> responseRef = new AtomicReference<>();
        MarkIndexForDLMForceMergeAction.UpdateTask task = new MarkIndexForDLMForceMergeAction.UpdateTask(
            ActionListener.wrap(responseRef::set, e -> fail("Should not fail: " + e.getMessage())),
            projectId,
            sourceIndex,
            targetIndex
        );

        ClusterState resultState = task.execute(initialState);

        // Verify the custom metadata was merged (not replaced)
        IndexMetadata updatedIndexMetadata = resultState.metadata().getProject(projectId).index(sourceIndex);
        assertThat(updatedIndexMetadata, is(notNullValue()));
        Map<String, String> customMetadata = updatedIndexMetadata.getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY);
        assertThat(customMetadata, is(notNullValue()));
        assertThat(customMetadata.get(DLM_INDEX_FOR_FORCE_MERGE_KEY), equalTo(targetIndex));
        assertThat(customMetadata.get("other_key"), equalTo("other_value"));
        assertThat(customMetadata.get("another_key"), equalTo("another_value"));
        assertThat(customMetadata.size(), equalTo(3));
    }

    public void testUpdateTaskIsIdempotent() {
        // Test scenario: running the update twice with the same target index should not modify cluster state on second run
        ProjectId projectId = randomProjectIdOrDefault();
        String sourceIndex = "test-index";
        String targetIndex = "clone-index";

        Map<String, String> existingCustomMetadata = new HashMap<>();
        existingCustomMetadata.put(DLM_INDEX_FOR_FORCE_MERGE_KEY, targetIndex);
        existingCustomMetadata.put("other_key", "other_value");

        IndexMetadata indexMetadata = IndexMetadata.builder(sourceIndex)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, randomAlphaOfLength(10))
            )
            .numberOfShards(1)
            .numberOfReplicas(1)
            .putCustom(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, existingCustomMetadata)
            .build();

        ClusterState initialState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(indexMetadata, false))
            .build();

        AtomicReference<AcknowledgedResponse> responseRef = new AtomicReference<>();
        MarkIndexForDLMForceMergeAction.UpdateTask task = new MarkIndexForDLMForceMergeAction.UpdateTask(
            ActionListener.wrap(responseRef::set, e -> fail("Should not fail: " + e.getMessage())),
            projectId,
            sourceIndex,
            targetIndex
        );

        ClusterState resultState = task.execute(initialState);

        // Since the key already matches, cluster state should remain unchanged
        assertThat(resultState, sameInstance(initialState));
    }

    public void testUpdateTaskOverwritesExistingKey() {
        // Test scenario: index already has DLM_INDEX_FOR_FORCE_MERGE_KEY but with a different value
        ProjectId projectId = randomProjectIdOrDefault();
        String sourceIndex = "test-index";
        String oldTargetIndex = "old-clone-index";
        String newTargetIndex = "new-clone-index";

        Map<String, String> existingCustomMetadata = new HashMap<>();
        existingCustomMetadata.put(DLM_INDEX_FOR_FORCE_MERGE_KEY, oldTargetIndex);
        existingCustomMetadata.put("other_key", "other_value");

        IndexMetadata indexMetadata = IndexMetadata.builder(sourceIndex)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, randomAlphaOfLength(10))
            )
            .numberOfShards(1)
            .numberOfReplicas(1)
            .putCustom(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, existingCustomMetadata)
            .build();

        ClusterState initialState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(indexMetadata, false))
            .build();

        AtomicReference<AcknowledgedResponse> responseRef = new AtomicReference<>();
        MarkIndexForDLMForceMergeAction.UpdateTask task = new MarkIndexForDLMForceMergeAction.UpdateTask(
            ActionListener.wrap(responseRef::set, e -> fail("Should not fail: " + e.getMessage())),
            projectId,
            sourceIndex,
            newTargetIndex
        );

        ClusterState resultState = task.execute(initialState);

        // Verify the key was updated to the new value
        IndexMetadata updatedIndexMetadata = resultState.metadata().getProject(projectId).index(sourceIndex);
        assertThat(updatedIndexMetadata, is(notNullValue()));
        Map<String, String> customMetadata = updatedIndexMetadata.getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY);
        assertThat(customMetadata, is(notNullValue()));
        assertThat(customMetadata.get(DLM_INDEX_FOR_FORCE_MERGE_KEY), equalTo(newTargetIndex));
        assertThat(customMetadata.get("other_key"), equalTo("other_value"));
    }
}
