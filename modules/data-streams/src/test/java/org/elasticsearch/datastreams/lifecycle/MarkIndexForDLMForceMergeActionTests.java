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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.lifecycle.transitions.steps.MarkIndexForDLMForceMergeAction;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.datastreams.DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY;
import static org.elasticsearch.datastreams.lifecycle.transitions.steps.MarkIndexForDLMForceMergeAction.DLM_INDEX_FOR_FORCE_MERGE_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class MarkIndexForDLMForceMergeActionTests extends ESTestCase {

    /**
     * Helper method to create IndexMetadata with standard settings for tests
     */
    private IndexMetadata createIndexMetadata(String indexName) {
        return IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, randomAlphaOfLength(10))
            )
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
    }

    /**
     * Helper method to create IndexMetadata with custom metadata
     */
    private IndexMetadata createIndexMetadata(String indexName, Map<String, String> customMetadata) {
        return IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, randomAlphaOfLength(10))
            )
            .numberOfShards(1)
            .numberOfReplicas(1)
            .putCustom(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, customMetadata)
            .build();
    }

    /**
     * Helper method to create ClusterState with source and target indices
     */
    private ClusterState createClusterState(ProjectId projectId, IndexMetadata sourceIndex, IndexMetadata targetIndex) {
        return ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(sourceIndex, false).put(targetIndex, false))
            .build();
    }

    public void testRequestValidation() {
        String sourceIndex = "test-index";
        String targetIndex = "clone-index";

        // Valid request
        MarkIndexForDLMForceMergeAction.Request validRequest = new MarkIndexForDLMForceMergeAction.Request(sourceIndex, targetIndex);
        assertThat(validRequest.validate(), is(nullValue()));

        // Null source index
        IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> new MarkIndexForDLMForceMergeAction.Request(null, targetIndex)
        );
        assertThat(e1.getMessage(), containsString("originalIndex must have text"));

        // Empty source index
        IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> new MarkIndexForDLMForceMergeAction.Request("", targetIndex)
        );
        assertThat(e2.getMessage(), containsString("originalIndex must have text"));

        // Whitespace source index
        IllegalArgumentException e3 = expectThrows(
            IllegalArgumentException.class,
            () -> new MarkIndexForDLMForceMergeAction.Request(" ", targetIndex)
        );
        assertThat(e3.getMessage(), containsString("originalIndex must have text"));

        // Null target index
        IllegalArgumentException e4 = expectThrows(
            IllegalArgumentException.class,
            () -> new MarkIndexForDLMForceMergeAction.Request(sourceIndex, null)
        );
        assertThat(e4.getMessage(), containsString("indexToBeForceMerged must have text"));

        // Empty target index
        IllegalArgumentException e5 = expectThrows(
            IllegalArgumentException.class,
            () -> new MarkIndexForDLMForceMergeAction.Request(sourceIndex, "")
        );
        assertThat(e5.getMessage(), containsString("indexToBeForceMerged must have text"));

        // Whitespace target index
        IllegalArgumentException e6 = expectThrows(
            IllegalArgumentException.class,
            () -> new MarkIndexForDLMForceMergeAction.Request(sourceIndex, " ")
        );
        assertThat(e6.getMessage(), containsString("indexToBeForceMerged must have text"));
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
        MarkIndexForDlmForceMergeTask task = new MarkIndexForDlmForceMergeTask(
            ActionListener.wrap(responseRef::set, e -> fail("Should not fail when index is missing")),
            projectId,
            new MarkIndexForDLMForceMergeAction.Request(sourceIndex, targetIndex)
        );

        ClusterState resultState = task.execute(initialState);
        // Since the index is missing, cluster state should remain unchanged
        assertThat(resultState, sameInstance(initialState));
    }

    public void testUpdateTaskWithExistingIndex() {
        // Test scenario: mark an index that exists but has no custom metadata yet
        ProjectId projectId = randomProjectIdOrDefault();
        String sourceIndex = "test-index";
        String targetIndex = "clone-index";

        IndexMetadata indexMetadata = createIndexMetadata(sourceIndex);
        IndexMetadata cloneIndexMetadata = createIndexMetadata(targetIndex);
        ClusterState initialState = createClusterState(projectId, indexMetadata, cloneIndexMetadata);

        AtomicReference<AcknowledgedResponse> responseRef = new AtomicReference<>();
        MarkIndexForDlmForceMergeTask task = new MarkIndexForDlmForceMergeTask(
            ActionListener.wrap(responseRef::set, e -> fail("Should not fail: " + e.getMessage())),
            projectId,
            new MarkIndexForDLMForceMergeAction.Request(sourceIndex, targetIndex)
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

        IndexMetadata indexMetadata = createIndexMetadata(sourceIndex, existingCustomMetadata);
        IndexMetadata cloneIndexMetadata = createIndexMetadata(targetIndex);
        ClusterState initialState = createClusterState(projectId, indexMetadata, cloneIndexMetadata);

        AtomicReference<AcknowledgedResponse> responseRef = new AtomicReference<>();
        MarkIndexForDlmForceMergeTask task = new MarkIndexForDlmForceMergeTask(
            ActionListener.wrap(responseRef::set, e -> fail("Should not fail: " + e.getMessage())),
            projectId,
            new MarkIndexForDLMForceMergeAction.Request(sourceIndex, targetIndex)
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

        IndexMetadata indexMetadata = createIndexMetadata(sourceIndex, existingCustomMetadata);
        IndexMetadata cloneIndexMetadata = createIndexMetadata(targetIndex);
        ClusterState initialState = createClusterState(projectId, indexMetadata, cloneIndexMetadata);

        AtomicReference<AcknowledgedResponse> responseRef = new AtomicReference<>();
        MarkIndexForDlmForceMergeTask task = new MarkIndexForDlmForceMergeTask(
            ActionListener.wrap(responseRef::set, e -> fail("Should not fail: " + e.getMessage())),
            projectId,
            new MarkIndexForDLMForceMergeAction.Request(sourceIndex, targetIndex)
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

        IndexMetadata indexMetadata = createIndexMetadata(sourceIndex, existingCustomMetadata);
        IndexMetadata newCloneIndexMetadata = createIndexMetadata(newTargetIndex);
        ClusterState initialState = createClusterState(projectId, indexMetadata, newCloneIndexMetadata);

        AtomicReference<AcknowledgedResponse> responseRef = new AtomicReference<>();
        MarkIndexForDlmForceMergeTask task = new MarkIndexForDlmForceMergeTask(
            ActionListener.wrap(responseRef::set, e -> fail("Should not fail: " + e.getMessage())),
            projectId,
            new MarkIndexForDLMForceMergeAction.Request(sourceIndex, newTargetIndex)
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
