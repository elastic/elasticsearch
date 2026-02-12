/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.transitions.steps;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ResultDeduplicator;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleErrorStore;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStepContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.datastreams.DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY;
import static org.elasticsearch.datastreams.lifecycle.transitions.steps.CloneStep.formCloneRequest;
import static org.elasticsearch.datastreams.lifecycle.transitions.steps.CloneStep.getDLMCloneIndexName;
import static org.elasticsearch.datastreams.lifecycle.transitions.steps.MarkIndexForDLMForceMergeAction.DLM_INDEX_FOR_FORCE_MERGE_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CloneStepTests extends ESTestCase {

    private CloneStep cloneStep;
    private ProjectId projectId;
    private String indexName;
    private Index index;
    private ThreadPool threadPool;
    private Client client;
    private DataStreamLifecycleErrorStore errorStore;
    private ResultDeduplicator<Tuple<ProjectId, TransportRequest>, Void> deduplicator;
    private AtomicReference<ActionListener<CreateIndexResponse>> capturedCloneListener;
    private AtomicReference<ActionListener<AcknowledgedResponse>> capturedDeleteListener;
    private AtomicReference<ResizeRequest> capturedResizeRequest;
    private AtomicReference<DeleteIndexRequest> capturedDeleteRequest;
    private AtomicReference<MarkIndexForDLMForceMergeAction.Request> capturedMarkRequest;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
        cloneStep = new CloneStep();
        projectId = randomProjectIdOrDefault();
        indexName = randomAlphaOfLength(10);
        index = new Index(indexName, randomAlphaOfLength(10));
        errorStore = new DataStreamLifecycleErrorStore(System::currentTimeMillis);
        deduplicator = new ResultDeduplicator<>(threadPool.getThreadContext());
        capturedCloneListener = new AtomicReference<>();
        capturedDeleteListener = new AtomicReference<>();
        capturedResizeRequest = new AtomicReference<>();
        capturedDeleteRequest = new AtomicReference<>();
        capturedMarkRequest = new AtomicReference<>();

        client = new NoOpClient(threadPool, TestProjectResolvers.usingRequestHeader(threadPool.getThreadContext())) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof ResizeRequest resizeRequest) {
                    capturedResizeRequest.set(resizeRequest);
                    capturedCloneListener.set((ActionListener<CreateIndexResponse>) listener);
                } else if (request instanceof DeleteIndexRequest deleteIndexRequest) {
                    capturedDeleteRequest.set(deleteIndexRequest);
                    capturedDeleteListener.set((ActionListener<AcknowledgedResponse>) listener);
                } else if (request instanceof MarkIndexForDLMForceMergeAction.Request markRequest) {
                    capturedMarkRequest.set(markRequest);
                }
            }
        };
    }

    @After
    public void cleanup() {
        terminate(threadPool);
    }

    public void testStepName() {
        assertThat(cloneStep.stepName(), equalTo("Clone Index"));
    }

    public void testStepNotCompletedWhenNoCloneIndexCallbackExists() {
        ProjectState projectState = createProjectState(indexName, 1, null);
        assertFalse(cloneStep.stepCompleted(index, projectState));
    }

    public void testStepNotCompletedWhenCloneNotMarkedInMetadata() {
        String cloneIndexName = getDLMCloneIndexName(indexName);
        ProjectState projectState = createProjectStateWithClone(indexName, cloneIndexName, null);
        assertFalse(cloneStep.stepCompleted(index, projectState));
    }

    public void testStepCompletedWhenCloneExistsAndMarkedInMetadata() {
        String cloneIndexName = getDLMCloneIndexName(indexName);
        Map<String, String> customMetadata = Map.of(DLM_INDEX_FOR_FORCE_MERGE_KEY, cloneIndexName);
        ProjectState projectState = createProjectStateWithCloneAndRouting(indexName, cloneIndexName, customMetadata, true);
        assertTrue(cloneStep.stepCompleted(index, projectState));
    }

    public void testStepNotCompletedWhenShardsNotActive() {
        String cloneIndexName = getDLMCloneIndexName(indexName);
        Map<String, String> customMetadata = Map.of(DLM_INDEX_FOR_FORCE_MERGE_KEY, cloneIndexName);
        ProjectState projectState = createProjectStateWithCloneAndRouting(indexName, cloneIndexName, customMetadata, false);
        assertFalse(cloneStep.stepCompleted(index, projectState));
    }

    public void testStepCompletedWhenOriginalIndexMarkedWithZeroReplicas() {
        Map<String, String> customMetadata = Map.of(DLM_INDEX_FOR_FORCE_MERGE_KEY, indexName);
        ProjectState projectState = createProjectStateWithRouting(indexName, 0, customMetadata, true);
        assertTrue(cloneStep.stepCompleted(index, projectState));
    }

    public void testExecuteSkipsCloneWhenIndexHasZeroReplicas() {
        ProjectState projectState = createProjectState(indexName, 0, null);
        DlmStepContext stepContext = createStepContext(projectState);

        cloneStep.execute(stepContext);

        assertThat(capturedResizeRequest.get(), is(nullValue()));

        assertThat(capturedMarkRequest.get(), is(notNullValue()));
        assertThat(capturedMarkRequest.get().getSourceIndex(), equalTo(indexName));
        assertThat(capturedMarkRequest.get().getIndexToBeForceMerged(), equalTo(indexName));
        assertThat(capturedMarkRequest.get().getProjectId(), equalTo(projectId));
    }

    public void testExecuteDeletesExistingCloneAndRetriesClone() {
        // Create a clone index that was created more than 12 hours ago (stuck)
        String cloneIndexName = getDLMCloneIndexName(indexName);
        ProjectState projectState = setupStuckCloneScenario(13);
        DlmStepContext stepContext = createStepContext(projectState);

        cloneStep.execute(stepContext);

        // Should issue delete request for stuck clone
        assertThat(capturedDeleteRequest.get(), is(notNullValue()));
        assertThat(capturedDeleteRequest.get().indices()[0], equalTo(cloneIndexName));
    }

    public void testExecuteCreatesCloneWithCorrectSettings() {
        ProjectState projectState = createProjectState(indexName, 1, null);
        DlmStepContext stepContext = createStepContext(projectState);

        cloneStep.execute(stepContext);

        assertThat(capturedResizeRequest.get(), is(notNullValue()));
        assertThat(capturedResizeRequest.get().getSourceIndex(), equalTo(indexName));
        assertThat(capturedResizeRequest.get().getTargetIndexRequest().index(), containsString("dlm-clone-"));
        assertThat(capturedResizeRequest.get().getTargetIndexRequest().settings().get("index.number_of_replicas"), equalTo("0"));
    }

    public void testExecuteWithSuccessfulCloneResponse() {
        ProjectState projectState = createProjectState(indexName, 1, null);
        DlmStepContext stepContext = createStepContext(projectState);

        cloneStep.execute(stepContext);

        assertThat("clone listener should be captured", capturedCloneListener.get(), is(notNullValue()));

        String cloneIndexName = getDLMCloneIndexName(indexName);
        CreateIndexResponse response = new CreateIndexResponse(true, true, cloneIndexName);
        capturedCloneListener.get().onResponse(response);

        assertThat(capturedMarkRequest.get(), is(notNullValue()));
        assertThat(capturedMarkRequest.get().getSourceIndex(), equalTo(indexName));
        assertThat(capturedMarkRequest.get().getIndexToBeForceMerged(), equalTo(cloneIndexName));
        assertThat(capturedMarkRequest.get().getProjectId(), equalTo(projectId));
    }

    public void testExecuteWithFailedCloneResponse() {
        ProjectState projectState = createProjectState(indexName, 1, null);
        DlmStepContext stepContext = createStepContext(projectState);

        cloneStep.execute(stepContext);

        ElasticsearchException exception = new ElasticsearchException("clone failed");
        capturedCloneListener.get().onFailure(exception);

        // Should NOT attempt to delete the clone index since it was never created in metadata
        assertThat(capturedDeleteRequest.get(), is(nullValue()));
    }

    public void testGetCloneIndexCallbackNameIsDeterministic() {
        String cloneName1 = getDLMCloneIndexName(indexName);
        String cloneName2 = getDLMCloneIndexName(indexName);
        assertThat(cloneName1, equalTo(cloneName2));
        assertThat(cloneName1, containsString(indexName));
        assertThat(cloneName1, containsString("dlm-clone-"));
    }

    public void testGetDLMCloneIndexName() {
        // Test with short name - should not be truncated
        String shortName = "test-index";
        String cloneName = getDLMCloneIndexName(shortName);
        assertThat("Clone name should be deterministic", cloneName, equalTo(getDLMCloneIndexName(shortName)));
        assertThat("Clone name should contain prefix", cloneName, containsString("dlm-clone-"));
        assertThat("Clone name should contain original name", cloneName, containsString(shortName));
        int shortNameLength = cloneName.getBytes(StandardCharsets.UTF_8).length;
        assertThat("Clone name should not exceed 255 bytes", shortNameLength <= 255, is(true));

        // Test with maximum length name that doesn't need truncation
        // 255 - 10 (prefix) - 64 (hash) - 1 (separator) = 180 bytes max for original name
        String maxLengthName = randomAlphaOfLength(180);
        String maxCloneName = getDLMCloneIndexName(maxLengthName);
        int maxLength = maxCloneName.getBytes(StandardCharsets.UTF_8).length;
        assertThat("Max length clone name should be exactly 255 bytes", maxLength, is(255));
        assertThat("Max length clone name should contain original name", maxCloneName, containsString(maxLengthName));

        // Test with name that exceeds max and needs truncation
        String longName = randomAlphaOfLength(200);
        String longCloneName = getDLMCloneIndexName(longName);
        int longLength = longCloneName.getBytes(StandardCharsets.UTF_8).length;
        assertThat("Long clone name should be exactly 255 bytes", longLength, is(255));
        assertThat("Long clone name should be deterministic", longCloneName, equalTo(getDLMCloneIndexName(longName)));
        assertThat("Long clone name should start with prefix", longCloneName, containsString("dlm-clone-"));
        // Original name should be truncated
        assertThat("Long clone name should not contain full original name", longCloneName.contains(longName), is(false));

        // Test with multi-byte UTF-8 characters - short enough to not need truncation
        String unicodeName = "index-unicode-αβγ";
        String unicodeCloneName = getDLMCloneIndexName(unicodeName);
        int unicodeLength = unicodeCloneName.getBytes(StandardCharsets.UTF_8).length;
        assertThat("Unicode clone name should not exceed 255 bytes", unicodeLength <= 255, is(true));

        // Test that different names produce different clone names (due to different hashes)
        String name1 = "index-1";
        String name2 = "index-2";
        assertThat(
            "Different names should produce different clone names",
            getDLMCloneIndexName(name1),
            not(equalTo(getDLMCloneIndexName(name2)))
        );
    }

    public void testDeleteCloneSuccessfully() {
        ProjectState projectState = setupStuckCloneScenario(13);
        DlmStepContext stepContext = createStepContext(projectState);

        cloneStep.execute(stepContext);

        // Respond to delete request successfully
        AcknowledgedResponse deleteResponse = AcknowledgedResponse.of(true);
        capturedDeleteListener.get().onResponse(deleteResponse);
    }

    public void testDeleteCloneWithFailure() {
        ProjectState projectState = setupStuckCloneScenario(13);
        DlmStepContext stepContext = createStepContext(projectState);

        cloneStep.execute(stepContext);

        // Respond to delete request with failure
        ElasticsearchException exception = new ElasticsearchException("delete failed");
        capturedDeleteListener.get().onFailure(exception);
    }

    public void testExecuteWaitsWhenCloneIsInProgressAndNotTimedOut() {
        // Create a clone index that was created less than 12 hours ago
        ProjectState projectState = setupStuckCloneScenario(6);
        DlmStepContext stepContext = createStepContext(projectState);
        cloneStep.execute(stepContext);

        // Should NOT issue a delete request since it's still fresh
        assertThat("Should not delete clone that is still within timeout", capturedDeleteRequest.get(), is(nullValue()));
        // Should NOT issue a new clone request
        assertThat("Should not create new clone request while one is in progress", capturedResizeRequest.get(), is(nullValue()));
    }

    public void testExecuteDeletesCloneWhenStuckForOver12Hours() {
        // Create a clone index that was created more than 12 hours ago
        String cloneIndexName = getDLMCloneIndexName(indexName);
        ProjectState projectState = setupStuckCloneScenario(13);
        DlmStepContext stepContext = createStepContext(projectState);
        cloneStep.execute(stepContext);

        // Should issue delete request for stuck clone
        assertThat("Should delete clone that exceeded timeout", capturedDeleteRequest.get(), is(notNullValue()));
        assertThat(capturedDeleteRequest.get().indices()[0], equalTo(cloneIndexName));
    }

    public void testExecuteWaitsWhenCloneExistsButNotInDeduplicatorAndNotTimedOut() {
        // Create a clone index that exists but is not in the deduplicator (completed but step not finished)
        // and was created less than 12 hours ago
        long currentTime = Clock.systemUTC().millis();
        long creationTime = currentTime - TimeValue.timeValueHours(2).millis(); // 2 hours ago

        String cloneIndexName = getDLMCloneIndexName(indexName);
        ProjectState projectState = createProjectStateWithCloneAndCreationTime(indexName, cloneIndexName, null, creationTime);

        DlmStepContext stepContext = createStepContext(projectState);
        cloneStep.execute(stepContext);

        // Should NOT issue a delete since it's not in the deduplicator (might be completing)
        assertThat("Should not delete clone not in deduplicator", capturedDeleteRequest.get(), is(nullValue()));
        // Should NOT issue a new clone request
        assertThat("Should not create new clone while one exists", capturedResizeRequest.get(), is(nullValue()));
    }

    public void testExecuteWaitsWhenCloneExistsOver12HoursButNotInDeduplicator() {
        // Create a clone index that exists, is not in the deduplicator, and was created > 12 hours ago
        // Since it's not in the deduplicator, it's not considered "stuck" - might be completing or already completed
        long currentTime = Clock.systemUTC().millis();
        long creationTime = currentTime - TimeValue.timeValueHours(15).millis(); // 15 hours ago

        String cloneIndexName = getDLMCloneIndexName(indexName);
        ProjectState projectState = createProjectStateWithCloneAndCreationTime(indexName, cloneIndexName, null, creationTime);

        DlmStepContext stepContext = createStepContext(projectState);
        cloneStep.execute(stepContext);

        // Should NOT delete - not in deduplicator means not actively stuck
        assertThat("Should not delete old clone if not in deduplicator", capturedDeleteRequest.get(), is(nullValue()));
    }

    public void testExecuteCreatesNewCloneAfterTimeoutAndCleanup() {
        // Test the full cycle: stuck clone gets deleted, then a new one is created on next run
        String cloneIndexName = getDLMCloneIndexName(indexName);
        ProjectState projectStateWithOldClone = setupStuckCloneScenario(14);
        DlmStepContext stepContext = createStepContext(projectStateWithOldClone);
        cloneStep.execute(stepContext);

        // First run: should delete the old clone
        assertThat(capturedDeleteRequest.get(), is(notNullValue()));
        assertThat(capturedDeleteRequest.get().indices()[0], equalTo(cloneIndexName));

        // Simulate successful delete
        capturedDeleteListener.get().onResponse(AcknowledgedResponse.of(true));

        // Reset captures and clear deduplicator to simulate that the old stuck request is no longer tracked
        capturedDeleteRequest.set(null);
        capturedResizeRequest.set(null);
        deduplicator.clear();

        // Second run: now without the clone index
        ProjectState projectStateWithoutClone = createProjectState(indexName, 1, null);
        DlmStepContext stepContext2 = createStepContext(projectStateWithoutClone);
        cloneStep.execute(stepContext2);

        // Should now create a new clone
        assertThat("Should create new clone after old one was deleted", capturedResizeRequest.get(), is(notNullValue()));
        assertThat(capturedResizeRequest.get().getSourceIndex(), equalTo(indexName));
    }

    private ProjectState createProjectState(String indexName, int numberOfReplicas, Map<String, String> customMetadata) {
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(numberOfReplicas);

        if (customMetadata != null) {
            indexMetadataBuilder.putCustom(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, customMetadata);
        }

        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId).put(indexMetadataBuilder.build(), false);

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(projectMetadataBuilder).build();

        return clusterState.projectState(projectId);
    }

    private ProjectState createProjectStateWithClone(String originalIndexName, String cloneIndexName, Map<String, String> customMetadata) {
        IndexMetadata.Builder originalIndexBuilder = IndexMetadata.builder(originalIndexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(1);

        if (customMetadata != null) {
            originalIndexBuilder.putCustom(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, customMetadata);
        }

        IndexMetadata cloneIndexMetadata = IndexMetadata.builder(cloneIndexName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId)
            .put(originalIndexBuilder.build(), false)
            .put(cloneIndexMetadata, false);

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(projectMetadataBuilder).build();

        return clusterState.projectState(projectId);
    }

    private ProjectState createProjectStateWithCloneAndRouting(
        String originalIndexName,
        String cloneIndexName,
        Map<String, String> customMetadata,
        boolean allShardsActive
    ) {
        IndexMetadata.Builder originalIndexBuilder = IndexMetadata.builder(originalIndexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(1);

        if (customMetadata != null) {
            originalIndexBuilder.putCustom(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, customMetadata);
        }

        IndexMetadata originalIndexMetadata = originalIndexBuilder.build();

        IndexMetadata cloneIndexMetadata = IndexMetadata.builder(cloneIndexName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId)
            .put(originalIndexMetadata, false)
            .put(cloneIndexMetadata, false);

        // Routing should reflect the index that will be force merged (the clone)
        Index cloneIndex = cloneIndexMetadata.getIndex();
        ShardRouting primaryShard = TestShardRouting.newShardRouting(
            new ShardId(cloneIndex, 0),
            "node1",
            true,
            allShardsActive ? ShardRoutingState.STARTED : ShardRoutingState.INITIALIZING
        );

        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(cloneIndex).addShard(primaryShard);

        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTableBuilder).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(projectMetadataBuilder)
            .putRoutingTable(projectId, routingTable)
            .build();

        return clusterState.projectState(projectId);
    }

    private ProjectState createProjectStateWithRouting(
        String indexName,
        int numberOfReplicas,
        Map<String, String> customMetadata,
        boolean allShardsActive
    ) {
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(numberOfReplicas);

        if (customMetadata != null) {
            indexMetadataBuilder.putCustom(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, customMetadata);
        }

        IndexMetadata indexMetadata = indexMetadataBuilder.build();
        Index idx = indexMetadata.getIndex();

        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId).put(indexMetadata, false);

        // Create routing table with shard status
        ShardRouting primaryShard = TestShardRouting.newShardRouting(
            new ShardId(idx, 0),
            "node1",
            true,
            allShardsActive ? ShardRoutingState.STARTED : ShardRoutingState.INITIALIZING
        );

        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(idx).addShard(primaryShard);

        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTableBuilder).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(projectMetadataBuilder)
            .putRoutingTable(projectId, routingTable)
            .build();

        return clusterState.projectState(projectId);
    }

    private DlmStepContext createStepContext(ProjectState projectState) {
        return new DlmStepContext(index, projectState, deduplicator, errorStore, randomIntBetween(1, 10), client);
    }

    private ProjectState createProjectStateWithCloneAndCreationTime(
        String originalIndexName,
        String cloneIndexName,
        Map<String, String> customMetadata,
        long creationTimeMillis
    ) {
        IndexMetadata.Builder originalIndexBuilder = IndexMetadata.builder(originalIndexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(1);

        if (customMetadata != null) {
            originalIndexBuilder.putCustom(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, customMetadata);
        }

        IndexMetadata cloneIndexMetadata = IndexMetadata.builder(cloneIndexName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .creationDate(creationTimeMillis)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId)
            .put(originalIndexBuilder.build(), false)
            .put(cloneIndexMetadata, false);

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(projectMetadataBuilder).build();

        return clusterState.projectState(projectId);
    }

    /**
     * Helper method to create a stuck clone scenario where:
     * - A clone index exists with the specified age in hours
     * - The clone request is registered in the deduplicator (simulating in-progress request)
     *
     * @param hoursAgo Number of hours ago the clone was created
     * @return ProjectState with the stuck clone index
     */
    private ProjectState setupStuckCloneScenario(int hoursAgo) {
        long currentTime = Clock.systemUTC().millis();
        long creationTime = currentTime - TimeValue.timeValueHours(hoursAgo).millis();

        String cloneIndexName = getDLMCloneIndexName(indexName);
        ProjectState projectState = createProjectStateWithCloneAndCreationTime(indexName, cloneIndexName, null, creationTime);

        // Pre-populate the deduplicator to simulate a stuck in-progress request
        ResizeRequest cloneRequest = formCloneRequest(indexName, cloneIndexName);
        deduplicator.executeOnce(
            Tuple.tuple(projectId, cloneRequest),
            ActionListener.noop(),
            (req, listener) -> {} // Don't actually execute, just track as in-progress
        );

        return projectState;
    }
}
