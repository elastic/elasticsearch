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
import org.elasticsearch.cluster.metadata.DataStream;
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
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.datastreams.DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY;
import static org.elasticsearch.datastreams.lifecycle.transitions.steps.CloneStep.CLONE_INDEX_PREFIX;
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
    private Clock fixedClock;

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
        fixedClock = Clock.fixed(Instant.parse("2026-02-24T12:00:00Z"), Clock.systemDefaultZone().getZone());
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

    public void testStepNotCompletedWhenNoCloneIndexExists() {
        ProjectState projectState = projectStateBuilder().build();
        assertFalse(cloneStep.stepCompleted(index, projectState));
    }

    public void testStepNotCompletedWhenCloneNotMarkedInMetadata() {
        ProjectState projectState = projectStateBuilder().withClone().build();
        assertFalse(cloneStep.stepCompleted(index, projectState));
    }

    public void testStepCompletedWhenCloneExistsAndMarkedInMetadata() {
        String cloneIndexName = getDLMCloneIndexName(indexName);
        Map<String, String> customMetadata = Map.of(DLM_INDEX_FOR_FORCE_MERGE_KEY, cloneIndexName);
        ProjectState projectState = projectStateBuilder().withClone().withCustomMetadata(customMetadata).withRouting().build();
        assertTrue(cloneStep.stepCompleted(index, projectState));
    }

    public void testStepNotCompletedWhenShardsNotActive() {
        String cloneIndexName = getDLMCloneIndexName(indexName);
        Map<String, String> customMetadata = Map.of(DLM_INDEX_FOR_FORCE_MERGE_KEY, cloneIndexName);
        ProjectState projectState = projectStateBuilder().withClone().withCustomMetadata(customMetadata).withRouting(false).build();
        assertFalse(cloneStep.stepCompleted(index, projectState));
    }

    public void testStepCompletedWhenOriginalIndexMarkedWithZeroReplicas() {
        Map<String, String> customMetadata = Map.of(DLM_INDEX_FOR_FORCE_MERGE_KEY, indexName);
        ProjectState projectState = projectStateBuilder().withReplicas(0).withCustomMetadata(customMetadata).withRouting().build();
        assertTrue(cloneStep.stepCompleted(index, projectState));
    }

    public void testExecuteSkipsCloneWhenIndexHasZeroReplicas() {
        ProjectState projectState = projectStateBuilder().withReplicas(0).build();
        DlmStepContext stepContext = createStepContext(projectState);

        cloneStep.execute(stepContext);

        assertThat(capturedResizeRequest.get(), is(nullValue()));

        assertThat(capturedMarkRequest.get(), is(notNullValue()));
        assertThat(capturedMarkRequest.get().getOriginalIndex(), equalTo(indexName));
        assertThat(capturedMarkRequest.get().getIndexToBeForceMerged(), equalTo(indexName));
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
        ProjectState projectState = projectStateBuilder().build();
        DlmStepContext stepContext = createStepContext(projectState);

        cloneStep.execute(stepContext);

        assertThat(capturedResizeRequest.get(), is(notNullValue()));
        assertThat(capturedResizeRequest.get().getSourceIndex(), equalTo(indexName));
        assertThat(capturedResizeRequest.get().getTargetIndexRequest().index(), containsString("dlm-clone-"));
        assertThat(capturedResizeRequest.get().getTargetIndexRequest().settings().get("index.number_of_replicas"), equalTo("0"));
    }

    public void testExecuteWithSuccessfulCloneResponse() {
        ProjectState projectState = projectStateBuilder().build();
        DlmStepContext stepContext = createStepContext(projectState);

        cloneStep.execute(stepContext);

        assertThat("clone listener should be captured", capturedCloneListener.get(), is(notNullValue()));

        String cloneIndexName = getDLMCloneIndexName(indexName);
        CreateIndexResponse response = new CreateIndexResponse(true, true, cloneIndexName);
        capturedCloneListener.get().onResponse(response);

        assertThat(capturedMarkRequest.get(), is(notNullValue()));
        assertThat(capturedMarkRequest.get().getOriginalIndex(), equalTo(indexName));
        assertThat(capturedMarkRequest.get().getIndexToBeForceMerged(), equalTo(cloneIndexName));
    }

    public void testExecuteWithFailedCloneResponse() {
        ProjectState projectState = projectStateBuilder().build();
        DlmStepContext stepContext = createStepContext(projectState);

        cloneStep.execute(stepContext);

        ElasticsearchException exception = new ElasticsearchException("clone failed");
        capturedCloneListener.get().onFailure(exception);

        // Should NOT attempt to delete the clone index since it was never created in metadata
        assertThat(capturedDeleteRequest.get(), is(nullValue()));
    }

    public void testGetDLMCloneIndexName() {
        String name = "test-index";
        String cloneName = getDLMCloneIndexName(name);
        assertThat("Clone name should be deterministic", cloneName, equalTo(getDLMCloneIndexName(name)));
        assertThat("Clone name should contain prefix", cloneName, containsString(CLONE_INDEX_PREFIX));
        int shortNameLength = cloneName.getBytes(StandardCharsets.UTF_8).length;
        assertThat("Clone name should not exceed 255 bytes", shortNameLength <= 255, is(true));

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
        long creationTime = fixedClock.millis() - TimeValue.timeValueHours(2).millis(); // 2 hours ago
        ProjectState projectState = projectStateBuilder().withClone().withCloneCreationTime(creationTime).build();
        DlmStepContext stepContext = createStepContext(projectState);
        cloneStep.execute(stepContext);
        // Should NOT issue a delete since it's not in the deduplicator (might be completing)
        assertThat("Should not delete clone not in deduplicator", capturedDeleteRequest.get(), is(nullValue()));
        // Should NOT issue a new clone request
        assertThat("Should not create new clone while one exists", capturedResizeRequest.get(), is(nullValue()));
    }

    public void testExecuteWaitsWhenCloneExistsOver12HoursButNotInDeduplicator() {
        // Create a clone index that exists, is not in the deduplicator, and was created > 12 hours ago
        // Since it's not in the deduplicator, but shards are not active, it should be deleted
        long creationTime = fixedClock.millis() - TimeValue.timeValueHours(15).millis(); // 15 hours ago
        ProjectState projectState = projectStateBuilder().withClone()
            .withCloneCreationTime(creationTime)
            .withCloneShardsNotActive()
            .build();
        DlmStepContext stepContext = createStepContext(projectState);
        cloneStep.execute(stepContext);
        // Should delete - not in deduplicator but shards are not active and clone is old
        assertThat(
            "Should delete old clone if not in deduplicator and shards are not active",
            capturedDeleteRequest.get(),
            is(notNullValue())
        );
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
        ProjectState projectStateWithoutClone = projectStateBuilder().build();
        DlmStepContext stepContext2 = createStepContext(projectStateWithoutClone);
        cloneStep.execute(stepContext2);

        // Should now create a new clone
        assertThat("Should create new clone after old one was deleted", capturedResizeRequest.get(), is(notNullValue()));
        assertThat(capturedResizeRequest.get().getSourceIndex(), equalTo(indexName));
    }

    public void testCloneFailureWithGenericExceptionRecordsError() {
        ProjectState projectState = projectStateBuilder().build();
        DlmStepContext stepContext = createStepContext(projectState);

        cloneStep.execute(stepContext);

        // Simulate clone failure with a generic exception
        assertThat(capturedCloneListener.get(), is(notNullValue()));
        ElasticsearchException cloneFailure = new ElasticsearchException("clone operation failed");
        capturedCloneListener.get().onFailure(cloneFailure);

        // Error should be recorded
        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(Objects.requireNonNull(errorStore.getError(projectId, indexName)).error(), containsString("clone operation failed"));
    }

    public void testStuckCloneCleanupFailureRecordsError() {
        ProjectState projectState = setupStuckCloneScenario(13);
        DlmStepContext stepContext = createStepContext(projectState);

        cloneStep.execute(stepContext);

        // Simulate failed cleanup of stuck clone
        assertThat(capturedDeleteListener.get(), is(notNullValue()));
        ElasticsearchException cleanupFailure = new ElasticsearchException("cleanup failed");
        capturedDeleteListener.get().onFailure(cleanupFailure);

        // Error should be recorded
        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(Objects.requireNonNull(errorStore.getError(projectId, indexName)).error(), containsString("cleanup failed"));
    }

    /**
     * Builder for creating ProjectState with various configurations for testing.
     */
    private class ProjectStateBuilder {
        private int numberOfReplicas = 1;
        private Map<String, String> customMetadata = null;
        private String cloneIndexName = null;
        private Long cloneCreationTime = null;
        private boolean withRouting = false;
        private boolean allShardsActive = true;

        ProjectStateBuilder withReplicas(int numberOfReplicas) {
            this.numberOfReplicas = numberOfReplicas;
            return this;
        }

        ProjectStateBuilder withCustomMetadata(Map<String, String> customMetadata) {
            this.customMetadata = customMetadata;
            return this;
        }

        ProjectStateBuilder withClone() {
            this.cloneIndexName = getDLMCloneIndexName(indexName);
            return this;
        }

        ProjectStateBuilder withClone(String cloneName) {
            this.cloneIndexName = cloneName;
            return this;
        }

        ProjectStateBuilder withCloneCreationTime(long creationTimeMillis) {
            this.cloneCreationTime = creationTimeMillis;
            return this;
        }

        ProjectStateBuilder withRouting() {
            this.withRouting = true;
            return this;
        }

        ProjectStateBuilder withRouting(boolean allShardsActive) {
            this.withRouting = true;
            this.allShardsActive = allShardsActive;
            return this;
        }

        ProjectStateBuilder withCloneShardsNotActive() {
            this.allShardsActive = false;
            this.withRouting = true;
            return this;
        }

        ProjectState build() {
            // Build original index metadata
            IndexMetadata.Builder originalIndexBuilder = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                        .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                        .build()
                )
                .numberOfShards(1)
                .numberOfReplicas(numberOfReplicas);

            if (customMetadata != null) {
                originalIndexBuilder.putCustom(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, customMetadata);
            }

            IndexMetadata originalIndexMetadata = originalIndexBuilder.build();
            ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId).put(originalIndexMetadata, false);

            // Build clone index metadata if requested
            IndexMetadata cloneIndexMetadata = null;
            if (cloneIndexName != null) {
                IndexMetadata.Builder cloneBuilder = IndexMetadata.builder(cloneIndexName)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
                    .numberOfShards(1)
                    .numberOfReplicas(0);

                if (cloneCreationTime != null) {
                    cloneBuilder.creationDate(cloneCreationTime);
                }

                cloneIndexMetadata = cloneBuilder.build();
                projectMetadataBuilder.put(cloneIndexMetadata, false);

                DataStream dataStream = DataStream.builder(
                    "test-datastream-" + indexName,
                    java.util.List.of(cloneIndexMetadata.getIndex(), originalIndexMetadata.getIndex())
                ).setGeneration(2).build();
                projectMetadataBuilder.put(dataStream);
            }

            // Build routing table if requested
            RoutingTable routingTable = null;
            if (withRouting) {
                // Route to the clone if it exists, otherwise to the original
                Index indexToRoute = cloneIndexMetadata != null ? cloneIndexMetadata.getIndex() : originalIndexMetadata.getIndex();
                ShardRouting primaryShard = TestShardRouting.newShardRouting(
                    new ShardId(indexToRoute, 0),
                    "node1",
                    true,
                    allShardsActive ? ShardRoutingState.STARTED : ShardRoutingState.INITIALIZING
                );
                IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexToRoute).addShard(primaryShard);
                routingTable = RoutingTable.builder().add(indexRoutingTableBuilder).build();
            }

            // Build final ProjectState
            ClusterState.Builder clusterStateBuilder = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(projectMetadataBuilder);
            if (routingTable != null) {
                clusterStateBuilder.putRoutingTable(projectId, routingTable);
            }

            return clusterStateBuilder.build().projectState(projectId);
        }
    }

    private ProjectStateBuilder projectStateBuilder() {
        return new ProjectStateBuilder();
    }

    private DlmStepContext createStepContext(ProjectState projectState) {
        return new DlmStepContext(index, projectState, deduplicator, errorStore, randomIntBetween(1, 10), client, fixedClock);
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
        long creationTime = fixedClock.millis() - TimeValue.timeValueHours(hoursAgo).millis();
        String cloneIndexName = getDLMCloneIndexName(indexName);
        ProjectState projectState = projectStateBuilder().withClone().withCloneCreationTime(creationTime).build();
        ResizeRequest cloneRequest = formCloneRequest(indexName, cloneIndexName);
        deduplicator.executeOnce(Tuple.tuple(projectId, cloneRequest), ActionListener.noop(), (req, listener) -> {});

        return projectState;
    }
}
