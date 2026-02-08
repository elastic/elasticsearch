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
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.Settings;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.datastreams.DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
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
    private AtomicReference<MarkIndexToBeForceMergedAction.Request> capturedMarkRequest;

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
                } else if (request instanceof MarkIndexToBeForceMergedAction.Request markRequest) {
                    capturedMarkRequest.set(markRequest);
                }
            }
        };
    }

    @After
    public void cleanup() {
        threadPool.shutdownNow();
    }

    public void testStepName() {
        assertThat(cloneStep.stepName(), equalTo("Clone Index"));
    }

    public void testStepNotCompletedWhenNoCloneIndexExists() {
        ProjectState projectState = createProjectState(indexName, 1, null);
        assertFalse(cloneStep.stepCompleted(index, projectState));
    }

    public void testStepNotCompletedWhenCloneNotMarkedInMetadata() {
        String cloneIndexName = generateExpectedCloneName(indexName);
        ProjectState projectState = createProjectStateWithClone(indexName, cloneIndexName, null);
        assertFalse(cloneStep.stepCompleted(index, projectState));
    }

    public void testStepCompletedWhenCloneExistsAndMarkedInMetadata() {
        String cloneIndexName = generateExpectedCloneName(indexName);
        Map<String, String> customMetadata = Map.of("dlm_index_to_be_force_merged", cloneIndexName);
        ProjectState projectState = createProjectStateWithCloneAndRouting(indexName, cloneIndexName, customMetadata, true);
        assertTrue(cloneStep.stepCompleted(index, projectState));
    }

    public void testStepNotCompletedWhenShardsNotActive() {
        String cloneIndexName = generateExpectedCloneName(indexName);
        Map<String, String> customMetadata = Map.of("dlm_index_to_be_force_merged", cloneIndexName);
        ProjectState projectState = createProjectStateWithCloneAndRouting(indexName, cloneIndexName, customMetadata, false);
        assertFalse(cloneStep.stepCompleted(index, projectState));
    }

    public void testStepCompletedWhenOriginalIndexMarkedWithZeroReplicas() {
        Map<String, String> customMetadata = Map.of("dlm_index_to_be_force_merged", indexName);
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
        String cloneIndexName = generateExpectedCloneName(indexName);
        ProjectState projectState = createProjectStateWithClone(indexName, cloneIndexName, null);
        DlmStepContext stepContext = createStepContext(projectState);

        cloneStep.execute(stepContext);

        // Should issue delete request for existing clone
        assertThat(capturedDeleteRequest.get(), is(notNullValue()));
        assertThat(capturedDeleteRequest.get().indices()[0], equalTo(cloneIndexName));
    }

    public void testExecuteCreatesCloneWithCorrectSettings() {
        ProjectState projectState = createProjectState(indexName, 1, null);
        DlmStepContext stepContext = createStepContext(projectState);

        cloneStep.execute(stepContext);

        assertThat(capturedResizeRequest.get(), is(notNullValue()));
        assertThat(capturedResizeRequest.get().getSourceIndex(), equalTo(indexName));
        assertThat(capturedResizeRequest.get().getTargetIndexRequest().index(), containsString("dlm-force-merge-clone-" + indexName));
        assertThat(capturedResizeRequest.get().getTargetIndexRequest().settings().get("index.number_of_replicas"), equalTo("0"));
    }

    public void testExecuteWithSuccessfulCloneResponse() {
        ProjectState projectState = createProjectState(indexName, 1, null);
        DlmStepContext stepContext = createStepContext(projectState);

        cloneStep.execute(stepContext);

        assertThat("clone listener should be captured", capturedCloneListener.get(), is(notNullValue()));

        String cloneIndexName = generateExpectedCloneName(indexName);
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

        // Should attempt to delete the clone index
        assertThat(capturedDeleteRequest.get(), is(notNullValue()));
    }

    public void testGetCloneIndexNameIsDeterministic() {
        String cloneName1 = generateExpectedCloneName(indexName);
        String cloneName2 = generateExpectedCloneName(indexName);
        assertThat(cloneName1, equalTo(cloneName2));
        assertThat(cloneName1, containsString(indexName));
        assertThat(cloneName1, containsString("dlm-force-merge-clone-"));
    }

    public void testDeleteCloneSuccessfully() {
        String cloneIndexName = generateExpectedCloneName(indexName);
        ProjectState projectState = createProjectStateWithClone(indexName, cloneIndexName, null);
        DlmStepContext stepContext = createStepContext(projectState);

        cloneStep.execute(stepContext);

        // Respond to delete request successfully
        AcknowledgedResponse deleteResponse = AcknowledgedResponse.of(true);
        capturedDeleteListener.get().onResponse(deleteResponse);
    }

    public void testDeleteCloneWithFailure() {
        String cloneIndexName = generateExpectedCloneName(indexName);
        ProjectState projectState = createProjectStateWithClone(indexName, cloneIndexName, null);
        DlmStepContext stepContext = createStepContext(projectState);

        cloneStep.execute(stepContext);

        // Respond to delete request with failure
        ElasticsearchException exception = new ElasticsearchException("delete failed");
        capturedDeleteListener.get().onFailure(exception);
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

    private ProjectState createProjectStateWithClone(String sourceIndexName, String cloneIndexName, Map<String, String> customMetadata) {
        IndexMetadata.Builder sourceIndexBuilder = IndexMetadata.builder(sourceIndexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(1);

        if (customMetadata != null) {
            sourceIndexBuilder.putCustom(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, customMetadata);
        }

        IndexMetadata cloneIndexMetadata = IndexMetadata.builder(cloneIndexName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId)
            .put(sourceIndexBuilder.build(), false)
            .put(cloneIndexMetadata, false);

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(projectMetadataBuilder).build();

        return clusterState.projectState(projectId);
    }

    private ProjectState createProjectStateWithCloneAndRouting(
        String sourceIndexName,
        String cloneIndexName,
        Map<String, String> customMetadata,
        boolean allShardsActive
    ) {
        IndexMetadata.Builder sourceIndexBuilder = IndexMetadata.builder(sourceIndexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(1);

        if (customMetadata != null) {
            sourceIndexBuilder.putCustom(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, customMetadata);
        }

        IndexMetadata sourceIndexMetadata = sourceIndexBuilder.build();

        IndexMetadata cloneIndexMetadata = IndexMetadata.builder(cloneIndexName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId)
            .put(sourceIndexMetadata, false)
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

    private String generateExpectedCloneName(String originalName) {
        String hash = MessageDigests.toHexString(MessageDigests.sha256().digest(originalName.getBytes(StandardCharsets.UTF_8)))
            .substring(0, 8);
        return "dlm-force-merge-clone-" + originalName + "-" + hash;
    }
}
