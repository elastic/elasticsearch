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
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleErrorStore;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStepContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.datastreams.lifecycle.transitions.steps.IndexCreationStep.DLM_FROZEN_INDEX_PREFIX;
import static org.elasticsearch.datastreams.lifecycle.transitions.steps.IndexCreationStep.SNAPSHOT_NAME_PREFIX;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class IndexCreationStepTests extends ESTestCase {

    private static final String TEST_REPOSITORY = "test-repo";

    private IndexCreationStep indexCreationStep;
    private ProjectId projectId;
    private String indexName;
    private Index index;
    private ThreadPool threadPool;
    private Client client;
    private DataStreamLifecycleErrorStore errorStore;
    private ResultDeduplicator<Tuple<ProjectId, TransportRequest>, Void> deduplicator;
    private AtomicReference<ActionListener<RestoreSnapshotResponse>> capturedMountListener;
    private AtomicReference<MountSearchableSnapshotRequest> capturedMountRequest;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
        indexCreationStep = new IndexCreationStep();
        projectId = randomProjectIdOrDefault();
        indexName = randomAlphaOfLength(10);
        index = new Index(indexName, randomAlphaOfLength(10));
        errorStore = new DataStreamLifecycleErrorStore(System::currentTimeMillis);
        deduplicator = new ResultDeduplicator<>(threadPool.getThreadContext());
        capturedMountListener = new AtomicReference<>();
        capturedMountRequest = new AtomicReference<>();
        client = new NoOpClient(threadPool, TestProjectResolvers.usingRequestHeader(threadPool.getThreadContext())) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof MountSearchableSnapshotRequest mountRequest) {
                    capturedMountRequest.set(mountRequest);
                    capturedMountListener.set((ActionListener<RestoreSnapshotResponse>) listener);
                }
            }
        };
    }

    @After
    public void cleanup() {
        terminate(threadPool);
    }

    public void testStepNotCompletedWhenMountedIndexDoesNotExist() {
        ProjectState projectState = projectStateBuilder().build();
        assertFalse(indexCreationStep.stepCompleted(index, projectState));
    }

    public void testStepNotCompletedWhenMountedIndexExistsButShardsNotActive() {
        ProjectState projectState = projectStateBuilder().withMountedIndex().withRouting(false).build();
        assertFalse(indexCreationStep.stepCompleted(index, projectState));
    }

    public void testStepCompletedWhenMountedIndexExistsAndAllPrimaryShardsActive() {
        ProjectState projectState = projectStateBuilder().withMountedIndex().withRouting(true).build();
        assertTrue(indexCreationStep.stepCompleted(index, projectState));
    }

    public void testMaybeMountSnapshotSendsMountRequest() {
        ProjectState projectState = projectStateBuilder().build();
        DlmStepContext stepContext = createStepContext(projectState);

        indexCreationStep.maybeMountSnapshot(stepContext);

        assertThat("mount request should have been captured", capturedMountRequest.get(), is(notNullValue()));
        MountSearchableSnapshotRequest req = capturedMountRequest.get();
        assertThat(req.mountedIndexName(), equalTo(DLM_FROZEN_INDEX_PREFIX + indexName));
        assertThat(req.repositoryName(), equalTo(TEST_REPOSITORY));
        assertThat(req.snapshotName(), equalTo(SNAPSHOT_NAME_PREFIX + indexName));
        assertThat(req.snapshotIndexName(), equalTo(indexName));
        assertThat(req.storage(), equalTo(MountSearchableSnapshotRequest.Storage.SHARED_CACHE));
        assertThat(req.waitForCompletion(), equalTo(false));
    }

    public void testMaybeMountSnapshotIgnoresCorrectSettings() {
        ProjectState projectState = projectStateBuilder().build();
        DlmStepContext stepContext = createStepContext(projectState);

        indexCreationStep.maybeMountSnapshot(stepContext);

        assertThat(capturedMountRequest.get(), is(notNullValue()));
        String[] ignored = capturedMountRequest.get().ignoreIndexSettings();
        assertThat(ignored.length, equalTo(1));
        assertThat(ignored[0], equalTo("index.routing.allocation.total_shards_per_node"));
    }

    public void testMaybeMountSnapshotWithFailureRecordsError() {
        ProjectState projectState = projectStateBuilder().build();
        DlmStepContext stepContext = createStepContext(projectState);

        indexCreationStep.maybeMountSnapshot(stepContext);
        assertThat(capturedMountListener.get(), is(notNullValue()));

        ElasticsearchException failure = new ElasticsearchException("mount failed");
        capturedMountListener.get().onFailure(failure);

        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(Objects.requireNonNull(errorStore.getError(projectId, indexName)).error(), containsString("mount failed"));
    }

    public void testMountSnapshotWithOkStatusCompletesSuccessfully() {
        ProjectState projectState = projectStateBuilder().build();
        DlmStepContext stepContext = createStepContext(projectState);

        indexCreationStep.maybeMountSnapshot(stepContext);
        assertThat(capturedMountListener.get(), is(notNullValue()));

        RestoreInfo restoreInfo = new RestoreInfo("test-snap", List.of(DLM_FROZEN_INDEX_PREFIX + indexName), 1, 1);
        capturedMountListener.get().onResponse(new RestoreSnapshotResponse(restoreInfo));

        assertThat(errorStore.getError(projectId, indexName), is(nullValue()));
    }

    public void testMountSnapshotWithAcceptedStatusCompletesSuccessfully() {
        ProjectState projectState = projectStateBuilder().build();
        DlmStepContext stepContext = createStepContext(projectState);

        indexCreationStep.maybeMountSnapshot(stepContext);
        assertThat(capturedMountListener.get(), is(notNullValue()));

        capturedMountListener.get().onResponse(new RestoreSnapshotResponse((RestoreInfo) null));

        assertThat(errorStore.getError(projectId, indexName), is(nullValue()));
    }

    public void testMountSnapshotFailureRecordsErrorInStore() {
        ProjectState projectState = projectStateBuilder().build();
        DlmStepContext stepContext = createStepContext(projectState);

        indexCreationStep.maybeMountSnapshot(stepContext);
        assertThat(capturedMountListener.get(), is(notNullValue()));

        ElasticsearchException networkFailure = new ElasticsearchException("connection refused");
        capturedMountListener.get().onFailure(networkFailure);

        assertThat(errorStore.getError(projectId, indexName), is(notNullValue()));
        assertThat(Objects.requireNonNull(errorStore.getError(projectId, indexName)).error(), containsString("connection refused"));
    }

    private DlmStepContext createStepContext(ProjectState projectState) {
        return new DlmStepContext(
            index,
            projectState,
            deduplicator,
            errorStore,
            randomIntBetween(1, 10),
            client,
            Clock.systemDefaultZone()
        );
    }

    /**
     * Builder for creating {@link ProjectState} instances for testing, using a cluster-level
     * metadata setting that provides the default repository name required by
     * {@link IndexCreationStep#maybeMountSnapshot}.
     */
    private class ProjectStateBuilder {
        private boolean withMountedIndex = false;
        private boolean allShardsActive = true;
        private boolean withRouting = false;

        ProjectStateBuilder withMountedIndex() {
            this.withMountedIndex = true;
            return this;
        }

        ProjectStateBuilder withRouting(boolean allShardsActive) {
            this.withRouting = true;
            this.allShardsActive = allShardsActive;
            return this;
        }

        ProjectState build() {
            String mountedName = DLM_FROZEN_INDEX_PREFIX + indexName;

            IndexMetadata originalMeta = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                        .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                        .build()
                )
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();

            ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectId).put(originalMeta, false);

            RoutingTable routingTable = null;
            if (withMountedIndex) {
                IndexMetadata mountedMeta = IndexMetadata.builder(mountedName)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build();
                projectMetadataBuilder.put(mountedMeta, false);

                if (withRouting) {
                    ShardRouting primaryShard = TestShardRouting.newShardRouting(
                        new ShardId(mountedMeta.getIndex(), 0),
                        "node1",
                        true,
                        allShardsActive ? ShardRoutingState.STARTED : ShardRoutingState.INITIALIZING
                    );
                    routingTable = RoutingTable.builder()
                        .add(IndexRoutingTable.builder(mountedMeta.getIndex()).addShard(primaryShard))
                        .build();
                }
            }

            // Set the cluster-level default repository setting so resolveRepositoryName() works
            Metadata clusterMetadata = Metadata.builder()
                .persistentSettings(
                    Settings.builder().put(RepositoriesService.DEFAULT_REPOSITORY_SETTING.getKey(), TEST_REPOSITORY).build()
                )
                .put(projectMetadataBuilder)
                .build();

            ClusterState.Builder clusterStateBuilder = ClusterState.builder(ClusterName.DEFAULT).metadata(clusterMetadata);
            if (routingTable != null) {
                clusterStateBuilder.putRoutingTable(projectId, routingTable);
            }
            return clusterStateBuilder.build().projectState(projectId);
        }
    }

    private ProjectStateBuilder projectStateBuilder() {
        return new ProjectStateBuilder();
    }
}
