/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamFailureStore;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.lifecycle.health.DataStreamLifecycleHealthInfoPublisher;
import org.elasticsearch.dlm.DataStreamLifecycleErrorStore;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.IndexMetadata.DownsampleTaskStatus.STARTED;
import static org.elasticsearch.cluster.metadata.IndexMetadata.DownsampleTaskStatus.SUCCESS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.DownsampleTaskStatus.UNKNOWN;
import static org.elasticsearch.datastreams.DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleFixtures.createDataStream;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleFixtures.randomRolloverConditions;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.ONE_HUNDRED_MB;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.TARGET_MERGE_FACTOR_VALUE;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class DataStreamLifecycleServiceTests extends DataStreamLifecycleServiceTestCase {

    public void testOperationsExecutedOnce() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStreamLifecycle zeroRetentionDataLifecycle = DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.ZERO).build();
        DataStreamLifecycle zeroRetentionFailuresLifecycle = DataStreamLifecycle.failuresLifecycleBuilder()
            .dataRetention(TimeValue.ZERO)
            .build();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            2,
            settings(IndexVersion.current()),
            zeroRetentionDataLifecycle,
            zeroRetentionFailuresLifecycle,
            now
        );
        builder.put(dataStream);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).build();

        dataStreamLifecycleService.run(state);
        assertThat(clientSeenRequests.size(), is(5));
        assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
        RolloverRequest rolloverBackingIndexRequest = (RolloverRequest) clientSeenRequests.get(0);
        assertThat(rolloverBackingIndexRequest.getRolloverTarget(), is(dataStreamName));
        assertThat(clientSeenRequests.get(1), instanceOf(RolloverRequest.class));
        RolloverRequest rolloverFailureIndexRequest = (RolloverRequest) clientSeenRequests.get(1);
        assertThat(
            rolloverFailureIndexRequest.getRolloverTarget(),
            is(IndexNameExpressionResolver.combineSelector(dataStreamName, IndexComponentSelector.FAILURES))
        );
        List<DeleteIndexRequest> deleteRequests = clientSeenRequests.subList(2, 5)
            .stream()
            .map(transportRequest -> (DeleteIndexRequest) transportRequest)
            .toList();
        Set<String> indicesToDelete = Set.of(
            deleteRequests.get(0).indices()[0],
            deleteRequests.get(1).indices()[0],
            deleteRequests.get(2).indices()[0]
        );
        Set<String> indicesInDataStreamToDelete = Set.of(
            dataStream.getIndices().get(0).getName(),
            dataStream.getIndices().get(1).getName(),
            dataStream.getFailureIndices().get(0).getName()
        );
        assertThat(indicesToDelete, equalTo(indicesInDataStreamToDelete));

        // on the second run the rollover and delete requests should not execute anymore
        // i.e. the count should *remain* 1 for rollover and 2 for deletes
        dataStreamLifecycleService.run(state);
        assertThat(clientSeenRequests.size(), is(5));
    }

    public void testDLMRunsOnlyOnce() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStreamLifecycle zeroRetentionDataLifecycle = DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.ZERO).build();
        DataStreamLifecycle zeroRetentionFailuresLifecycle = DataStreamLifecycle.failuresLifecycleBuilder()
            .dataRetention(TimeValue.ZERO)
            .build();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            2,
            settings(IndexVersion.current()),
            zeroRetentionDataLifecycle,
            zeroRetentionFailuresLifecycle,
            now
        );
        builder.put(dataStream);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).build();

        clientWaitLatch = new CountDownLatch(1);
        invokerWaitLatch = new CountDownLatch(1);
        AtomicBoolean runCompleted = new AtomicBoolean(false);
        // Should block because of the latch
        Thread t = new Thread(() -> {
            dataStreamLifecycleService.run(state);
            runCompleted.set(true);
        });
        t.start();

        // So it's possible for the thread to be started above, but for the
        // actual `.run` invocation not to have been called by this point.
        // What we actually need to do is wait for some moment where we know
        // we're in the middle of the DLM service. In order to do that, we wait
        // for the "invokerWaitLatch" which is counted down inside of the fake
        // client. That way we know that the DLM service is running, but is
        // "paused" because of the `clientWaitLatch`.
        try {
            assertTrue("expected the client to count the latch down, but it didn't", invokerWaitLatch.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            fail("expected the client to have been invoked, but it never was");
        }
        // Will return immediately because it's already running
        logger.info("--> second 'run' invocation");
        dataStreamLifecycleService.run(state);

        // Let the first invocation proceed by decrementing clientWatchLatch.
        logger.info("--> decrementing latch");
        clientWaitLatch.countDown();
        try {
            logger.info("--> waiting for first run to complete");
            t.join();
        } catch (InterruptedException e) {
            throw new ElasticsearchException(e);
        }
        // Always check that we finished the initial `.run` call that we did
        // inside the thread.
        assertTrue(runCompleted.get());
    }

    public void testRetentionNotConfigured() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE,
            now
        );
        builder.put(dataStream);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).build();
        dataStreamLifecycleService.run(state);
        assertThat(clientSeenRequests.size(), is(3));  // rollover the write index, and force merge the other two
        assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
    }

    public void testRetentionNotExecutedDueToAge() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        int numFailureIndices = 2;
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            numFailureIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.timeValueDays(700)).build(),
            null,
            now
        );
        builder.put(dataStream);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).build();
        dataStreamLifecycleService.run(state);
        assertThat(clientSeenRequests.size(), is(5)); // roll over the 2 write indices, and force merge the other three
        assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
        assertThat(clientSeenRequests.get(1), instanceOf(RolloverRequest.class));
    }

    public void testRetentionNotExecutedForTSIndicesWithinTimeBounds() {
        Instant currentTime = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        // These ranges are on the edge of each other temporal boundaries.
        Instant start1 = currentTime.minus(6, ChronoUnit.HOURS);
        Instant end1 = currentTime.minus(4, ChronoUnit.HOURS);
        Instant start2 = currentTime.minus(4, ChronoUnit.HOURS);
        Instant end2 = currentTime.plus(2, ChronoUnit.HOURS);
        Instant start3 = currentTime.plus(2, ChronoUnit.HOURS);
        Instant end3 = currentTime.plus(4, ChronoUnit.HOURS);

        final var projectId = randomProjectIdOrDefault();
        String dataStreamName = "logs_my-app_prod";
        var clusterState = DataStreamTestHelper.getClusterStateWithDataStream(
            projectId,
            dataStreamName,
            List.of(Tuple.tuple(start1, end1), Tuple.tuple(start2, end2), Tuple.tuple(start3, end3))
        );
        ProjectMetadata.Builder builder = ProjectMetadata.builder(clusterState.metadata().getProject(projectId));
        DataStream dataStream = builder.dataStream(dataStreamName);
        builder.put(
            dataStream.copy()
                .setName(dataStreamName)
                .setGeneration(dataStream.getGeneration() + 1)
                .setLifecycle(DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.ZERO).build())
                .build()
        );
        clusterState = ClusterState.builder(clusterState).putProjectMetadata(builder).build();

        dataStreamLifecycleService.run(clusterState);
        assertThat(clientSeenRequests.size(), is(2)); // rollover the write index and one delete request for the index that's out of the
        // TS time bounds
        assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
        TransportRequest deleteIndexRequest = clientSeenRequests.get(1);
        assertThat(deleteIndexRequest, instanceOf(DeleteIndexRequest.class));
        // only the first generation index should be eligible for retention
        assertThat(((DeleteIndexRequest) deleteIndexRequest).indices(), arrayContaining(dataStream.getIndices().getFirst().getName()));
    }

    public void testMergePolicyNotExecutedForTSIndicesWithinTimeBounds() {
        Instant currentTime = Instant.ofEpochMilli(now).truncatedTo(ChronoUnit.MILLIS);
        // These ranges are on the edge of each other temporal boundaries.
        Instant start1 = currentTime.minus(6, ChronoUnit.HOURS);
        Instant end1 = currentTime.minus(4, ChronoUnit.HOURS);
        Instant start2 = currentTime.minus(4, ChronoUnit.HOURS);
        Instant end2 = currentTime.plus(2, ChronoUnit.HOURS);
        Instant start3 = currentTime.plus(2, ChronoUnit.HOURS);
        Instant end3 = currentTime.plus(4, ChronoUnit.HOURS);

        final var projectId = randomProjectIdOrDefault();
        String dataStreamName = "logs_my-app_prod";
        var clusterState = DataStreamTestHelper.getClusterStateWithDataStream(
            projectId,
            dataStreamName,
            List.of(Tuple.tuple(start1, end1), Tuple.tuple(start2, end2), Tuple.tuple(start3, end3))
        );
        ProjectMetadata.Builder builder = ProjectMetadata.builder(clusterState.metadata().getProject(projectId));
        DataStream dataStream = builder.dataStream(dataStreamName);
        // Overwrite the data stream in the cluster state to set the lifecycle policy, with no retention policy (i.e. infinite retention).
        builder.put(
            dataStream.copy()
                .setName(dataStreamName)
                .setGeneration(dataStream.getGeneration() + 1)
                .setLifecycle(DataStreamLifecycle.dataLifecycleBuilder().build())
                .build()
        );
        clusterState = ClusterState.builder(clusterState).putProjectMetadata(builder).build();

        dataStreamLifecycleService.run(clusterState);
        // There should be two client requests: one rollover, and one to update the merge policy settings. N.B. The merge policy settings
        // will always be updated before the force merge is done, see testMergePolicySettingsAreConfiguredBeforeForcemerge.
        assertThat(clientSeenRequests.size(), is(2));
        assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
        TransportRequest updateSettingsRequest = clientSeenRequests.get(1);
        assertThat(updateSettingsRequest, instanceOf(UpdateSettingsRequest.class));
        // Only the first generation index should be eligible for merging. The other have end dates in the future.
        assertThat(
            ((UpdateSettingsRequest) updateSettingsRequest).indices(),
            arrayContaining(dataStream.getIndices().getFirst().getName())
        );
        assertThat(
            ((UpdateSettingsRequest) updateSettingsRequest).settings().keySet(),
            containsInAnyOrder(
                MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(),
                MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING.getKey()
            )
        );
    }

    public void testRetentionSkippedWhilstDownsamplingInProgress() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.timeValueMillis(0)).build(),
            now
        );
        builder.put(dataStream);

        final var project = builder.build();

        {
            ProjectMetadata.Builder newProjectBuilder = ProjectMetadata.builder(project);

            String firstBackingIndex = dataStream.getIndices().getFirst().getName();
            IndexMetadata indexMetadata = project.index(firstBackingIndex);
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(indexMetadata);
            indexMetaBuilder.settings(
                Settings.builder()
                    .put(indexMetadata.getSettings())
                    .put(
                        IndexMetadata.INDEX_DOWNSAMPLE_STATUS_KEY,
                        STARTED // See: See TransportDownsampleAction#createDownsampleIndex(...)
                    )
            );
            indexMetaBuilder.putCustom(
                LIFECYCLE_CUSTOM_INDEX_METADATA_KEY,
                Map.of(FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY, String.valueOf(System.currentTimeMillis()))
            );
            newProjectBuilder.put(indexMetaBuilder);
            ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(newProjectBuilder).build();

            dataStreamLifecycleService.run(state);
            assertThat(clientSeenRequests.size(), is(2)); // rollover the write index and delete the second generation
            assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
            assertThat(clientSeenRequests.get(1), instanceOf(DeleteIndexRequest.class));
            assertThat(
                ((DeleteIndexRequest) clientSeenRequests.get(1)).indices()[0],
                DataStreamTestHelper.backingIndexEqualTo(dataStreamName, 2)
            );
        }

        {
            // a lack of downsample status (i.e. the default `UNKNOWN`) must not prevent retention
            ProjectMetadata.Builder newProjectBuilder = ProjectMetadata.builder(project);

            String firstBackingIndex = dataStream.getIndices().getFirst().getName();
            IndexMetadata indexMetadata = project.index(firstBackingIndex);
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(indexMetadata);
            indexMetaBuilder.settings(
                Settings.builder().put(indexMetadata.getSettings()).putNull(IndexMetadata.INDEX_DOWNSAMPLE_STATUS_KEY)
            );
            newProjectBuilder.put(indexMetaBuilder);
            ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(newProjectBuilder).build();

            dataStreamLifecycleService.run(state);
            assertThat(clientSeenRequests.size(), is(3)); // rollover the write index and delete the other two generations
            assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
            assertThat(clientSeenRequests.get(1), instanceOf(DeleteIndexRequest.class));
            assertThat(
                ((DeleteIndexRequest) clientSeenRequests.get(1)).indices()[0],
                DataStreamTestHelper.backingIndexEqualTo(dataStreamName, 2)
            );
            assertThat(clientSeenRequests.get(2), instanceOf(DeleteIndexRequest.class));
            assertThat(
                ((DeleteIndexRequest) clientSeenRequests.get(2)).indices()[0],
                DataStreamTestHelper.backingIndexEqualTo(dataStreamName, 1)
            );
        }
    }

    public void testIlmManagedIndicesAreSkipped() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            Settings.builder()
                .put(IndexMetadata.LIFECYCLE_NAME, "ILM_policy")
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()),
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.ZERO).build(),
            now
        );
        builder.put(dataStream);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).build();
        dataStreamLifecycleService.run(state);
        assertThat(clientSeenRequests.isEmpty(), is(true));
    }

    public void testDataStreamsWithoutLifecycleAreSkipped() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            Settings.builder()
                .put(IndexMetadata.LIFECYCLE_NAME, "ILM_policy")
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()),
            null,
            now
        );
        builder.put(dataStream);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).build();
        dataStreamLifecycleService.run(state);
        assertThat(clientSeenRequests.isEmpty(), is(true));
    }

    public void testDeletedIndicesAreRemovedFromTheErrorStore() throws IOException {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()),
            DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE,
            now
        );
        builder.put(dataStream);
        String nodeId = "localNode";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        // we are the master node
        nodesBuilder.masterNodeId(nodeId);
        ClusterState previousState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).nodes(nodesBuilder).build();

        // all backing indices are in the error store
        for (Index index : dataStream.getIndices()) {
            dataStreamLifecycleService.getErrorStore().recordError(builder.getId(), index.getName(), new NullPointerException("bad"));
        }
        Index writeIndex = dataStream.getWriteIndex();
        // all indices but the write index are deleted
        List<Index> deletedIndices = dataStream.getIndices().stream().filter(index -> index.equals(writeIndex) == false).toList();

        ClusterState.Builder newStateBuilder = ClusterState.builder(previousState);
        newStateBuilder.stateUUID(UUIDs.randomBase64UUID());
        ProjectMetadata.Builder metaBuilder = ProjectMetadata.builder(previousState.metadata().getProject(builder.getId()));
        for (Index index : deletedIndices) {
            metaBuilder.remove(index.getName());
            IndexGraveyard.Builder graveyardBuilder = IndexGraveyard.builder(metaBuilder.indexGraveyard());
            graveyardBuilder.addTombstone(index);
            metaBuilder.indexGraveyard(graveyardBuilder.build());
        }
        newStateBuilder.putProjectMetadata(metaBuilder);
        ClusterState stateWithDeletedIndices = newStateBuilder.nodes(buildNodes(nodeId).masterNodeId(nodeId)).build();
        setState(clusterService, stateWithDeletedIndices);

        dataStreamLifecycleService.run(stateWithDeletedIndices);

        for (Index deletedIndex : deletedIndices) {
            assertThat(dataStreamLifecycleService.getErrorStore().getError(builder.getId(), deletedIndex.getName()), nullValue());
        }
        // the value for the write index should still be in the error store
        assertThat(
            dataStreamLifecycleService.getErrorStore().getError(builder.getId(), dataStream.getWriteIndex().getName()),
            notNullValue()
        );
    }

    public void testErrorStoreIsClearedOnBackingIndexBecomingUnmanaged() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.timeValueDays(700)).build(),
            now
        );
        // all backing indices are in the error store
        for (Index index : dataStream.getIndices()) {
            dataStreamLifecycleService.getErrorStore().recordError(builder.getId(), index.getName(), new NullPointerException("bad"));
        }
        builder.put(dataStream);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).build();

        final var project = state.metadata().getProject(builder.getId());
        ProjectMetadata.Builder newBuilder = ProjectMetadata.builder(project);

        // update the backing indices to be ILM managed
        for (Index index : dataStream.getIndices()) {
            IndexMetadata indexMetadata = project.index(index);
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(indexMetadata);
            indexMetaBuilder.settings(Settings.builder().put(indexMetadata.getSettings()).put(IndexMetadata.LIFECYCLE_NAME, "ILM_policy"));
            newBuilder.put(indexMetaBuilder.build(), true);
        }
        ClusterState updatedState = ClusterState.builder(state).putProjectMetadata(newBuilder).build();
        setState(clusterService, updatedState);

        dataStreamLifecycleService.run(updatedState);

        for (Index index : dataStream.getIndices()) {
            assertThat(dataStreamLifecycleService.getErrorStore().getError(builder.getId(), index.getName()), nullValue());
        }
    }

    public void testBackingIndicesFromMultipleDataStreamsInErrorStore() {
        String ilmManagedDataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStream ilmManagedDataStream = createDataStream(
            builder,
            ilmManagedDataStreamName,
            3,
            settings(IndexVersion.current()),
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.timeValueDays(700)).build(),
            now
        );
        // all backing indices are in the error store
        for (Index index : ilmManagedDataStream.getIndices()) {
            dataStreamLifecycleService.getErrorStore()
                .recordError(builder.getId(), index.getName(), new NullPointerException("will be ILM managed soon"));
        }
        String dataStreamWithBackingIndicesInErrorState = randomAlphaOfLength(15).toLowerCase(Locale.ROOT);
        DataStream dslManagedDataStream = createDataStream(
            builder,
            dataStreamWithBackingIndicesInErrorState,
            5,
            settings(IndexVersion.current()),
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.timeValueDays(700)).build(),
            now
        );
        // put all backing indices in the error store
        for (Index index : dslManagedDataStream.getIndices()) {
            dataStreamLifecycleService.getErrorStore()
                .recordError(builder.getId(), index.getName(), new NullPointerException("dsl managed index"));
        }
        builder.put(ilmManagedDataStream);
        builder.put(dslManagedDataStream);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).build();

        final var project = state.metadata().getProject(builder.getId());
        ProjectMetadata.Builder newBuilder = ProjectMetadata.builder(project);

        // update the backing indices to be ILM managed so they should be removed from the error store on the next DSL run
        for (Index index : ilmManagedDataStream.getIndices()) {
            IndexMetadata indexMetadata = project.index(index);
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(indexMetadata);
            indexMetaBuilder.settings(Settings.builder().put(indexMetadata.getSettings()).put(IndexMetadata.LIFECYCLE_NAME, "ILM_policy"));
            newBuilder.put(indexMetaBuilder.build(), true);
        }
        ClusterState updatedState = ClusterState.builder(state).putProjectMetadata(newBuilder).build();
        setState(clusterService, updatedState);

        dataStreamLifecycleService.run(updatedState);

        for (Index index : dslManagedDataStream.getIndices()) {
            assertThat(dataStreamLifecycleService.getErrorStore().getError(builder.getId(), index.getName()), notNullValue());
        }
        for (Index index : ilmManagedDataStream.getIndices()) {
            assertThat(dataStreamLifecycleService.getErrorStore().getError(builder.getId(), index.getName()), nullValue());
        }
    }

    @SuppressWarnings("unchecked")
    public void testForceMerge() throws Exception {
        // We want this test method to get fake force merge responses, because this is what triggers a cluster state update
        clientDelegate = (action, request, listener) -> {
            if (action.name().equals("indices:admin/forcemerge")) {
                listener.onResponse(new BroadcastResponse(5, 5, 0, List.of()));
            }
        };
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()).put(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(), ONE_HUNDRED_MB)
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING.getKey(), TARGET_MERGE_FACTOR_VALUE),
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.MAX_VALUE).build(),
            now
        );
        builder.put(dataStream);

        String nodeId = "localNode";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        // we are the master node
        nodesBuilder.masterNodeId(nodeId);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).nodes(nodesBuilder).build();
        setState(clusterService, state);
        dataStreamLifecycleService.run(clusterService.state());

        // There are 3 backing indices. One gets rolled over. The other two get force merged:
        assertBusy(() -> {
            final var project = clusterService.state().metadata().getProject(builder.getId());
            assertThat(project.index(dataStream.getIndices().get(0)).getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY), notNullValue());
            assertThat(project.index(dataStream.getIndices().get(1)).getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY), notNullValue());
            assertThat(
                project.index(dataStream.getIndices().get(0))
                    .getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY)
                    .get(FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY),
                notNullValue()
            );
            assertThat(
                project.index(dataStream.getIndices().get(1))
                    .getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY)
                    .get(FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY),
                notNullValue()
            );
        });
        assertBusy(() -> { assertThat(clientSeenRequests.size(), is(3)); }, 30, TimeUnit.SECONDS);
        assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
        assertThat(((RolloverRequest) clientSeenRequests.get(0)).getRolloverTarget(), is(dataStreamName));
        List<ForceMergeRequest> forceMergeRequests = clientSeenRequests.subList(1, 3)
            .stream()
            .map(transportRequest -> (ForceMergeRequest) transportRequest)
            .toList();
        assertThat(forceMergeRequests.get(0).indices()[0], is(dataStream.getIndices().get(0).getName()));
        assertThat(forceMergeRequests.get(1).indices()[0], is(dataStream.getIndices().get(1).getName()));

        // No changes, so running should not create any more requests
        dataStreamLifecycleService.run(clusterService.state());
        assertThat(clientSeenRequests.size(), is(3));

        // Add another index backing, and make sure that the only thing that happens is another force merge
        IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(
            DataStream.getDefaultBackingIndexName(dataStreamName, numBackingIndices + 1)
        )
            .settings(
                settings(IndexVersion.current()).put(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(), ONE_HUNDRED_MB)
                    .put(MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING.getKey(), TARGET_MERGE_FACTOR_VALUE)
            )
            .numberOfShards(1)
            .numberOfReplicas(1)
            .creationDate(now - 3000L);
        MaxAgeCondition rolloverCondition = new MaxAgeCondition(TimeValue.timeValueMillis(now - 2000L));
        indexMetaBuilder.putRolloverInfo(new RolloverInfo(dataStreamName, List.of(rolloverCondition), now - 2000L));
        IndexMetadata newIndexMetadata = indexMetaBuilder.build();
        ProjectMetadata newProject = ProjectMetadata.builder(clusterService.state().metadata().getProject(builder.getId()))
            .put(newIndexMetadata, true)
            .build();
        state = ClusterState.builder(clusterService.state()).putProjectMetadata(newProject).build();
        setState(clusterService, state);
        DataStream dataStream2 = dataStream.addBackingIndex(newProject, newIndexMetadata.getIndex());
        newProject = ProjectMetadata.builder(newProject).put(dataStream2).build();
        state = ClusterState.builder(clusterService.state()).putProjectMetadata(newProject).build();
        setState(clusterService, state);
        dataStreamLifecycleService.run(clusterService.state());
        assertBusy(() -> { assertThat(clientSeenRequests.size(), is(4)); });
        assertThat(((ForceMergeRequest) clientSeenRequests.get(3)).indices().length, is(1));
        assertBusy(() -> {
            final var retrievedProject = clusterService.state().metadata().getProject(builder.getId());
            assertThat(
                retrievedProject.index(dataStream2.getIndices().get(2)).getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY),
                notNullValue()
            );
            assertThat(
                retrievedProject.index(dataStream2.getIndices().get(2))
                    .getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY)
                    .get(FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY),
                notNullValue()
            );
        });
    }

    @SuppressWarnings("unchecked")
    public void testForceMergeRetries() throws Exception {
        /*
         * This test makes sure that data stream lifecycle correctly retries (or doesn't) forcemerge requests on failure.
         * First, we set up a datastream with 3 backing indices. On the first run of the data stream lifecycle we'll expect
         * one to get rolled over and two to be forcemerged.
         */
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()).put(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(), ONE_HUNDRED_MB)
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING.getKey(), TARGET_MERGE_FACTOR_VALUE),
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.MAX_VALUE).build(),
            now
        );
        builder.put(dataStream);

        String nodeId = "localNode";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        // we are the master node
        nodesBuilder.masterNodeId(nodeId);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).nodes(nodesBuilder).build();
        setState(clusterService, state);

        {
            /*
             * For the first data stream lifecycle run we're intentionally making forcemerge fail:
             */
            AtomicInteger forceMergeFailedCount = new AtomicInteger(0);
            clientDelegate = (action, request, listener) -> {
                if (action.name().equals("indices:admin/forcemerge")) {
                    listener.onFailure(new RuntimeException("Forcemerge failure"));
                    forceMergeFailedCount.incrementAndGet();
                }
            };
            dataStreamLifecycleService.run(clusterService.state());
            /*
             * We expect that data stream lifecycle will try to pick it up next time.
             */
            assertBusy(() -> {
                assertThat(forceMergeFailedCount.get(), equalTo(2));
                final var project = clusterService.state().metadata().getProject(builder.getId());
                assertThat(project.index(dataStream.getIndices().get(0)).getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY), nullValue());
            });
        }

        {
            /*
             * For the next data stream lifecycle run we're intentionally making forcemerge fail by reporting failed shards:
             */
            AtomicInteger forceMergeFailedCount = new AtomicInteger(0);
            clientDelegate = (action, request, listener) -> {
                if (action.name().equals("indices:admin/forcemerge")) {
                    listener.onResponse(
                        new BroadcastResponse(
                            5,
                            5,
                            1,
                            List.of(new DefaultShardOperationFailedException(new ElasticsearchException("failure")))
                        )
                    );
                    forceMergeFailedCount.incrementAndGet();
                }
            };
            dataStreamLifecycleService.run(clusterService.state());
            assertBusy(() -> {
                assertThat(forceMergeFailedCount.get(), equalTo(2));
                final var project = clusterService.state().metadata().getProject(builder.getId());
                assertThat(project.index(dataStream.getIndices().get(0)).getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY), nullValue());
            });
        }

        {
            /*
             * For the next data stream lifecycle run we're intentionally making forcemerge fail on the same indices by having the
             * successful shards not equal to the total.
             */
            AtomicInteger forceMergeFailedCount = new AtomicInteger(0);
            clientDelegate = (action, request, listener) -> {
                if (action.name().equals("indices:admin/forcemerge")) {
                    listener.onResponse(new BroadcastResponse(5, 4, 0, List.of()));
                    forceMergeFailedCount.incrementAndGet();
                }
            };
            dataStreamLifecycleService.run(clusterService.state());
            assertBusy(() -> {
                assertThat(forceMergeFailedCount.get(), equalTo(2));
                final var project = clusterService.state().metadata().getProject(builder.getId());
                assertThat(project.index(dataStream.getIndices().get(0)).getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY), nullValue());
            });
        }

        {
            // For the final data stream lifecycle run, we let forcemerge run normally
            clientDelegate = (action, request, listener) -> {
                if (action.name().equals("indices:admin/forcemerge")) {
                    listener.onResponse(new BroadcastResponse(5, 5, 0, List.of()));
                }
            };
            dataStreamLifecycleService.run(clusterService.state());
            /*
             * And this time we expect that it will actually run the forcemerge, and update the marker to complete:
             */
            assertBusy(() -> {
                final var project = clusterService.state().metadata().getProject(builder.getId());
                assertThat(
                    project.index(dataStream.getIndices().get(0)).getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY),
                    notNullValue()
                );
                assertThat(
                    project.index(dataStream.getIndices().get(1)).getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY),
                    notNullValue()
                );
                assertThat(
                    project.index(dataStream.getIndices().get(0))
                        .getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY)
                        .get(FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY),
                    notNullValue()
                );
                assertThat(
                    project.index(dataStream.getIndices().get(1))
                        .getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY)
                        .get(FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY),
                    notNullValue()
                );
            });
            assertBusy(() -> { assertThat(clientSeenRequests.size(), is(9)); });
            assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
            assertThat(((RolloverRequest) clientSeenRequests.get(0)).getRolloverTarget(), is(dataStreamName));
            // There will be two more forcemerge requests total now: the six failed ones from before, and now the two successful ones
            List<ForceMergeRequest> forceMergeRequests = clientSeenRequests.subList(1, 9)
                .stream()
                .map(transportRequest -> (ForceMergeRequest) transportRequest)
                .toList();
            assertThat(forceMergeRequests.get(0).indices()[0], is(dataStream.getIndices().get(0).getName()));
            assertThat(forceMergeRequests.get(1).indices()[0], is(dataStream.getIndices().get(1).getName()));
            assertThat(forceMergeRequests.get(2).indices()[0], is(dataStream.getIndices().get(0).getName()));
            assertThat(forceMergeRequests.get(3).indices()[0], is(dataStream.getIndices().get(1).getName()));
            assertThat(forceMergeRequests.get(4).indices()[0], is(dataStream.getIndices().get(0).getName()));
            assertThat(forceMergeRequests.get(5).indices()[0], is(dataStream.getIndices().get(1).getName()));
            assertThat(forceMergeRequests.get(6).indices()[0], is(dataStream.getIndices().get(0).getName()));
            assertThat(forceMergeRequests.get(7).indices()[0], is(dataStream.getIndices().get(1).getName()));
        }
    }

    @SuppressWarnings("unchecked")
    public void testForceMergeDedup() throws Exception {
        /*
         * This test creates a datastream with one index, and then runs data stream lifecycle repeatedly many times. We assert that the size
         *  of the transportActionsDeduplicator never goes over 1, and is 0 by the end. This is to make sure that the equals/hashcode
         * methods of ForceMergeRequests are interacting with the deduplicator as expected.
         */
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 1))
            .settings(
                settings(IndexVersion.current()).put(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(), ONE_HUNDRED_MB)
                    .put(MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING.getKey(), TARGET_MERGE_FACTOR_VALUE)
            )
            .numberOfShards(1)
            .numberOfReplicas(1)
            .creationDate(now - 3000L);
        MaxAgeCondition rolloverCondition = new MaxAgeCondition(TimeValue.timeValueMillis(now - 2000L));
        indexMetaBuilder.putRolloverInfo(new RolloverInfo(dataStreamName, List.of(rolloverCondition), now - 2000L));
        IndexMetadata newIndexMetadata = indexMetaBuilder.build();

        DataStream dataStream = DataStreamTestHelper.newInstance(
            dataStreamName,
            // TODO: we have to add a write index that does not exist in the metadata to make this test pass. There is probably some value
            // in checking that the deduplicator works, but this test depends on a broken/weird cluster state and it doesn't seem to have
            // much value in its current state anyway.
            List.of(newIndexMetadata.getIndex(), new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 2), randomUUID())),
            1L,
            null,
            false,
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.MAX_VALUE).build()
        );

        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault()).put(newIndexMetadata, true).put(dataStream);
        ClusterState state = ClusterState.builder(clusterService.state()).putProjectMetadata(builder).build();
        setState(clusterService, state);
        clientDelegate = (action, request, listener) -> {
            if (action.name().equals("indices:admin/forcemerge")) {
                listener.onResponse(new BroadcastResponse(5, 5, 0, List.of()));
            }
        };
        for (int i = 0; i < 100; i++) {
            dataStreamLifecycleService.run(clusterService.state());
            assertThat(dataStreamLifecycleService.transportActionsDeduplicator.size(), lessThanOrEqualTo(1));
        }
        assertBusy(() -> assertThat(dataStreamLifecycleService.transportActionsDeduplicator.size(), equalTo(0)));
    }

    public void testUpdateForceMergeCompleteTask() throws Exception {
        AtomicInteger onResponseCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicReference<Exception> failure = new AtomicReference<>();
        ActionListener<Void> listener = new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                onResponseCount.incrementAndGet();
            }

            @Override
            public void onFailure(Exception e) {
                failureCount.incrementAndGet();
                failure.set(e);
            }
        };
        final var projectId = randomProjectIdOrDefault();
        String targetIndex = randomAlphaOfLength(20);
        DataStreamLifecycleService.UpdateForceMergeCompleteTask task = new DataStreamLifecycleService.UpdateForceMergeCompleteTask(
            listener,
            projectId,
            targetIndex,
            threadPool
        );
        {
            Exception exception = new RuntimeException("task failed");
            task.onFailure(exception);
            assertThat(failureCount.get(), equalTo(1));
            assertThat(onResponseCount.get(), equalTo(0));
            assertThat(failure.get(), equalTo(exception));
            ClusterState clusterState = createClusterState(projectId, targetIndex, null);
            ClusterState newClusterState = task.execute(clusterState);
            IndexMetadata indexMetadata = newClusterState.metadata().getProject(projectId).index(targetIndex);
            assertThat(indexMetadata, notNullValue());
            Map<String, String> dataStreamLifecycleMetadata = indexMetadata.getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY);
            assertThat(dataStreamLifecycleMetadata, notNullValue());
            assertThat(dataStreamLifecycleMetadata.size(), equalTo(1));
            String forceMergeCompleteTimestampString = dataStreamLifecycleMetadata.get(FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY);
            assertThat(forceMergeCompleteTimestampString, notNullValue());
            long forceMergeCompleteTimestamp = Long.parseLong(forceMergeCompleteTimestampString);
            assertThat(forceMergeCompleteTimestamp, lessThanOrEqualTo(threadPool.absoluteTimeInMillis()));
            // The listener's onResponse should not be called by execute():
            assertThat(onResponseCount.get(), equalTo(0));
        }
        {
            /*
             * This is the same as the previous block, except that this time we'll have previously-existing data stream lifecycle custom
             * metadata in the index's metadata, and make sure that it doesn't get blown away when we set the timestamp.
             */
            String preExistingDataStreamLifecycleCustomMetadataKey = randomAlphaOfLength(10);
            String preExistingDataStreamLifecycleCustomMetadataValue = randomAlphaOfLength(20);
            Map<String, String> preExistingDataStreamLifecycleCustomMetadata = Map.of(
                preExistingDataStreamLifecycleCustomMetadataKey,
                preExistingDataStreamLifecycleCustomMetadataValue
            );
            ClusterState clusterState = createClusterState(projectId, targetIndex, preExistingDataStreamLifecycleCustomMetadata);
            ClusterState newClusterState = task.execute(clusterState);
            IndexMetadata indexMetadata = newClusterState.metadata().getProject(projectId).index(targetIndex);
            Map<String, String> dataStreamLifecycleMetadata = indexMetadata.getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY);
            assertThat(dataStreamLifecycleMetadata, notNullValue());
            assertThat(dataStreamLifecycleMetadata.size(), equalTo(2));
            assertThat(
                dataStreamLifecycleMetadata.get(preExistingDataStreamLifecycleCustomMetadataKey),
                equalTo(preExistingDataStreamLifecycleCustomMetadataValue)
            );
            String forceMergeCompleteTimestampString = dataStreamLifecycleMetadata.get(FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY);
            assertThat(forceMergeCompleteTimestampString, notNullValue());
            long forceMergeCompleteTimestamp = Long.parseLong(forceMergeCompleteTimestampString);
            assertThat(forceMergeCompleteTimestamp, lessThanOrEqualTo(threadPool.absoluteTimeInMillis()));
            // The listener's onResponse should not be called by execute():
            assertThat(onResponseCount.get(), equalTo(0));
        }
    }

    public void testDefaultRolloverRequest() {
        // test auto max_age and another concrete condition
        {
            RolloverConditions randomConcreteRolloverConditions = randomRolloverConditions(false);
            RolloverRequest rolloverRequest = DataStreamLifecycleService.getDefaultRolloverRequest(
                new RolloverConfiguration(randomConcreteRolloverConditions, Set.of("max_age")),
                "my-data-stream",
                null,
                false
            );
            assertThat(rolloverRequest.getRolloverTarget(), equalTo("my-data-stream"));
            assertThat(
                rolloverRequest.getConditions(),
                equalTo(
                    RolloverConditions.newBuilder(randomConcreteRolloverConditions)
                        .addMaxIndexAgeCondition(TimeValue.timeValueDays(30))
                        .build()
                )
            );
            RolloverRequest rolloverRequestWithRetention = DataStreamLifecycleService.getDefaultRolloverRequest(
                new RolloverConfiguration(randomConcreteRolloverConditions, Set.of("max_age")),
                "my-data-stream",
                TimeValue.timeValueDays(3),
                false
            );
            assertThat(
                rolloverRequestWithRetention.getConditions(),
                equalTo(
                    RolloverConditions.newBuilder(randomConcreteRolloverConditions)
                        .addMaxIndexAgeCondition(TimeValue.timeValueDays(1))
                        .build()
                )
            );
        }
        // test without any automatic conditions
        {
            RolloverConditions randomConcreteRolloverConditions = randomRolloverConditions(true);
            RolloverRequest rolloverRequest = DataStreamLifecycleService.getDefaultRolloverRequest(
                new RolloverConfiguration(randomConcreteRolloverConditions),
                "my-data-stream",
                null,
                false
            );
            assertThat(rolloverRequest.getRolloverTarget(), equalTo("my-data-stream"));
            assertThat(rolloverRequest.getConditions(), equalTo(randomConcreteRolloverConditions));
            RolloverRequest rolloverRequestWithRetention = DataStreamLifecycleService.getDefaultRolloverRequest(
                new RolloverConfiguration(randomConcreteRolloverConditions),
                "my-data-stream",
                TimeValue.timeValueDays(1),
                false
            );
            assertThat(rolloverRequestWithRetention.getConditions(), equalTo(randomConcreteRolloverConditions));
        }
    }

    public void testForceMergeRequestWrapperEqualsHashCode() {
        String[] indices = new String[randomIntBetween(0, 10)];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = randomAlphaOfLength(20);
        }
        ForceMergeRequest originalRequest = new ForceMergeRequest(indices);
        originalRequest.setRequestId(randomLong());
        originalRequest.setShouldStoreResult(randomBoolean());
        originalRequest.maxNumSegments(randomInt(1000));
        originalRequest.setParentTask(randomAlphaOfLength(10), randomLong());
        originalRequest.onlyExpungeDeletes(randomBoolean());
        originalRequest.flush(randomBoolean());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new ForceMergeRequestWrapper(originalRequest),
            DataStreamLifecycleServiceTests::copyForceMergeRequestWrapperRequest,
            DataStreamLifecycleServiceTests::mutateForceMergeRequestWrapper
        );
    }

    public void testMergePolicySettingsAreConfiguredBeforeForcemerge() throws Exception {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.MAX_VALUE).build(),
            now
        );
        builder.put(dataStream);

        String nodeId = "localNode";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        // we are the master node
        nodesBuilder.masterNodeId(nodeId);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).nodes(nodesBuilder).build();
        setState(clusterService, state);
        dataStreamLifecycleService.run(clusterService.state());

        // There are 3 backing indices. One gets rolled over. The other two will need to have their merge policy updated:
        assertBusy(() -> assertThat(clientSeenRequests.size(), is(3)), 30, TimeUnit.SECONDS);
        assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
        assertThat(((RolloverRequest) clientSeenRequests.get(0)).getRolloverTarget(), is(dataStreamName));
        List<UpdateSettingsRequest> updateSettingsRequests = clientSeenRequests.subList(1, 3)
            .stream()
            .map(transportRequest -> (UpdateSettingsRequest) transportRequest)
            .toList();
        assertThat(updateSettingsRequests.get(0).indices()[0], is(dataStream.getIndices().get(0).getName()));
        assertThat(updateSettingsRequests.get(1).indices()[0], is(dataStream.getIndices().get(1).getName()));

        for (UpdateSettingsRequest settingsRequest : updateSettingsRequests) {
            assertThat(
                settingsRequest.settings()
                    .getAsBytesSize(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(), ByteSizeValue.MINUS_ONE),
                is(ONE_HUNDRED_MB)
            );
            assertThat(
                settingsRequest.settings().getAsInt(MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING.getKey(), -1),
                is(TARGET_MERGE_FACTOR_VALUE)
            );
        }
        // No changes, so running should not create any more requests
        dataStreamLifecycleService.run(clusterService.state());
        assertThat(clientSeenRequests.size(), is(3));

        // let's add one more backing index that has the expected merge policy and check the data stream lifecycle issues a forcemerge
        // request for it
        IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(
            DataStream.getDefaultBackingIndexName(dataStreamName, numBackingIndices + 1)
        )
            .settings(
                settings(IndexVersion.current()).put(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(), ONE_HUNDRED_MB)
                    .put(MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING.getKey(), TARGET_MERGE_FACTOR_VALUE)
            )
            .numberOfShards(1)
            .numberOfReplicas(1)
            .creationDate(now - 3000L);
        MaxAgeCondition rolloverCondition = new MaxAgeCondition(TimeValue.timeValueMillis(now - 2000L));
        indexMetaBuilder.putRolloverInfo(new RolloverInfo(dataStreamName, List.of(rolloverCondition), now - 2000L));
        IndexMetadata newIndexMetadata = indexMetaBuilder.build();
        builder = ProjectMetadata.builder(clusterService.state().metadata().getProject(builder.getId())).put(newIndexMetadata, true);
        state = ClusterState.builder(clusterService.state()).putProjectMetadata(builder).build();
        setState(clusterService, state);
        DataStream modifiedDataStream = dataStream.addBackingIndex(
            clusterService.state().metadata().getProject(builder.getId()),
            newIndexMetadata.getIndex()
        );
        builder = ProjectMetadata.builder(clusterService.state().metadata().getProject(builder.getId())).put(modifiedDataStream);
        state = ClusterState.builder(clusterService.state()).putProjectMetadata(builder).build();
        setState(clusterService, state);
        dataStreamLifecycleService.run(clusterService.state());
        assertBusy(() -> assertThat(clientSeenRequests.size(), is(4)));
        assertThat(((ForceMergeRequest) clientSeenRequests.get(3)).indices().length, is(1));
    }

    public void testWithTinyRetentions() {
        final String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int numBackingIndices = 1;
        final int numFailureIndices = 1;
        final ProjectMetadata.Builder metadataBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
        final DataStreamLifecycle dataLifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .dataRetention(TimeValue.timeValueDays(1))
            .build();
        final DataStreamLifecycle failuresLifecycle = DataStreamLifecycle.failuresLifecycleBuilder()
            .dataRetention(TimeValue.timeValueHours(12))
            .build();
        final DataStream dataStream = createDataStream(
            metadataBuilder,
            dataStreamName,
            numBackingIndices,
            numFailureIndices,
            settings(IndexVersion.current()),
            dataLifecycle,
            failuresLifecycle,
            now
        );
        metadataBuilder.put(dataStream);

        final ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(metadataBuilder).build();
        dataStreamLifecycleService.run(state);

        assertThat(clientSeenRequests, hasSize(2));
        assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
        assertThat(clientSeenRequests.get(1), instanceOf(RolloverRequest.class));

        // test main data stream max_age
        final RolloverRequest rolloverRequest = (RolloverRequest) clientSeenRequests.get(0);
        assertThat(rolloverRequest.getRolloverTarget(), is(dataStreamName));
        final RolloverConditions conditions = rolloverRequest.getConditions();
        assertThat(conditions.getMaxAge(), equalTo(TimeValue.timeValueHours(1))); // 1 day retention -> 1h max_age

        // test failure data stream max_age
        final RolloverRequest rolloverFailureIndexRequest = (RolloverRequest) clientSeenRequests.get(1);
        assertThat(
            rolloverFailureIndexRequest.getRolloverTarget(),
            is(IndexNameExpressionResolver.combineSelector(dataStreamName, IndexComponentSelector.FAILURES))
        );

        final RolloverConditions failureStoreConditions = rolloverFailureIndexRequest.getConditions();
        assertThat(failureStoreConditions.getMaxAge(), equalTo(TimeValue.timeValueHours(1))); // 12h retention -> 1h max_age
    }

    public void testTimeSeriesIndicesStillWithinTimeBounds() {
        Instant currentTime = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        // These ranges are on the edge of each other temporal boundaries.
        Instant start1 = currentTime.minus(6, ChronoUnit.HOURS);
        Instant end1 = currentTime.minus(4, ChronoUnit.HOURS);
        Instant start2 = currentTime.minus(4, ChronoUnit.HOURS);
        Instant end2 = currentTime.plus(2, ChronoUnit.HOURS);
        Instant start3 = currentTime.plus(2, ChronoUnit.HOURS);
        Instant end3 = currentTime.plus(4, ChronoUnit.HOURS);

        final var projectId = randomProjectIdOrDefault();
        String dataStreamName = "logs_my-app_prod";
        var clusterState = DataStreamTestHelper.getClusterStateWithDataStream(
            projectId,
            dataStreamName,
            List.of(Tuple.tuple(start1, end1), Tuple.tuple(start2, end2), Tuple.tuple(start3, end3))
        );
        final var project = clusterState.metadata().getProject(projectId);
        DataStream dataStream = project.dataStreams().get(dataStreamName);

        {
            // test for an index for which `now` is outside its time bounds
            Index firstGenIndex = dataStream.getIndices().get(0);
            Set<Index> indices = DataStreamLifecycleService.timeSeriesIndicesStillWithinTimeBounds(
                // the end_time for the first generation has lapsed
                project,
                List.of(firstGenIndex),
                currentTime::toEpochMilli
            );
            assertThat(indices.size(), is(0));
        }

        {
            Set<Index> indices = DataStreamLifecycleService.timeSeriesIndicesStillWithinTimeBounds(
                // the end_time for the first generation has lapsed, but the other 2 generations are still within bounds
                project,
                dataStream.getIndices(),
                currentTime::toEpochMilli
            );
            assertThat(indices.size(), is(2));
            assertThat(indices, containsInAnyOrder(dataStream.getIndices().get(1), dataStream.getIndices().get(2)));
        }

        {
            // non time_series indices are not within time bounds (they don't have any)
            IndexMetadata indexMeta = IndexMetadata.builder(randomAlphaOfLengthBetween(10, 30))
                .settings(indexSettings(1, 1).put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current()))
                .build();

            ProjectMetadata newProject = ProjectMetadata.builder(project).put(indexMeta, true).build();

            Set<Index> indices = DataStreamLifecycleService.timeSeriesIndicesStillWithinTimeBounds(
                newProject,
                List.of(indexMeta.getIndex()),
                currentTime::toEpochMilli
            );
            assertThat(indices.size(), is(0));
        }
    }

    public void testTrackingTimeStats() {
        AtomicLong now = new AtomicLong(0);
        long delta = randomLongBetween(10, 10000);
        DataStreamLifecycleErrorStore errorStore = new DataStreamLifecycleErrorStore(() -> Clock.systemUTC().millis());
        DataStreamLifecycleService service = new DataStreamLifecycleService(
            Settings.EMPTY,
            getTransportRequestsRecordingClient(),
            clusterService,
            Clock.systemUTC(),
            threadPool,
            () -> now.getAndAdd(delta),
            errorStore,
            mock(AllocationService.class),
            new DataStreamLifecycleHealthInfoPublisher(Settings.EMPTY, getTransportRequestsRecordingClient(), clusterService, errorStore),
            globalRetentionSettings
        );
        assertThat(service.getLastRunDuration(), is(nullValue()));
        assertThat(service.getTimeBetweenStarts(), is(nullValue()));

        service.run(ClusterState.EMPTY_STATE);
        assertThat(service.getLastRunDuration(), is(delta));
        assertThat(service.getTimeBetweenStarts(), is(nullValue()));

        service.run(ClusterState.EMPTY_STATE);
        assertThat(service.getLastRunDuration(), is(delta));
        assertThat(service.getTimeBetweenStarts(), is(2 * delta));
    }

    public void testTargetIndices() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        int numFailureIndices = 2;
        int mutationBranch = randomIntBetween(0, 2);
        DataStreamOptions dataStreamOptions = switch (mutationBranch) {
            case 0 -> DataStreamOptions.EMPTY;
            case 1 -> DataStreamOptions.FAILURE_STORE_ENABLED;
            case 2 -> DataStreamOptions.FAILURE_STORE_DISABLED;
            default -> throw new IllegalStateException("Unexpected value: " + mutationBranch);
        };
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            numFailureIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE,
            null,
            now
        ).copy().setDataStreamOptions(dataStreamOptions).build(); // failure store is managed even when disabled
        builder.put(dataStream);
        ProjectMetadata project = builder.build();
        Set<Index> indicesToExclude = Set.of(dataStream.getIndices().get(0), dataStream.getFailureIndices().get(0));
        List<Index> targetBackingIndicesOnly = DataStreamLifecycleService.getTargetIndices(
            dataStream,
            indicesToExclude,
            project::index,
            false
        );
        assertThat(targetBackingIndicesOnly, equalTo(dataStream.getIndices().subList(1, 3)));
        List<Index> targetIndices = DataStreamLifecycleService.getTargetIndices(dataStream, indicesToExclude, project::index, true);
        assertThat(
            targetIndices,
            equalTo(List.of(dataStream.getIndices().get(1), dataStream.getIndices().get(2), dataStream.getFailureIndices().get(1)))
        );
    }

    public void testFailureStoreIsManagedEvenWhenDisabled() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 1;
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            2,
            settings(IndexVersion.current()),
            null,
            null,
            now
        ).copy()
            .setDataStreamOptions(
                new DataStreamOptions(
                    DataStreamFailureStore.builder()
                        .enabled(false)
                        .lifecycle(DataStreamLifecycle.failuresLifecycleBuilder().dataRetention(TimeValue.ZERO).build())
                        .build()
                )
            )
            .build(); // failure store is managed even when disabled
        builder.put(dataStream);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).build();

        dataStreamLifecycleService.run(state);
        assertThat(clientSeenRequests.size(), is(2));
        RolloverRequest rolloverFailureIndexRequest = (RolloverRequest) clientSeenRequests.get(0);
        assertThat(
            rolloverFailureIndexRequest.getRolloverTarget(),
            is(IndexNameExpressionResolver.combineSelector(dataStreamName, IndexComponentSelector.FAILURES))
        );
        assertThat(((DeleteIndexRequest) clientSeenRequests.get(1)).indices()[0], is(dataStream.getFailureIndices().get(0).getName()));
    }

    public void testMaybeExecuteRetentionSuccessfulDownsampledIndex() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final var projectId = randomProjectIdOrDefault();
        ClusterState state = downsampleSetup(projectId, dataStreamName, SUCCESS);
        final var project = state.metadata().getProject(projectId);
        DataStream dataStream = project.dataStreams().get(dataStreamName);
        String firstGenIndexName = dataStream.getIndices().getFirst().getName();
        TimeValue dataRetention = dataStream.getDataLifecycle().dataRetention();

        // Executing the method to be tested:
        Set<Index> indicesToBeRemoved = dataStreamLifecycleService.maybeExecuteRetention(
            project,
            dataStream,
            dataRetention,
            null,
            Set.of()
        );
        assertThat(indicesToBeRemoved, contains(project.index(firstGenIndexName).getIndex()));
    }

    public void testMaybeExecuteRetentionDownsampledIndexInProgress() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final var projectId = randomProjectIdOrDefault();
        ClusterState state = downsampleSetup(projectId, dataStreamName, STARTED);
        final var project = state.metadata().getProject(projectId);
        DataStream dataStream = project.dataStreams().get(dataStreamName);
        TimeValue dataRetention = dataStream.getDataLifecycle().dataRetention();

        // Executing the method to be tested:
        Set<Index> indicesToBeRemoved = dataStreamLifecycleService.maybeExecuteRetention(
            project,
            dataStream,
            dataRetention,
            null,
            Set.of()
        );
        assertThat(indicesToBeRemoved, empty());
    }

    public void testMaybeExecuteRetentionDownsampledUnknown() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final var projectId = randomProjectIdOrDefault();
        ClusterState state = downsampleSetup(projectId, dataStreamName, UNKNOWN);
        final var project = state.metadata().getProject(projectId);
        DataStream dataStream = project.dataStreams().get(dataStreamName);
        String firstGenIndexName = dataStream.getIndices().getFirst().getName();
        TimeValue dataRetention = dataStream.getDataLifecycle().dataRetention();

        // Executing the method to be tested:
        Set<Index> indicesToBeRemoved = dataStreamLifecycleService.maybeExecuteRetention(
            project,
            dataStream,
            dataRetention,
            null,
            Set.of()
        );
        assertThat(indicesToBeRemoved, contains(project.index(firstGenIndexName).getIndex()));
    }

    private static ForceMergeRequestWrapper copyForceMergeRequestWrapperRequest(ForceMergeRequestWrapper original) {
        return new ForceMergeRequestWrapper(original);
    }

    private static ForceMergeRequestWrapper mutateForceMergeRequestWrapper(ForceMergeRequestWrapper original) {
        switch (randomIntBetween(0, 4)) {
            case 0 -> {
                ForceMergeRequestWrapper copy = copyForceMergeRequestWrapperRequest(original);
                String[] originalIndices = original.indices();
                int changedIndexIndex;
                if (originalIndices.length > 0) {
                    changedIndexIndex = randomIntBetween(0, originalIndices.length - 1);
                } else {
                    originalIndices = new String[1];
                    changedIndexIndex = 0;
                }
                String[] newIndices = new String[originalIndices.length];
                System.arraycopy(originalIndices, 0, newIndices, 0, originalIndices.length);
                newIndices[changedIndexIndex] = randomAlphaOfLength(40);
                copy.indices(newIndices);
                return copy;
            }
            case 1 -> {
                ForceMergeRequestWrapper copy = copyForceMergeRequestWrapperRequest(original);
                copy.onlyExpungeDeletes(original.onlyExpungeDeletes() == false);
                return copy;
            }
            case 2 -> {
                ForceMergeRequestWrapper copy = copyForceMergeRequestWrapperRequest(original);
                copy.flush(original.flush() == false);
                return copy;
            }
            case 3 -> {
                ForceMergeRequestWrapper copy = copyForceMergeRequestWrapperRequest(original);
                copy.maxNumSegments(original.maxNumSegments() + 1);
                return copy;
            }
            case 4 -> {
                ForceMergeRequestWrapper copy = copyForceMergeRequestWrapperRequest(original);
                copy.setRequestId(original.getRequestId() + 1);
                return copy;
            }
            default -> throw new AssertionError("Can't get here");
        }
    }

    public void testFormatExecutionTimeMilliseconds() {
        assertThat(DataStreamLifecycleService.formatExecutionTime(500), equalTo("500ms/500ms"));
    }

    public void testFormatExecutionTimeSeconds() {
        assertThat(DataStreamLifecycleService.formatExecutionTime(1525), equalTo("1525ms/1.5s"));
    }

    public void testFormatExecutionTimeMinutes() {
        assertThat(DataStreamLifecycleService.formatExecutionTime(90000), equalTo("90000ms/1.5m"));
    }

    public void testFormatExecutionTimeHours() {
        assertThat(DataStreamLifecycleService.formatExecutionTime(5400000), equalTo("5400000ms/1.5h"));
    }

    public void testIndexMarkedForFrozen() {
        IndexMetadata plainIndex = IndexMetadata.builder("foo")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexMetadata indexWithOtherCustom = IndexMetadata.builder("foo")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putCustom(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, Map.of("foo", "bar"))
            .build();
        IndexMetadata indexWithFrozenCustom = IndexMetadata.builder("foo")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putCustom(
                LIFECYCLE_CUSTOM_INDEX_METADATA_KEY,
                Map.of(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY, "my-repo")
            )
            .build();

        assertFalse(DataStreamLifecycleService.indexMarkedForFrozen(plainIndex));
        assertFalse(DataStreamLifecycleService.indexMarkedForFrozen(indexWithOtherCustom));
        assertTrue(DataStreamLifecycleService.indexMarkedForFrozen(indexWithFrozenCustom));
    }

    public void testNonWriteIndexWithLifecycleSkipIsNotDeleted() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            3,
            settings(IndexVersion.current()),
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.ZERO).build(),
            now
        );
        builder.put(dataStream);
        final var project = builder.build();

        ProjectMetadata.Builder newBuilder = ProjectMetadata.builder(project);
        Index firstBackingIndex = dataStream.getIndices().getFirst();
        IndexMetadata indexMetadata = project.index(firstBackingIndex);
        newBuilder.put(
            IndexMetadata.builder(indexMetadata)
                .settings(Settings.builder().put(indexMetadata.getSettings()).put(IndexMetadata.LIFECYCLE_SKIP_SETTING.getKey(), true))
        );
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(newBuilder).build();

        dataStreamLifecycleService.run(state);

        // Rollover the write index; only backing index 2 is deleted — the skip-flagged backing index 1 is left alone.
        assertThat(clientSeenRequests.size(), is(2));
        assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
        assertThat(clientSeenRequests.get(1), instanceOf(DeleteIndexRequest.class));
        assertThat(
            ((DeleteIndexRequest) clientSeenRequests.get(1)).indices()[0],
            DataStreamTestHelper.backingIndexEqualTo(dataStreamName, 2)
        );
    }

    public void testWriteIndexWithLifecycleSkipIsNotRolledOver() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            3,
            settings(IndexVersion.current()),
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.ZERO).build(),
            now
        );
        builder.put(dataStream);
        final var project = builder.build();

        ProjectMetadata.Builder newBuilder = ProjectMetadata.builder(project);
        Index writeIndex = dataStream.getWriteIndex();
        IndexMetadata indexMetadata = project.index(writeIndex);
        newBuilder.put(
            IndexMetadata.builder(indexMetadata)
                .settings(Settings.builder().put(indexMetadata.getSettings()).put(IndexMetadata.LIFECYCLE_SKIP_SETTING.getKey(), true))
        );
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(newBuilder).build();

        dataStreamLifecycleService.run(state);

        // Both non-write backing indices are deleted; the skip-flagged write index is not rolled over.
        assertThat(clientSeenRequests.size(), is(2));
        assertThat(clientSeenRequests.get(0), instanceOf(DeleteIndexRequest.class));
        assertThat(clientSeenRequests.get(1), instanceOf(DeleteIndexRequest.class));
    }

    public void testFailureStoreIndexWithLifecycleSkipIsNotDeleted() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStreamLifecycle zeroRetentionFailuresLifecycle = DataStreamLifecycle.failuresLifecycleBuilder()
            .dataRetention(TimeValue.ZERO)
            .build();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            1,
            2,
            settings(IndexVersion.current()),
            null,
            zeroRetentionFailuresLifecycle,
            now
        );
        builder.put(dataStream);
        final var project = builder.build();

        ProjectMetadata.Builder newBuilder = ProjectMetadata.builder(project);
        Index skippedFailureIndex = dataStream.getFailureIndices().get(0);
        IndexMetadata failureIndexMetadata = project.index(skippedFailureIndex);
        newBuilder.put(
            IndexMetadata.builder(failureIndexMetadata)
                .settings(
                    Settings.builder().put(failureIndexMetadata.getSettings()).put(IndexMetadata.LIFECYCLE_SKIP_SETTING.getKey(), true)
                )
        );
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(newBuilder).build();

        dataStreamLifecycleService.run(state);

        // The failure write index is rolled over; the skip-flagged non-write failure index is not deleted.
        assertThat(clientSeenRequests.size(), is(1));
        assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
    }

    @SuppressWarnings("unchecked")
    public void testLifecycleSkipPreventsForceMerge() throws Exception {
        clientDelegate = (action, request, listener) -> {
            if (action.name().equals("indices:admin/forcemerge")) {
                listener.onResponse(new BroadcastResponse(5, 5, 0, List.of()));
            }
        };
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            3,
            settings(IndexVersion.current()).put(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(), ONE_HUNDRED_MB)
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING.getKey(), TARGET_MERGE_FACTOR_VALUE),
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.MAX_VALUE).build(),
            now
        );
        builder.put(dataStream);
        final var project = builder.build();

        ProjectMetadata.Builder newBuilder = ProjectMetadata.builder(project);
        Index skippedIndex = dataStream.getIndices().get(0);
        IndexMetadata skippedIndexMetadata = project.index(skippedIndex);
        newBuilder.put(
            IndexMetadata.builder(skippedIndexMetadata)
                .settings(
                    Settings.builder().put(skippedIndexMetadata.getSettings()).put(IndexMetadata.LIFECYCLE_SKIP_SETTING.getKey(), true)
                )
        );

        String nodeId = "localNode";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        nodesBuilder.masterNodeId(nodeId);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(newBuilder).nodes(nodesBuilder).build();
        setState(clusterService, state);
        dataStreamLifecycleService.run(clusterService.state());

        // Only the non-skipped non-write index (generation 2) is force-merged; the skip-flagged index (generation 1) is not.
        Index expectedForceMergedIndex = dataStream.getIndices().get(1);
        assertBusy(() -> assertThat(clientSeenRequests.size(), is(2)), 30, TimeUnit.SECONDS);
        assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
        ForceMergeRequest forceMergeRequest = (ForceMergeRequest) clientSeenRequests.get(1);
        assertThat(forceMergeRequest.indices()[0], is(expectedForceMergedIndex.getName()));
    }

    public void testGatheringCandidatesForFrozen() {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        int backingIndices = randomIntBetween(3, 10);
        DataStream dataStreamWithNoFrozen = createDataStream(
            builder,
            "my-datastream",
            backingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE,
            now
        );
        DataStream dataStreamWithFrozen = createDataStream(
            builder,
            "my-datastream-with-frozen",
            backingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.dataLifecycleBuilder().enabled(true).frozenAfter(TimeValue.timeValueMinutes(1)).build(),
            now
        );
        builder.put(dataStreamWithNoFrozen);
        builder.put(dataStreamWithFrozen);
        ProjectMetadata projectMetadata = builder.build();

        assertThat(
            DataStreamLifecycleService.candidatesForFrozen(projectMetadata, dataStreamWithNoFrozen, () -> now, List.of()),
            equalTo(Set.of())
        );
        assertThat(
            DataStreamLifecycleService.candidatesForFrozen(
                projectMetadata,
                dataStreamWithNoFrozen,
                () -> now,
                dataStreamWithNoFrozen.getIndices()
            ),
            equalTo(Set.of())
        );
        assertThat(
            DataStreamLifecycleService.candidatesForFrozen(projectMetadata, dataStreamWithFrozen, () -> now, List.of()),
            equalTo(Set.of())
        );
        assertThat(
            DataStreamLifecycleService.candidatesForFrozen(
                projectMetadata,
                dataStreamWithFrozen,
                () -> now,
                dataStreamWithFrozen.getIndices()
            ),
            equalTo(Set.of())
        );
        assertThat(
            DataStreamLifecycleService.candidatesForFrozen(
                projectMetadata,
                dataStreamWithFrozen,
                () -> now + TimeValue.timeValueDays(2).millis(),
                List.of()
            ),
            equalTo(Set.of())
        );
        Set<Index> candidates = DataStreamLifecycleService.candidatesForFrozen(
            projectMetadata,
            dataStreamWithFrozen,
            () -> now + TimeValue.timeValueMinutes(2).millis(),
            dataStreamWithFrozen.getIndices()
        );
        assertThat(
            new TreeSet<>(candidates.stream().map(Index::getName).toList()),
            equalTo(
                new TreeSet<>(
                    dataStreamWithFrozen.getIndices()
                        .subList(0, (int) dataStreamWithFrozen.getGeneration() - 1)
                        .stream()
                        .map(Index::getName)
                        .toList()
                )
            )
        );
    }

    public void testGatheringCandidatesForFrozenSkipsDlmCreatedIndices() {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        int backingIndices = randomIntBetween(3, 10);
        DataStream dataStreamWithFrozen = createDataStream(
            builder,
            "my-datastream-with-frozen",
            backingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.dataLifecycleBuilder().enabled(true).frozenAfter(TimeValue.timeValueMinutes(1)).build(),
            now
        );
        builder.put(dataStreamWithFrozen);
        ProjectMetadata projectMetadata = builder.build();

        // Pick the first aged (non-write-index) backing index and mark it as DLM-created
        Index dlmCreatedIndex = dataStreamWithFrozen.getIndices().getFirst();
        IndexMetadata originalMeta = projectMetadata.index(dlmCreatedIndex);
        IndexMetadata dlmCreatedMeta = IndexMetadata.builder(originalMeta)
            .settings(
                Settings.builder().put(originalMeta.getSettings()).put(DataStreamLifecycleService.DLM_CREATED_SETTING_KEY, true).build()
            )
            .build();
        ProjectMetadata updatedProjectMetadata = ProjectMetadata.builder(projectMetadata).put(dlmCreatedMeta, false).build();

        long nowPastFrozenAfter = now + TimeValue.timeValueMinutes(2).millis();

        Set<Index> candidates = DataStreamLifecycleService.candidatesForFrozen(
            updatedProjectMetadata,
            dataStreamWithFrozen,
            () -> nowPastFrozenAfter,
            dataStreamWithFrozen.getIndices()
        );

        assertFalse(
            "DLM-created index should not be a frozen candidate",
            candidates.stream().anyMatch(i -> i.getName().equals(dlmCreatedIndex.getName()))
        );
        assertThat(
            new TreeSet<>(candidates.stream().map(Index::getName).toList()),
            equalTo(
                new TreeSet<>(
                    dataStreamWithFrozen.getIndices()
                        .subList(0, (int) dataStreamWithFrozen.getGeneration() - 1)
                        .stream()
                        .map(Index::getName)
                        .filter(name -> name.equals(dlmCreatedIndex.getName()) == false)
                        .toList()
                )
            )
        );
    }
}
