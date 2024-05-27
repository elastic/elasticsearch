/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.datastreams.lifecycle.ErrorEntry;
import org.elasticsearch.action.downsample.DownsampleAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamFactoryRetention;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionResolver;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle.Downsampling;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle.Downsampling.Round;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.DataStreamFeatures;
import org.elasticsearch.datastreams.lifecycle.health.DataStreamLifecycleHealthInfoPublisher;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.DownsampleTaskStatus.STARTED;
import static org.elasticsearch.cluster.metadata.IndexMetadata.DownsampleTaskStatus.SUCCESS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.DownsampleTaskStatus.UNKNOWN;
import static org.elasticsearch.datastreams.DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleFixtures.createDataStream;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.DATA_STREAM_MERGE_POLICY_TARGET_FACTOR_SETTING;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.DATA_STREAM_MERGE_POLICY_TARGET_FLOOR_SEGMENT_SETTING;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.DATA_STREAM_SIGNALLING_ERROR_RETRY_INTERVAL_SETTING;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.DOWNSAMPLED_INDEX_PREFIX;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.ONE_HUNDRED_MB;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.TARGET_MERGE_FACTOR_VALUE;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class DataStreamLifecycleServiceTests extends ESTestCase {

    private long now;
    private ThreadPool threadPool;
    private DataStreamLifecycleService dataStreamLifecycleService;
    private List<TransportRequest> clientSeenRequests;
    private DoExecuteDelegate clientDelegate;
    private ClusterService clusterService;
    private final DataStreamGlobalRetentionResolver globalRetentionResolver = new DataStreamGlobalRetentionResolver(
        DataStreamFactoryRetention.emptyFactoryRetention()
    );

    @Before
    public void setupServices() {
        threadPool = new TestThreadPool(getTestName());
        Set<Setting<?>> builtInClusterSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        builtInClusterSettings.add(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL_SETTING);
        builtInClusterSettings.add(DATA_STREAM_MERGE_POLICY_TARGET_FLOOR_SEGMENT_SETTING);
        builtInClusterSettings.add(DATA_STREAM_MERGE_POLICY_TARGET_FACTOR_SETTING);
        builtInClusterSettings.add(DATA_STREAM_SIGNALLING_ERROR_RETRY_INTERVAL_SETTING);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, builtInClusterSettings);
        clusterService = createClusterService(threadPool, clusterSettings);

        now = System.currentTimeMillis();
        Clock clock = Clock.fixed(Instant.ofEpochMilli(now), ZoneId.of(randomFrom(ZoneId.getAvailableZoneIds())));
        clientSeenRequests = new CopyOnWriteArrayList<>();

        final Client client = getTransportRequestsRecordingClient();
        AllocationService allocationService = new AllocationService(
            new AllocationDeciders(
                new HashSet<>(
                    Arrays.asList(new SameShardAllocationDecider(clusterSettings), new ReplicaAfterPrimaryActiveAllocationDecider())
                )
            ),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
        DataStreamLifecycleErrorStore errorStore = new DataStreamLifecycleErrorStore(() -> now);
        dataStreamLifecycleService = new DataStreamLifecycleService(
            Settings.EMPTY,
            client,
            clusterService,
            clock,
            threadPool,
            () -> now,
            errorStore,
            allocationService,
            new DataStreamLifecycleHealthInfoPublisher(
                Settings.EMPTY,
                client,
                clusterService,
                errorStore,
                new FeatureService(List.of(new DataStreamFeatures()))
            ),
            globalRetentionResolver
        );
        clientDelegate = null;
        dataStreamLifecycleService.init();
    }

    @After
    public void cleanup() {
        clientSeenRequests.clear();
        dataStreamLifecycleService.close();
        clusterService.close();
        threadPool.shutdownNow();
    }

    public void testOperationsExecutedOnce() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            2,
            settings(IndexVersion.current()),
            DataStreamLifecycle.newBuilder().dataRetention(0).build(),
            now
        );
        builder.put(dataStream);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();

        dataStreamLifecycleService.run(state);
        assertThat(clientSeenRequests.size(), is(5));
        assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
        RolloverRequest rolloverBackingIndexRequest = (RolloverRequest) clientSeenRequests.get(0);
        assertThat(rolloverBackingIndexRequest.getRolloverTarget(), is(dataStreamName));
        assertThat(
            rolloverBackingIndexRequest.indicesOptions().failureStoreOptions(),
            equalTo(new IndicesOptions.FailureStoreOptions(true, false))
        );
        assertThat(clientSeenRequests.get(1), instanceOf(RolloverRequest.class));
        RolloverRequest rolloverFailureIndexRequest = (RolloverRequest) clientSeenRequests.get(1);
        assertThat(rolloverFailureIndexRequest.getRolloverTarget(), is(dataStreamName));
        assertThat(
            rolloverFailureIndexRequest.indicesOptions().failureStoreOptions(),
            equalTo(new IndicesOptions.FailureStoreOptions(false, true))
        );
        List<DeleteIndexRequest> deleteRequests = clientSeenRequests.subList(2, 5)
            .stream()
            .map(transportRequest -> (DeleteIndexRequest) transportRequest)
            .toList();
        assertThat(deleteRequests.get(0).indices()[0], is(dataStream.getIndices().get(0).getName()));
        assertThat(deleteRequests.get(1).indices()[0], is(dataStream.getIndices().get(1).getName()));
        assertThat(deleteRequests.get(2).indices()[0], is(dataStream.getFailureIndices().getIndices().get(0).getName()));

        // on the second run the rollover and delete requests should not execute anymore
        // i.e. the count should *remain* 1 for rollover and 2 for deletes
        dataStreamLifecycleService.run(state);
        assertThat(clientSeenRequests.size(), is(5));
    }

    public void testRetentionNotConfigured() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()),
            new DataStreamLifecycle(),
            now
        );
        builder.put(dataStream);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();
        dataStreamLifecycleService.run(state);
        assertThat(clientSeenRequests.size(), is(3));  // rollover the write index, and force merge the other two
        assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
    }

    public void testRetentionNotExecutedDueToAge() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        int numFailureIndices = 2;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            numFailureIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.newBuilder().dataRetention(TimeValue.timeValueDays(700)).build(),
            now
        );
        builder.put(dataStream);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();
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

        String dataStreamName = "logs_my-app_prod";
        var clusterState = DataStreamTestHelper.getClusterStateWithDataStream(
            dataStreamName,
            List.of(Tuple.tuple(start1, end1), Tuple.tuple(start2, end2), Tuple.tuple(start3, end3))
        );
        Metadata.Builder builder = Metadata.builder(clusterState.metadata());
        DataStream dataStream = builder.dataStream(dataStreamName);
        builder.put(
            dataStream.copy()
                .setName(dataStreamName)
                .setGeneration(dataStream.getGeneration() + 1)
                .setLifecycle(DataStreamLifecycle.newBuilder().dataRetention(0L).build())
                .build()
        );
        clusterState = ClusterState.builder(clusterState).metadata(builder).build();

        dataStreamLifecycleService.run(clusterState);
        assertThat(clientSeenRequests.size(), is(2)); // rollover the write index and one delete request for the index that's out of the
        // TS time bounds
        assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
        TransportRequest deleteIndexRequest = clientSeenRequests.get(1);
        assertThat(deleteIndexRequest, instanceOf(DeleteIndexRequest.class));
        // only the first generation index should be eligible for retention
        assertThat(((DeleteIndexRequest) deleteIndexRequest).indices(), is(new String[] { dataStream.getIndices().get(0).getName() }));
    }

    public void testRetentionSkippedWhilstDownsamplingInProgress() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.newBuilder().dataRetention(TimeValue.timeValueMillis(0)).build(),
            now
        );
        builder.put(dataStream);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();

        {
            Metadata metadata = state.metadata();
            Metadata.Builder metaBuilder = Metadata.builder(metadata);

            String firstBackingIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
            IndexMetadata indexMetadata = metadata.index(firstBackingIndex);
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
            metaBuilder.put(indexMetaBuilder);
            state = ClusterState.builder(ClusterName.DEFAULT).metadata(metaBuilder).build();

            dataStreamLifecycleService.run(state);
            assertThat(clientSeenRequests.size(), is(2)); // rollover the write index and delete the second generation
            assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
            assertThat(clientSeenRequests.get(1), instanceOf(DeleteIndexRequest.class));
            assertThat(
                ((DeleteIndexRequest) clientSeenRequests.get(1)).indices()[0],
                is(DataStream.getDefaultBackingIndexName(dataStreamName, 2))
            );
        }

        {
            // a lack of downsample status (i.e. the default `UNKNOWN`) must not prevent retention
            Metadata metadata = state.metadata();
            Metadata.Builder metaBuilder = Metadata.builder(metadata);

            String firstBackingIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
            IndexMetadata indexMetadata = metadata.index(firstBackingIndex);
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(indexMetadata);
            indexMetaBuilder.settings(
                Settings.builder().put(indexMetadata.getSettings()).putNull(IndexMetadata.INDEX_DOWNSAMPLE_STATUS_KEY)
            );
            metaBuilder.put(indexMetaBuilder);
            state = ClusterState.builder(ClusterName.DEFAULT).metadata(metaBuilder).build();

            dataStreamLifecycleService.run(state);
            assertThat(clientSeenRequests.size(), is(3)); // rollover the write index and delete the other two generations
            assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
            assertThat(clientSeenRequests.get(1), instanceOf(DeleteIndexRequest.class));
            assertThat(
                ((DeleteIndexRequest) clientSeenRequests.get(1)).indices()[0],
                is(DataStream.getDefaultBackingIndexName(dataStreamName, 2))
            );
            assertThat(clientSeenRequests.get(2), instanceOf(DeleteIndexRequest.class));
            assertThat(
                ((DeleteIndexRequest) clientSeenRequests.get(2)).indices()[0],
                is(DataStream.getDefaultBackingIndexName(dataStreamName, 1))
            );
        }
    }

    public void testIlmManagedIndicesAreSkipped() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            Settings.builder()
                .put(IndexMetadata.LIFECYCLE_NAME, "ILM_policy")
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()),
            DataStreamLifecycle.newBuilder().dataRetention(0).build(),
            now
        );
        builder.put(dataStream);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();
        dataStreamLifecycleService.run(state);
        assertThat(clientSeenRequests.isEmpty(), is(true));
    }

    public void testDataStreamsWithoutLifecycleAreSkipped() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
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

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();
        dataStreamLifecycleService.run(state);
        assertThat(clientSeenRequests.isEmpty(), is(true));
    }

    public void testDeletedIndicesAreRemovedFromTheErrorStore() throws IOException {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()),
            new DataStreamLifecycle(),
            now
        );
        builder.put(dataStream);
        String nodeId = "localNode";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        // we are the master node
        nodesBuilder.masterNodeId(nodeId);
        ClusterState previousState = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).nodes(nodesBuilder).build();

        // all backing indices are in the error store
        for (Index index : dataStream.getIndices()) {
            dataStreamLifecycleService.getErrorStore().recordError(index.getName(), new NullPointerException("bad"));
        }
        Index writeIndex = dataStream.getWriteIndex();
        // all indices but the write index are deleted
        List<Index> deletedIndices = dataStream.getIndices().stream().filter(index -> index.equals(writeIndex) == false).toList();

        ClusterState.Builder newStateBuilder = ClusterState.builder(previousState);
        newStateBuilder.stateUUID(UUIDs.randomBase64UUID());
        Metadata.Builder metaBuilder = Metadata.builder(previousState.metadata());
        for (Index index : deletedIndices) {
            metaBuilder.remove(index.getName());
            IndexGraveyard.Builder graveyardBuilder = IndexGraveyard.builder(metaBuilder.indexGraveyard());
            graveyardBuilder.addTombstone(index);
            metaBuilder.indexGraveyard(graveyardBuilder.build());
        }
        newStateBuilder.metadata(metaBuilder);
        ClusterState stateWithDeletedIndices = newStateBuilder.nodes(buildNodes(nodeId).masterNodeId(nodeId)).build();
        setState(clusterService, stateWithDeletedIndices);

        dataStreamLifecycleService.run(stateWithDeletedIndices);

        for (Index deletedIndex : deletedIndices) {
            assertThat(dataStreamLifecycleService.getErrorStore().getError(deletedIndex.getName()), nullValue());
        }
        // the value for the write index should still be in the error store
        assertThat(dataStreamLifecycleService.getErrorStore().getError(dataStream.getWriteIndex().getName()), notNullValue());
    }

    public void testErrorStoreIsClearedOnBackingIndexBecomingUnmanaged() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.newBuilder().dataRetention(TimeValue.timeValueDays(700)).build(),
            now
        );
        // all backing indices are in the error store
        for (Index index : dataStream.getIndices()) {
            dataStreamLifecycleService.getErrorStore().recordError(index.getName(), new NullPointerException("bad"));
        }
        builder.put(dataStream);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();

        Metadata metadata = state.metadata();
        Metadata.Builder metaBuilder = Metadata.builder(metadata);

        // update the backing indices to be ILM managed
        for (Index index : dataStream.getIndices()) {
            IndexMetadata indexMetadata = metadata.index(index);
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(indexMetadata);
            indexMetaBuilder.settings(Settings.builder().put(indexMetadata.getSettings()).put(IndexMetadata.LIFECYCLE_NAME, "ILM_policy"));
            metaBuilder.put(indexMetaBuilder.build(), true);
        }
        ClusterState updatedState = ClusterState.builder(state).metadata(metaBuilder).build();
        setState(clusterService, updatedState);

        dataStreamLifecycleService.run(updatedState);

        for (Index index : dataStream.getIndices()) {
            assertThat(dataStreamLifecycleService.getErrorStore().getError(index.getName()), nullValue());
        }
    }

    public void testBackingIndicesFromMultipleDataStreamsInErrorStore() {
        String ilmManagedDataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        Metadata.Builder builder = Metadata.builder();
        DataStream ilmManagedDataStream = createDataStream(
            builder,
            ilmManagedDataStreamName,
            3,
            settings(IndexVersion.current()),
            DataStreamLifecycle.newBuilder().dataRetention(TimeValue.timeValueDays(700)).build(),
            now
        );
        // all backing indices are in the error store
        for (Index index : ilmManagedDataStream.getIndices()) {
            dataStreamLifecycleService.getErrorStore().recordError(index.getName(), new NullPointerException("will be ILM managed soon"));
        }
        String dataStreamWithBackingIndicesInErrorState = randomAlphaOfLength(15).toLowerCase(Locale.ROOT);
        DataStream dslManagedDataStream = createDataStream(
            builder,
            dataStreamWithBackingIndicesInErrorState,
            5,
            settings(IndexVersion.current()),
            DataStreamLifecycle.newBuilder().dataRetention(TimeValue.timeValueDays(700)).build(),
            now
        );
        // put all backing indices in the error store
        for (Index index : dslManagedDataStream.getIndices()) {
            dataStreamLifecycleService.getErrorStore().recordError(index.getName(), new NullPointerException("dsl managed index"));
        }
        builder.put(ilmManagedDataStream);
        builder.put(dslManagedDataStream);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();

        Metadata metadata = state.metadata();
        Metadata.Builder metaBuilder = Metadata.builder(metadata);

        // update the backing indices to be ILM managed so they should be removed from the error store on the next DSL run
        for (Index index : ilmManagedDataStream.getIndices()) {
            IndexMetadata indexMetadata = metadata.index(index);
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(indexMetadata);
            indexMetaBuilder.settings(Settings.builder().put(indexMetadata.getSettings()).put(IndexMetadata.LIFECYCLE_NAME, "ILM_policy"));
            metaBuilder.put(indexMetaBuilder.build(), true);
        }
        ClusterState updatedState = ClusterState.builder(state).metadata(metaBuilder).build();
        setState(clusterService, updatedState);

        dataStreamLifecycleService.run(updatedState);

        for (Index index : dslManagedDataStream.getIndices()) {
            assertThat(dataStreamLifecycleService.getErrorStore().getError(index.getName()), notNullValue());
        }
        for (Index index : ilmManagedDataStream.getIndices()) {
            assertThat(dataStreamLifecycleService.getErrorStore().getError(index.getName()), nullValue());
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
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()).put(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(), ONE_HUNDRED_MB)
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING.getKey(), TARGET_MERGE_FACTOR_VALUE),
            DataStreamLifecycle.newBuilder().dataRetention(TimeValue.MAX_VALUE).build(),
            now
        );
        builder.put(dataStream);

        String nodeId = "localNode";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        // we are the master node
        nodesBuilder.masterNodeId(nodeId);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).nodes(nodesBuilder).build();
        setState(clusterService, state);
        dataStreamLifecycleService.run(clusterService.state());

        // There are 3 backing indices. One gets rolled over. The other two get force merged:
        assertBusy(() -> {
            assertThat(
                clusterService.state().metadata().index(dataStream.getIndices().get(0)).getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY),
                notNullValue()
            );
            assertThat(
                clusterService.state().metadata().index(dataStream.getIndices().get(1)).getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY),
                notNullValue()
            );
            assertThat(
                clusterService.state()
                    .metadata()
                    .index(dataStream.getIndices().get(0))
                    .getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY)
                    .get(FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY),
                notNullValue()
            );
            assertThat(
                clusterService.state()
                    .metadata()
                    .index(dataStream.getIndices().get(1))
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
        builder = Metadata.builder(clusterService.state().metadata()).put(newIndexMetadata, true);
        state = ClusterState.builder(clusterService.state()).metadata(builder).build();
        setState(clusterService, state);
        DataStream dataStream2 = dataStream.addBackingIndex(clusterService.state().metadata(), newIndexMetadata.getIndex());
        builder = Metadata.builder(clusterService.state().metadata());
        builder.put(dataStream2);
        state = ClusterState.builder(clusterService.state()).metadata(builder).build();
        setState(clusterService, state);
        dataStreamLifecycleService.run(clusterService.state());
        assertBusy(() -> { assertThat(clientSeenRequests.size(), is(4)); });
        assertThat(((ForceMergeRequest) clientSeenRequests.get(3)).indices().length, is(1));
        assertBusy(() -> {
            assertThat(
                clusterService.state().metadata().index(dataStream2.getIndices().get(2)).getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY),
                notNullValue()
            );
            assertThat(
                clusterService.state()
                    .metadata()
                    .index(dataStream2.getIndices().get(2))
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
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()).put(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(), ONE_HUNDRED_MB)
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING.getKey(), TARGET_MERGE_FACTOR_VALUE),
            DataStreamLifecycle.newBuilder().dataRetention(TimeValue.MAX_VALUE).build(),
            now
        );
        builder.put(dataStream);

        String nodeId = "localNode";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        // we are the master node
        nodesBuilder.masterNodeId(nodeId);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).nodes(nodesBuilder).build();
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
                assertThat(
                    clusterService.state()
                        .metadata()
                        .index(dataStream.getIndices().get(0))
                        .getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY),
                    nullValue()
                );
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
                assertThat(
                    clusterService.state()
                        .metadata()
                        .index(dataStream.getIndices().get(0))
                        .getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY),
                    nullValue()
                );
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
                assertThat(
                    clusterService.state()
                        .metadata()
                        .index(dataStream.getIndices().get(0))
                        .getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY),
                    nullValue()
                );
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
                assertThat(
                    clusterService.state()
                        .metadata()
                        .index(dataStream.getIndices().get(0))
                        .getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY),
                    notNullValue()
                );
                assertThat(
                    clusterService.state()
                        .metadata()
                        .index(dataStream.getIndices().get(1))
                        .getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY),
                    notNullValue()
                );
                assertThat(
                    clusterService.state()
                        .metadata()
                        .index(dataStream.getIndices().get(0))
                        .getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY)
                        .get(FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY),
                    notNullValue()
                );
                assertThat(
                    clusterService.state()
                        .metadata()
                        .index(dataStream.getIndices().get(1))
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
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.newBuilder().dataRetention(TimeValue.MAX_VALUE).build(),
            now
        );
        builder.put(dataStream);
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
        builder = Metadata.builder(clusterService.state().metadata()).put(newIndexMetadata, true);
        ClusterState state = ClusterState.builder(clusterService.state()).metadata(builder).build();
        setState(clusterService, state);
        DataStream dataStream2 = dataStream.addBackingIndex(clusterService.state().metadata(), newIndexMetadata.getIndex());
        builder = Metadata.builder(clusterService.state().metadata());
        builder.put(dataStream2);
        state = ClusterState.builder(clusterService.state()).metadata(builder).build();
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
        String targetIndex = randomAlphaOfLength(20);
        DataStreamLifecycleService.UpdateForceMergeCompleteTask task = new DataStreamLifecycleService.UpdateForceMergeCompleteTask(
            listener,
            targetIndex,
            threadPool
        );
        {
            Exception exception = new RuntimeException("task failed");
            task.onFailure(exception);
            assertThat(failureCount.get(), equalTo(1));
            assertThat(onResponseCount.get(), equalTo(0));
            assertThat(failure.get(), equalTo(exception));
            ClusterState clusterState = createClusterState(targetIndex, null);
            ClusterState newClusterState = task.execute(clusterState);
            IndexMetadata indexMetadata = newClusterState.metadata().index(targetIndex);
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
            ClusterState clusterState = createClusterState(targetIndex, preExistingDataStreamLifecycleCustomMetadata);
            ClusterState newClusterState = task.execute(clusterState);
            IndexMetadata indexMetadata = newClusterState.metadata().index(targetIndex);
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
            new DataStreamLifecycleService.ForceMergeRequestWrapper(originalRequest),
            DataStreamLifecycleServiceTests::copyForceMergeRequestWrapperRequest,
            DataStreamLifecycleServiceTests::mutateForceMergeRequestWrapper
        );
    }

    public void testMergePolicySettingsAreConfiguredBeforeForcemerge() throws Exception {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.newBuilder().dataRetention(TimeValue.MAX_VALUE).build(),
            now
        );
        builder.put(dataStream);

        String nodeId = "localNode";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        // we are the master node
        nodesBuilder.masterNodeId(nodeId);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).nodes(nodesBuilder).build();
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
        builder = Metadata.builder(clusterService.state().metadata()).put(newIndexMetadata, true);
        state = ClusterState.builder(clusterService.state()).metadata(builder).build();
        setState(clusterService, state);
        DataStream modifiedDataStream = dataStream.addBackingIndex(clusterService.state().metadata(), newIndexMetadata.getIndex());
        builder = Metadata.builder(clusterService.state().metadata());
        builder.put(modifiedDataStream);
        state = ClusterState.builder(clusterService.state()).metadata(builder).build();
        setState(clusterService, state);
        dataStreamLifecycleService.run(clusterService.state());
        assertBusy(() -> assertThat(clientSeenRequests.size(), is(4)));
        assertThat(((ForceMergeRequest) clientSeenRequests.get(3)).indices().length, is(1));
    }

    public void testDownsampling() throws Exception {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 2;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()).put(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(), ONE_HUNDRED_MB)
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING.getKey(), TARGET_MERGE_FACTOR_VALUE)
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .put("index.routing_path", "@timestamp"),
            DataStreamLifecycle.newBuilder()
                .downsampling(
                    new Downsampling(
                        List.of(new Round(TimeValue.timeValueMillis(0), new DownsampleConfig(new DateHistogramInterval("5m"))))
                    )
                )
                .dataRetention(TimeValue.MAX_VALUE)
                .build(),
            now
        );
        builder.put(dataStream);

        String nodeId = "localNode";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        // we are the master node
        nodesBuilder.masterNodeId(nodeId);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).nodes(nodesBuilder).build();
        setState(clusterService, state);
        String firstGenIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        Index firstGenIndex = clusterService.state().metadata().index(firstGenIndexName).getIndex();
        Set<Index> affectedIndices = dataStreamLifecycleService.maybeExecuteDownsampling(
            clusterService.state(),
            dataStream,
            List.of(firstGenIndex)
        );

        assertThat(affectedIndices, is(Set.of(firstGenIndex)));
        // we first mark the index as read-only
        assertThat(clientSeenRequests.size(), is(1));
        assertThat(clientSeenRequests.get(0), instanceOf(AddIndexBlockRequest.class));

        {
            // we do the read-only bit ourselves as it's unit-testing
            Metadata.Builder metadataBuilder = Metadata.builder(state.metadata());
            IndexMetadata indexMetadata = metadataBuilder.getSafe(firstGenIndex);
            Settings updatedSettings = Settings.builder().put(indexMetadata.getSettings()).put(WRITE.settingName(), true).build();
            metadataBuilder.put(
                IndexMetadata.builder(indexMetadata).settings(updatedSettings).settingsVersion(indexMetadata.getSettingsVersion() + 1)
            );

            ClusterBlock indexBlock = MetadataIndexStateService.createUUIDBasedBlock(WRITE.getBlock());
            ClusterBlocks.Builder blocks = ClusterBlocks.builder(state.blocks());
            blocks.addIndexBlock(firstGenIndexName, indexBlock);

            state = ClusterState.builder(state).blocks(blocks).metadata(metadataBuilder).build();
            setState(clusterService, state);
        }

        // on the next run downsampling should be triggered
        affectedIndices = dataStreamLifecycleService.maybeExecuteDownsampling(clusterService.state(), dataStream, List.of(firstGenIndex));
        assertThat(affectedIndices, is(Set.of(firstGenIndex)));
        assertThat(clientSeenRequests.size(), is(2));
        assertThat(clientSeenRequests.get(1), instanceOf(DownsampleAction.Request.class));

        String downsampleIndexName = DownsampleConfig.generateDownsampleIndexName(
            DOWNSAMPLED_INDEX_PREFIX,
            state.metadata().index(firstGenIndex),
            new DateHistogramInterval("5m")
        );
        {
            // let's simulate the in-progress downsampling
            IndexMetadata firstGenMetadata = state.metadata().index(firstGenIndexName);
            Metadata.Builder metadataBuilder = Metadata.builder(state.metadata());

            metadataBuilder.put(
                IndexMetadata.builder(downsampleIndexName)
                    .settings(
                        Settings.builder()
                            .put(firstGenMetadata.getSettings())
                            .put(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_UUID.getKey(), firstGenIndex.getUUID())
                            .put(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey(), STARTED)
                    )
                    .numberOfReplicas(0)
                    .numberOfShards(1)
            );
            state = ClusterState.builder(state).metadata(metadataBuilder).build();
            setState(clusterService, state);
        }

        // on the next run downsampling nothing should be triggered as downsampling is in progress (i.e. the STATUS is STARTED)
        affectedIndices = dataStreamLifecycleService.maybeExecuteDownsampling(clusterService.state(), dataStream, List.of(firstGenIndex));
        assertThat(affectedIndices, is(Set.of(firstGenIndex)));
        // still only 2 witnessed requests, nothing extra
        assertThat(clientSeenRequests.size(), is(2));

        {
            // mark the downsample operation as complete
            IndexMetadata firstGenMetadata = state.metadata().index(firstGenIndexName);
            Metadata.Builder metadataBuilder = Metadata.builder(state.metadata());

            metadataBuilder.put(
                IndexMetadata.builder(downsampleIndexName)
                    .settings(
                        Settings.builder()
                            .put(firstGenMetadata.getSettings())
                            .put(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME_KEY, firstGenIndexName)
                            .put(IndexMetadata.INDEX_DOWNSAMPLE_ORIGIN_NAME_KEY, firstGenIndexName)
                            .put(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey(), SUCCESS)
                    )
                    .numberOfReplicas(0)
                    .numberOfShards(1)
            );
            state = ClusterState.builder(state).metadata(metadataBuilder).build();
            setState(clusterService, state);
        }

        // on this run, as downsampling is complete we expect to trigger the {@link
        // org.elasticsearch.datastreams.lifecycle.downsampling.DeleteSourceAndAddDownsampleToDS}
        // cluster service task and delete the source index whilst adding the downsample index in the data stream
        affectedIndices = dataStreamLifecycleService.maybeExecuteDownsampling(clusterService.state(), dataStream, List.of(firstGenIndex));
        assertThat(affectedIndices, is(Set.of(firstGenIndex)));
        assertBusy(() -> {
            ClusterState newState = clusterService.state();
            IndexAbstraction downsample = newState.metadata().getIndicesLookup().get(downsampleIndexName);
            // the downsample index must be part of the data stream
            assertThat(downsample.getParentDataStream(), is(notNullValue()));
            assertThat(downsample.getParentDataStream().getName(), is(dataStreamName));
            // the source index was deleted
            IndexAbstraction sourceIndexAbstraction = newState.metadata().getIndicesLookup().get(firstGenIndexName);
            assertThat(sourceIndexAbstraction, is(nullValue()));

            // no further requests should be triggered
            assertThat(clientSeenRequests.size(), is(2));
        }, 30, TimeUnit.SECONDS);
    }

    public void testDownsamplingWhenTargetIndexNameClashYieldsException() throws Exception {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 2;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()).put(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(), ONE_HUNDRED_MB)
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING.getKey(), TARGET_MERGE_FACTOR_VALUE)
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .put("index.routing_path", "@timestamp"),
            DataStreamLifecycle.newBuilder()
                .downsampling(
                    new Downsampling(
                        List.of(new Round(TimeValue.timeValueMillis(0), new DownsampleConfig(new DateHistogramInterval("5m"))))
                    )
                )
                .dataRetention(TimeValue.MAX_VALUE)
                .build(),
            now
        );
        builder.put(dataStream);

        String nodeId = "localNode";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        // we are the master node
        nodesBuilder.masterNodeId(nodeId);
        String firstGenIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);

        // mark the first generation as read-only already
        IndexMetadata indexMetadata = builder.get(firstGenIndexName);
        Settings updatedSettings = Settings.builder().put(indexMetadata.getSettings()).put(WRITE.settingName(), true).build();
        builder.put(IndexMetadata.builder(indexMetadata).settings(updatedSettings).settingsVersion(indexMetadata.getSettingsVersion() + 1));

        ClusterBlock indexBlock = MetadataIndexStateService.createUUIDBasedBlock(WRITE.getBlock());
        ClusterBlocks.Builder blocks = ClusterBlocks.builder();
        blocks.addIndexBlock(firstGenIndexName, indexBlock);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).blocks(blocks).metadata(builder).nodes(nodesBuilder).build();

        // add another index to the cluster state that clashes with the expected downsample index name for the configured round
        String downsampleIndexName = DownsampleConfig.generateDownsampleIndexName(
            DOWNSAMPLED_INDEX_PREFIX,
            state.metadata().index(firstGenIndexName),
            new DateHistogramInterval("5m")
        );
        Metadata.Builder newMetadata = Metadata.builder(state.metadata())
            .put(
                IndexMetadata.builder(downsampleIndexName).settings(settings(IndexVersion.current())).numberOfReplicas(0).numberOfShards(1)
            );
        state = ClusterState.builder(state).metadata(newMetadata).nodes(nodesBuilder).build();
        setState(clusterService, state);

        Index firstGenIndex = state.metadata().index(firstGenIndexName).getIndex();
        dataStreamLifecycleService.maybeExecuteDownsampling(clusterService.state(), dataStream, List.of(firstGenIndex));

        assertThat(clientSeenRequests.size(), is(0));
        ErrorEntry error = dataStreamLifecycleService.getErrorStore().getError(firstGenIndexName);
        assertThat(error, notNullValue());
        assertThat(error.error(), containsString("resource_already_exists_exception"));
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

        String dataStreamName = "logs_my-app_prod";
        var clusterState = DataStreamTestHelper.getClusterStateWithDataStream(
            dataStreamName,
            List.of(Tuple.tuple(start1, end1), Tuple.tuple(start2, end2), Tuple.tuple(start3, end3))
        );
        DataStream dataStream = clusterState.getMetadata().dataStreams().get(dataStreamName);

        {
            // test for an index for which `now` is outside its time bounds
            Index firstGenIndex = dataStream.getIndices().get(0);
            Set<Index> indices = DataStreamLifecycleService.timeSeriesIndicesStillWithinTimeBounds(
                clusterState.metadata(),
                // the end_time for the first generation has lapsed
                List.of(firstGenIndex),
                currentTime::toEpochMilli
            );
            assertThat(indices.size(), is(0));
        }

        {
            Set<Index> indices = DataStreamLifecycleService.timeSeriesIndicesStillWithinTimeBounds(
                clusterState.metadata(),
                // the end_time for the first generation has lapsed, but the other 2 generations are still within bounds
                dataStream.getIndices(),
                currentTime::toEpochMilli
            );
            assertThat(indices.size(), is(2));
            assertThat(indices, containsInAnyOrder(dataStream.getIndices().get(1), dataStream.getIndices().get(2)));
        }

        {
            // non time_series indices are not within time bounds (they don't have any)
            IndexMetadata indexMeta = IndexMetadata.builder(randomAlphaOfLengthBetween(10, 30))
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                        .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                        .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current())
                        .build()
                )
                .build();

            Metadata newMetadata = Metadata.builder(clusterState.metadata()).put(indexMeta, true).build();

            Set<Index> indices = DataStreamLifecycleService.timeSeriesIndicesStillWithinTimeBounds(
                newMetadata,
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
            new DataStreamLifecycleHealthInfoPublisher(
                Settings.EMPTY,
                getTransportRequestsRecordingClient(),
                clusterService,
                errorStore,
                new FeatureService(List.of(new DataStreamFeatures()))
            ),
            globalRetentionResolver
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
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            numFailureIndices,
            settings(IndexVersion.current()),
            new DataStreamLifecycle(),
            now
        ).copy().setFailureStoreEnabled(randomBoolean()).build(); // failure store is managed even when disabled
        builder.put(dataStream);
        Metadata metadata = builder.build();
        Set<Index> indicesToExclude = Set.of(dataStream.getIndices().get(0), dataStream.getFailureIndices().getIndices().get(0));
        List<Index> targetBackingIndicesOnly = DataStreamLifecycleService.getTargetIndices(
            dataStream,
            indicesToExclude,
            metadata::index,
            false
        );
        assertThat(targetBackingIndicesOnly, equalTo(dataStream.getIndices().subList(1, 3)));
        List<Index> targetIndices = DataStreamLifecycleService.getTargetIndices(dataStream, indicesToExclude, metadata::index, true);
        assertThat(
            targetIndices,
            equalTo(
                List.of(dataStream.getIndices().get(1), dataStream.getIndices().get(2), dataStream.getFailureIndices().getIndices().get(1))
            )
        );
    }

    public void testFailureStoreIsManagedEvenWhenDisabled() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 1;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            2,
            settings(IndexVersion.current()),
            DataStreamLifecycle.newBuilder().dataRetention(0).build(),
            now
        ).copy().setFailureStoreEnabled(false).build(); // failure store is managed even when it is disabled
        builder.put(dataStream);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();

        dataStreamLifecycleService.run(state);
        assertThat(clientSeenRequests.size(), is(3));
        assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
        RolloverRequest rolloverBackingIndexRequest = (RolloverRequest) clientSeenRequests.get(0);
        assertThat(rolloverBackingIndexRequest.getRolloverTarget(), is(dataStreamName));
        assertThat(
            rolloverBackingIndexRequest.indicesOptions().failureStoreOptions(),
            equalTo(new IndicesOptions.FailureStoreOptions(true, false))
        );
        assertThat(clientSeenRequests.get(1), instanceOf(RolloverRequest.class));
        RolloverRequest rolloverFailureIndexRequest = (RolloverRequest) clientSeenRequests.get(1);
        assertThat(rolloverFailureIndexRequest.getRolloverTarget(), is(dataStreamName));
        assertThat(
            rolloverFailureIndexRequest.indicesOptions().failureStoreOptions(),
            equalTo(new IndicesOptions.FailureStoreOptions(false, true))
        );
        assertThat(
            ((DeleteIndexRequest) clientSeenRequests.get(2)).indices()[0],
            is(dataStream.getFailureIndices().getIndices().get(0).getName())
        );
    }

    public void testMaybeExecuteRetentionSuccessfulDownsampledIndex() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        ClusterState state = downsampleSetup(dataStreamName, SUCCESS);
        DataStream dataStream = state.metadata().dataStreams().get(dataStreamName);
        String firstGenIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);

        // Executing the method to be tested:
        Set<Index> indicesToBeRemoved = dataStreamLifecycleService.maybeExecuteRetention(clusterService.state(), dataStream, Set.of());
        assertThat(indicesToBeRemoved, contains(state.getMetadata().index(firstGenIndexName).getIndex()));
    }

    public void testMaybeExecuteRetentionDownsampledIndexInProgress() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        ClusterState state = downsampleSetup(dataStreamName, STARTED);
        DataStream dataStream = state.metadata().dataStreams().get(dataStreamName);
        String firstGenIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);

        // Executing the method to be tested:
        Set<Index> indicesToBeRemoved = dataStreamLifecycleService.maybeExecuteRetention(clusterService.state(), dataStream, Set.of());
        assertThat(indicesToBeRemoved, empty());
    }

    public void testMaybeExecuteRetentionDownsampledUnknown() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        ClusterState state = downsampleSetup(dataStreamName, UNKNOWN);
        DataStream dataStream = state.metadata().dataStreams().get(dataStreamName);
        String firstGenIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);

        // Executing the method to be tested:
        Set<Index> indicesToBeRemoved = dataStreamLifecycleService.maybeExecuteRetention(clusterService.state(), dataStream, Set.of());
        assertThat(indicesToBeRemoved, contains(state.getMetadata().index(firstGenIndexName).getIndex()));
    }

    private ClusterState downsampleSetup(String dataStreamName, IndexMetadata.DownsampleTaskStatus status) {
        // Base setup:
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            2,
            settings(IndexVersion.current()).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .put("index.routing_path", "@timestamp"),
            DataStreamLifecycle.newBuilder()
                .downsampling(
                    new Downsampling(
                        List.of(new Round(TimeValue.timeValueMillis(0), new DownsampleConfig(new DateHistogramInterval("5m"))))
                    )
                )
                .dataRetention(TimeValue.timeValueMillis(1))
                .build(),
            now
        );
        builder.put(dataStream);

        // Update the first backing index so that is appears to have been downsampled:
        String firstGenIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        var imd = builder.get(firstGenIndexName);
        var imdBuilder = new IndexMetadata.Builder(imd);
        imdBuilder.settings(Settings.builder().put(imd.getSettings()).put(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey(), status).build());
        builder.put(imdBuilder);

        // Attaching state:
        String nodeId = "localNode";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        nodesBuilder.masterNodeId(nodeId);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).nodes(nodesBuilder).build();
        setState(clusterService, state);
        return state;
    }

    /*
     * Creates a test cluster state with the given indexName. If customDataStreamLifecycleMetadata is not null, it is added as the value
     * of the index's custom metadata named "data_stream_lifecycle".
     */
    private ClusterState createClusterState(String indexName, Map<String, String> customDataStreamLifecycleMetadata) {
        var routingTableBuilder = RoutingTable.builder();
        Metadata.Builder metadataBuilder = Metadata.builder();
        Map<String, IndexMetadata> indices = new HashMap<>();
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), randomIntBetween(1, 10))
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), randomIntBetween(0, 3))
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current())
            .build();
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName).version(randomLong()).settings(indexSettings);
        if (customDataStreamLifecycleMetadata != null) {
            indexMetadataBuilder.putCustom(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, customDataStreamLifecycleMetadata);
        }
        indices.put(indexName, indexMetadataBuilder.build());
        return ClusterState.builder(new ClusterName("test-cluster"))
            .routingTable(routingTableBuilder.build())
            .metadata(metadataBuilder.indices(indices).build())
            .build();
    }

    private static DataStreamLifecycleService.ForceMergeRequestWrapper copyForceMergeRequestWrapperRequest(
        DataStreamLifecycleService.ForceMergeRequestWrapper original
    ) {
        return new DataStreamLifecycleService.ForceMergeRequestWrapper(original);
    }

    private static DataStreamLifecycleService.ForceMergeRequestWrapper mutateForceMergeRequestWrapper(
        DataStreamLifecycleService.ForceMergeRequestWrapper original
    ) {
        switch (randomIntBetween(0, 4)) {
            case 0 -> {
                DataStreamLifecycleService.ForceMergeRequestWrapper copy = copyForceMergeRequestWrapperRequest(original);
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
                DataStreamLifecycleService.ForceMergeRequestWrapper copy = copyForceMergeRequestWrapperRequest(original);
                copy.onlyExpungeDeletes(original.onlyExpungeDeletes() == false);
                return copy;
            }
            case 2 -> {
                DataStreamLifecycleService.ForceMergeRequestWrapper copy = copyForceMergeRequestWrapperRequest(original);
                copy.flush(original.flush() == false);
                return copy;
            }
            case 3 -> {
                DataStreamLifecycleService.ForceMergeRequestWrapper copy = copyForceMergeRequestWrapperRequest(original);
                copy.maxNumSegments(original.maxNumSegments() + 1);
                return copy;
            }
            case 4 -> {
                DataStreamLifecycleService.ForceMergeRequestWrapper copy = copyForceMergeRequestWrapperRequest(original);
                copy.setRequestId(original.getRequestId() + 1);
                return copy;
            }
            default -> throw new AssertionError("Can't get here");
        }
    }

    private static RolloverConditions randomRolloverConditions(boolean includeMaxAge) {
        ByteSizeValue maxSize = randomBoolean() ? randomByteSizeValue() : null;
        ByteSizeValue maxPrimaryShardSize = randomBoolean() ? randomByteSizeValue() : null;
        Long maxDocs = randomBoolean() ? randomNonNegativeLong() : null;
        TimeValue maxAge = includeMaxAge && randomBoolean() ? TimeValue.timeValueMillis(randomMillisUpToYear9999()) : null;
        Long maxPrimaryShardDocs = randomBoolean() ? randomNonNegativeLong() : null;
        ByteSizeValue minSize = randomBoolean() ? randomByteSizeValue() : null;
        ByteSizeValue minPrimaryShardSize = randomBoolean() ? randomByteSizeValue() : null;
        Long minDocs = randomBoolean() ? randomNonNegativeLong() : null;
        TimeValue minAge = randomBoolean() ? randomPositiveTimeValue() : null;
        Long minPrimaryShardDocs = randomBoolean() ? randomNonNegativeLong() : null;

        return RolloverConditions.newBuilder()
            .addMaxIndexSizeCondition(maxSize)
            .addMaxPrimaryShardSizeCondition(maxPrimaryShardSize)
            .addMaxIndexAgeCondition(maxAge)
            .addMaxIndexDocsCondition(maxDocs)
            .addMaxPrimaryShardDocsCondition(maxPrimaryShardDocs)
            .addMinIndexSizeCondition(minSize)
            .addMinPrimaryShardSizeCondition(minPrimaryShardSize)
            .addMinIndexAgeCondition(minAge)
            .addMinIndexDocsCondition(minDocs)
            .addMinPrimaryShardDocsCondition(minPrimaryShardDocs)
            .build();
    }

    private static DiscoveryNodes.Builder buildNodes(String nodeId) {
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.localNodeId(nodeId);
        nodesBuilder.add(getNode(nodeId));
        return nodesBuilder;
    }

    private static DiscoveryNode getNode(String nodeId) {
        return DiscoveryNodeUtils.builder(nodeId)
            .name(nodeId)
            .ephemeralId(nodeId)
            .address("host", "host_address", buildNewFakeTransportAddress())
            .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_HOT_NODE_ROLE, DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE))
            .build();
    }

    /**
     * This method returns a client that keeps track of the requests it has seen in clientSeenRequests. By default it does nothing else
     * (it does not even notify the listener), but tests can provide an implementation of clientDelegate to provide any needed behavior.
     */
    private Client getTransportRequestsRecordingClient() {
        return new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                clientSeenRequests.add(request);
                if (clientDelegate != null) {
                    clientDelegate.doExecute(action, request, listener);
                }
            }
        };
    }

    private interface DoExecuteDelegate {
        @SuppressWarnings("rawtypes")
        void doExecute(ActionType action, ActionRequest request, ActionListener listener);
    }
}
