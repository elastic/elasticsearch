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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionSettings;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle.DownsamplingRound;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.lifecycle.health.DataStreamLifecycleHealthInfoPublisher;
import org.elasticsearch.dlm.DataStreamLifecycleErrorStore;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.datastreams.DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleFixtures.createDataStream;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.DATA_STREAM_MERGE_POLICY_TARGET_FACTOR_SETTING;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.DATA_STREAM_MERGE_POLICY_TARGET_FLOOR_SEGMENT_SETTING;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;

public abstract class DataStreamLifecycleServiceTestCase extends ESTestCase {

    protected long now;
    protected ThreadPool threadPool;
    protected DataStreamLifecycleService dataStreamLifecycleService;
    protected List<TransportRequest> clientSeenRequests;
    protected DoExecuteDelegate clientDelegate;
    protected volatile CountDownLatch clientWaitLatch;
    protected volatile CountDownLatch invokerWaitLatch;
    protected ClusterService clusterService;
    protected final DataStreamGlobalRetentionSettings globalRetentionSettings = DataStreamGlobalRetentionSettings.create(
        ClusterSettings.createBuiltInClusterSettings()
    );

    @Before
    public void setupServices() {
        threadPool = new TestThreadPool(getTestName());
        Set<Setting<?>> builtInClusterSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        builtInClusterSettings.add(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL_SETTING);
        builtInClusterSettings.add(DATA_STREAM_MERGE_POLICY_TARGET_FLOOR_SEGMENT_SETTING);
        builtInClusterSettings.add(DATA_STREAM_MERGE_POLICY_TARGET_FACTOR_SETTING);
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
            new DataStreamLifecycleHealthInfoPublisher(Settings.EMPTY, client, clusterService, errorStore),
            globalRetentionSettings
        );
        clientWaitLatch = null;
        invokerWaitLatch = null;
        clientDelegate = null;
    }

    @After
    public void cleanup() {
        clientSeenRequests.clear();
        dataStreamLifecycleService.close();
        clusterService.close();
        threadPool.shutdownNow();
    }

    protected ClusterState downsampleSetup(ProjectId projectId, String dataStreamName, IndexMetadata.DownsampleTaskStatus status) {
        // Base setup:
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            2,
            settings(IndexVersion.current()).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .put("index.routing_path", "@timestamp"),
            DataStreamLifecycle.dataLifecycleBuilder()
                .downsamplingRounds(List.of(new DownsamplingRound(TimeValue.timeValueMillis(0), new DateHistogramInterval("5m"))))
                .dataRetention(TimeValue.timeValueMillis(1))
                .build(),
            now
        );
        builder.put(dataStream);

        // Update the first backing index so that is appears to have been downsampled:
        String firstGenIndexName = dataStream.getIndices().getFirst().getName();
        var imd = builder.get(firstGenIndexName);
        var imdBuilder = new IndexMetadata.Builder(imd);
        imdBuilder.settings(Settings.builder().put(imd.getSettings()).put(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey(), status).build());
        builder.put(imdBuilder);

        // Attaching state:
        String nodeId = "localNode";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        nodesBuilder.masterNodeId(nodeId);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).nodes(nodesBuilder).build();
        setState(clusterService, state);
        return state;
    }

    /*
     * Creates a test cluster state with the given indexName. If customDataStreamLifecycleMetadata is not null, it is added as the value
     * of the index's custom metadata named "data_stream_lifecycle".
     */
    protected ClusterState createClusterState(
        ProjectId projectId,
        String indexName,
        Map<String, String> customDataStreamLifecycleMetadata
    ) {
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
            .version(randomLong())
            .settings(
                indexSettings(randomIntBetween(1, 10), randomIntBetween(0, 3)).put(
                    IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(),
                    IndexVersion.current()
                )
            );
        if (customDataStreamLifecycleMetadata != null) {
            indexMetadataBuilder.putCustom(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, customDataStreamLifecycleMetadata);
        }
        final var project = ProjectMetadata.builder(projectId).put(indexMetadataBuilder.build(), false).build();
        return ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(project).build();
    }

    protected static DiscoveryNodes.Builder buildNodes(String nodeId) {
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
    protected Client getTransportRequestsRecordingClient() {
        return new NoOpClient(threadPool, TestProjectResolvers.usingRequestHeader(threadPool.getThreadContext())) {
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
                if (invokerWaitLatch != null) {
                    invokerWaitLatch.countDown();
                }
                if (clientWaitLatch != null && clientWaitLatch.getCount() > 0) {
                    try {
                        logger.info("--> blocking client invocation");
                        assertTrue("waited for latch but it never decremented", clientWaitLatch.await(10, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new ElasticsearchException(e);
                    }
                }
            }
        };
    }

    protected interface DoExecuteDelegate {
        @SuppressWarnings("rawtypes")
        void doExecute(ActionType action, ActionRequest request, ActionListener listener);
    }
}
