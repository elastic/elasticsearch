/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dlm;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataLifecycle;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DataLifecycleServiceTests extends ESTestCase {

    private long now;
    private ThreadPool threadPool;
    private DataLifecycleService dataLifecycleService;
    private List<TransportRequest> clientSeenRequests;
    private NoOpClient client;
    private ClusterService clusterService;

    @Before
    public void setupServices() {
        threadPool = new TestThreadPool(getTestName());
        Set<Setting<?>> builtInClusterSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        builtInClusterSettings.add(DataLifecycleService.DLM_POLL_INTERVAL_SETTING);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, builtInClusterSettings);
        clusterService = createClusterService(threadPool, clusterSettings);

        now = System.currentTimeMillis();
        Clock clock = Clock.fixed(Instant.ofEpochMilli(now), ZoneId.of(randomFrom(ZoneId.getAvailableZoneIds())));
        clientSeenRequests = new CopyOnWriteArrayList<>();

        client = getTransportRequestsRecordingClient();
        dataLifecycleService = new DataLifecycleService(
            Settings.EMPTY,
            client,
            clusterService,
            clock,
            threadPool,
            () -> now,
            new DataLifecycleErrorStore()
        );
        dataLifecycleService.init();
    }

    @After
    public void cleanup() {
        clientSeenRequests.clear();
        dataLifecycleService.close();
        clusterService.close();
        threadPool.shutdownNow();
        client.close();
    }

    public void testOperationsExecutedOnce() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(Version.CURRENT),
            new DataLifecycle(TimeValue.timeValueMillis(0))
        );
        builder.put(dataStream);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();

        dataLifecycleService.run(state);
        assertThat(clientSeenRequests.size(), is(3));
        assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
        assertThat(((RolloverRequest) clientSeenRequests.get(0)).getRolloverTarget(), is(dataStreamName));
        List<DeleteIndexRequest> deleteRequests = clientSeenRequests.subList(1, 3)
            .stream()
            .map(transportRequest -> (DeleteIndexRequest) transportRequest)
            .toList();
        assertThat(deleteRequests.get(0).indices()[0], is(dataStream.getIndices().get(0).getName()));
        assertThat(deleteRequests.get(1).indices()[0], is(dataStream.getIndices().get(1).getName()));

        // on the second run the rollover and delete requests should not execute anymore
        // i.e. the count should *remain* 1 for rollover and 2 for deletes
        dataLifecycleService.run(state);
        assertThat(clientSeenRequests.size(), is(3));
    }

    public void testRetentionNotConfigured() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(Version.CURRENT),
            new DataLifecycle((TimeValue) null)
        );
        builder.put(dataStream);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();
        dataLifecycleService.run(state);
        assertThat(clientSeenRequests.size(), is(1));
        assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
    }

    public void testRetentionNotExecutedDueToAge() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(Version.CURRENT),
            new DataLifecycle(TimeValue.timeValueDays(700))
        );
        builder.put(dataStream);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();
        dataLifecycleService.run(state);
        assertThat(clientSeenRequests.size(), is(1));
        assertThat(clientSeenRequests.get(0), instanceOf(RolloverRequest.class));
    }

    public void testIlmManagedIndicesAreSkipped() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            Settings.builder().put(IndexMetadata.LIFECYCLE_NAME, "ILM_policy").put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT),
            new DataLifecycle(TimeValue.timeValueMillis(0))
        );
        builder.put(dataStream);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();
        dataLifecycleService.run(state);
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
            Settings.builder().put(IndexMetadata.LIFECYCLE_NAME, "ILM_policy").put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT),
            null
        );
        builder.put(dataStream);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();
        dataLifecycleService.run(state);
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
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT),
            new DataLifecycle()
        );
        builder.put(dataStream);
        String nodeId = "localNode";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        // we are the master node
        nodesBuilder.masterNodeId(nodeId);
        ClusterState previousState = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).nodes(nodesBuilder).build();

        // all backing indices are in the error store
        for (Index index : dataStream.getIndices()) {
            dataLifecycleService.getErrorStore().recordError(index.getName(), new NullPointerException("bad"));
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

        dataLifecycleService.run(stateWithDeletedIndices);

        for (Index deletedIndex : deletedIndices) {
            assertThat(dataLifecycleService.getErrorStore().getError(deletedIndex.getName()), nullValue());
        }
        // the value for the write index should still be in the error store
        assertThat(dataLifecycleService.getErrorStore().getError(dataStream.getWriteIndex().getName()), notNullValue());
    }

    public void testErrorStoreIsClearedOnBackingIndexBecomingUnmanaged() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(Version.CURRENT),
            new DataLifecycle(TimeValue.timeValueDays(700))
        );
        // all backing indices are in the error store
        for (Index index : dataStream.getIndices()) {
            dataLifecycleService.getErrorStore().recordError(index.getName(), new NullPointerException("bad"));
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

        dataLifecycleService.run(updatedState);

        for (Index index : dataStream.getIndices()) {
            assertThat(dataLifecycleService.getErrorStore().getError(index.getName()), nullValue());
        }
    }

    private static DiscoveryNodes.Builder buildNodes(String nodeId) {
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.localNodeId(nodeId);
        nodesBuilder.add(getNode(nodeId));
        return nodesBuilder;
    }

    private static DiscoveryNode getNode(String nodeId) {
        return new DiscoveryNode(
            nodeId,
            nodeId,
            nodeId,
            "host",
            "host_address",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_HOT_NODE_ROLE, DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE),
            Version.CURRENT
        );
    }

    private DataStream createDataStream(
        Metadata.Builder builder,
        String dataStreamName,
        int backingIndicesCount,
        Settings.Builder backingIndicesSettings,
        @Nullable DataLifecycle lifecycle
    ) {
        final List<Index> backingIndices = new ArrayList<>();
        for (int k = 1; k <= backingIndicesCount; k++) {
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, k))
                .settings(backingIndicesSettings)
                .numberOfShards(1)
                .numberOfReplicas(1)
                .creationDate(now - 3000L);
            if (k < backingIndicesCount) {
                // add rollover info only for non-write indices
                MaxAgeCondition rolloverCondition = new MaxAgeCondition(TimeValue.timeValueMillis(now - 2000L));
                indexMetaBuilder.putRolloverInfo(new RolloverInfo(dataStreamName, List.of(rolloverCondition), now - 2000L));
            }
            IndexMetadata indexMetadata = indexMetaBuilder.build();
            builder.put(indexMetadata, false);
            backingIndices.add(indexMetadata.getIndex());
        }
        return newInstance(dataStreamName, backingIndices, backingIndicesCount, null, false, lifecycle);
    }

    private NoOpClient getTransportRequestsRecordingClient() {
        return new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                clientSeenRequests.add(request);
            }
        };
    }
}
