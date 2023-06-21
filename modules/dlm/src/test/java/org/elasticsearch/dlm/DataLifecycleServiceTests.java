/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dlm;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.client.internal.Client;
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
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.dlm.DLMFixtures.createDataStream;
import static org.elasticsearch.dlm.DataLifecycleService.FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY;
import static org.elasticsearch.dlm.DataLifecycleService.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DataLifecycleServiceTests extends ESTestCase {

    private long now;
    private ThreadPool threadPool;
    private DataLifecycleService dataLifecycleService;
    private List<TransportRequest> clientSeenRequests;
    private Client client;
    private DoExecuteDelegate clientDelegate;
    private ClusterService clusterService;

    @Before
    public void setupServices() {
        threadPool = new TestThreadPool(getTestName());
        Set<Setting<?>> builtInClusterSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        builtInClusterSettings.add(DataLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL_SETTING);
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
        clientDelegate = null;
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
            new DataLifecycle(TimeValue.timeValueMillis(0)),
            now
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
            new DataLifecycle((TimeValue) null),
            now
        );
        builder.put(dataStream);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();
        dataLifecycleService.run(state);
        assertThat(clientSeenRequests.size(), is(3));  // rollover the write index, and force merge the other two
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
            new DataLifecycle(TimeValue.timeValueDays(700)),
            now
        );
        builder.put(dataStream);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();
        dataLifecycleService.run(state);
        assertThat(clientSeenRequests.size(), is(3)); // rollover the write index, and force merge the other two
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
            new DataLifecycle(TimeValue.timeValueMillis(0)),
            now
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
            null,
            now
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
            new DataLifecycle(),
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
            new DataLifecycle(TimeValue.timeValueDays(700)),
            now
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

    @SuppressWarnings("unchecked")
    public void testForceMerge() throws Exception {
        // We want this test method to get fake force merge responses, because this is what triggers a cluster state update
        clientDelegate = (action, request, listener) -> {
            if (action.name().equals("indices:admin/forcemerge")) {
                listener.onResponse(new ForceMergeResponse(5, 5, 0, List.of()));
            }
        };
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(Version.CURRENT),
            new DataLifecycle(TimeValue.MAX_VALUE),
            now
        );
        builder.put(dataStream);

        String nodeId = "localNode";
        DiscoveryNodes.Builder nodesBuilder = buildNodes(nodeId);
        // we are the master node
        nodesBuilder.masterNodeId(nodeId);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).nodes(nodesBuilder).build();
        setState(clusterService, state);
        dataLifecycleService.run(clusterService.state());

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
        dataLifecycleService.run(clusterService.state());
        assertThat(clientSeenRequests.size(), is(3));

        // Add another index backing, and make sure that the only thing that happens is another force merge
        IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(
            DataStream.getDefaultBackingIndexName(dataStreamName, numBackingIndices + 1)
        ).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1).creationDate(now - 3000L);
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
        dataLifecycleService.run(clusterService.state());
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
         * This test makes sure that DLM correctly retries (or doesn't) forcemerge requests on failure.
         * First, we set up a datastream with 3 backing indices. On the first run of DLM we'll expect one to get rolled over and two to
         * be forcemerged.
         */
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(Version.CURRENT),
            new DataLifecycle(TimeValue.MAX_VALUE),
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
             * For the first DLM run we're intentionally making forcemerge fail:
             */
            AtomicInteger forceMergeFailedCount = new AtomicInteger(0);
            clientDelegate = (action, request, listener) -> {
                if (action.name().equals("indices:admin/forcemerge")) {
                    listener.onFailure(new RuntimeException("Forcemerge failure"));
                    forceMergeFailedCount.incrementAndGet();
                }
            };
            dataLifecycleService.run(clusterService.state());
            /*
             * We expect that DLM will try to pick it up next time.
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
             * For the next DLM run we're intentionally making forcemerge fail by reporting failed shards:
             */
            AtomicInteger forceMergeFailedCount = new AtomicInteger(0);
            clientDelegate = (action, request, listener) -> {
                if (action.name().equals("indices:admin/forcemerge")) {
                    listener.onResponse(
                        new ForceMergeResponse(
                            5,
                            5,
                            1,
                            List.of(new DefaultShardOperationFailedException(new ElasticsearchException("failure")))
                        )
                    );
                    forceMergeFailedCount.incrementAndGet();
                }
            };
            dataLifecycleService.run(clusterService.state());
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
             * For the next DLM run we're intentionally making forcemerge fail on the same indices by having the successful shards not equal
             *  to the total:
             */
            AtomicInteger forceMergeFailedCount = new AtomicInteger(0);
            clientDelegate = (action, request, listener) -> {
                if (action.name().equals("indices:admin/forcemerge")) {
                    listener.onResponse(new ForceMergeResponse(5, 4, 0, List.of()));
                    forceMergeFailedCount.incrementAndGet();
                }
            };
            dataLifecycleService.run(clusterService.state());
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
            // For the final DLM run, we let forcemerge run normally
            clientDelegate = (action, request, listener) -> {
                if (action.name().equals("indices:admin/forcemerge")) {
                    listener.onResponse(new ForceMergeResponse(5, 5, 0, List.of()));
                }
            };
            dataLifecycleService.run(clusterService.state());
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
         * This test creates a datastream with one index, and then runs DLM repeatedly many times. We assert that the size of the
         * transportActionsDeduplicator never goes over 1, and is 0 by the end. This is to make sure that the equals/hashcode methods
         * of ForceMergeRequests are interacting with the deduplicator as expected.
         */
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(Version.CURRENT),
            new DataLifecycle(TimeValue.MAX_VALUE),
            now
        );
        builder.put(dataStream);
        IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(
            DataStream.getDefaultBackingIndexName(dataStreamName, numBackingIndices + 1)
        ).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1).creationDate(now - 3000L);
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
                listener.onResponse(new ForceMergeResponse(5, 5, 0, List.of()));
            }
        };
        for (int i = 0; i < 100; i++) {
            dataLifecycleService.run(clusterService.state());
            assertThat(dataLifecycleService.transportActionsDeduplicator.size(), lessThanOrEqualTo(1));
        }
        assertBusy(() -> assertThat(dataLifecycleService.transportActionsDeduplicator.size(), equalTo(0)));
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
        DataLifecycleService.UpdateForceMergeCompleteTask task = new DataLifecycleService.UpdateForceMergeCompleteTask(
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
            Map<String, String> dlmMetadata = indexMetadata.getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY);
            assertThat(dlmMetadata, notNullValue());
            assertThat(dlmMetadata.size(), equalTo(1));
            String forceMergeCompleteTimestampString = dlmMetadata.get(FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY);
            assertThat(forceMergeCompleteTimestampString, notNullValue());
            long forceMergeCompleteTimestamp = Long.parseLong(forceMergeCompleteTimestampString);
            assertThat(forceMergeCompleteTimestamp, lessThanOrEqualTo(threadPool.absoluteTimeInMillis()));
            // The listener's onResponse should not be called by execute():
            assertThat(onResponseCount.get(), equalTo(0));
        }
        {
            /*
             * This is the same as the previous block, except that this time we'll have previously-existing DLM custom metadata in the
             * index's metadata, and make sure that it doesn't get blown away when we set the timestamp.
             */
            String preExistingDlmCustomMetadataKey = randomAlphaOfLength(10);
            String preExistingDlmCustomMetadataValue = randomAlphaOfLength(20);
            Map<String, String> preExistingDlmCustomMetadata = Map.of(preExistingDlmCustomMetadataKey, preExistingDlmCustomMetadataValue);
            ClusterState clusterState = createClusterState(targetIndex, preExistingDlmCustomMetadata);
            ClusterState newClusterState = task.execute(clusterState);
            IndexMetadata indexMetadata = newClusterState.metadata().index(targetIndex);
            Map<String, String> dlmMetadata = indexMetadata.getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY);
            assertThat(dlmMetadata, notNullValue());
            assertThat(dlmMetadata.size(), equalTo(2));
            assertThat(dlmMetadata.get(preExistingDlmCustomMetadataKey), equalTo(preExistingDlmCustomMetadataValue));
            String forceMergeCompleteTimestampString = dlmMetadata.get(FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY);
            assertThat(forceMergeCompleteTimestampString, notNullValue());
            long forceMergeCompleteTimestamp = Long.parseLong(forceMergeCompleteTimestampString);
            assertThat(forceMergeCompleteTimestamp, lessThanOrEqualTo(threadPool.absoluteTimeInMillis()));
            // The listener's onResponse should not be called by execute():
            assertThat(onResponseCount.get(), equalTo(0));
        }
    }

    /*
     * Creates a test cluster state with the given indexName. If customDlmMetadata is not null, it is added as the value of the index's
     * custom metadata named "dlm".
     */
    private ClusterState createClusterState(String indexName, Map<String, String> customDlmMetadata) {
        var routingTableBuilder = RoutingTable.builder();
        Metadata.Builder metadataBuilder = Metadata.builder();
        Map<String, IndexMetadata> indices = new HashMap<>();
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), randomIntBetween(1, 10))
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), randomIntBetween(0, 3))
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            .build();
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName).version(randomLong()).settings(indexSettings);
        if (customDlmMetadata != null) {
            indexMetadataBuilder.putCustom(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, customDlmMetadata);
        }
        indices.put(indexName, indexMetadataBuilder.build());
        return ClusterState.builder(new ClusterName("test-cluster"))
            .routingTable(routingTableBuilder.build())
            .metadata(metadataBuilder.indices(indices).build())
            .build();
    }

    public void testDefaultRolloverRequest() {
        // test auto max_age and another concrete condition
        {
            RolloverConditions randomConcreteRolloverConditions = randomRolloverConditions(false);
            RolloverRequest rolloverRequest = DataLifecycleService.getDefaultRolloverRequest(
                new RolloverConfiguration(randomConcreteRolloverConditions, Set.of("max_age")),
                "my-data-stream",
                null
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
            RolloverRequest rolloverRequestWithRetention = DataLifecycleService.getDefaultRolloverRequest(
                new RolloverConfiguration(randomConcreteRolloverConditions, Set.of("max_age")),
                "my-data-stream",
                TimeValue.timeValueDays(3)
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
            RolloverRequest rolloverRequest = DataLifecycleService.getDefaultRolloverRequest(
                new RolloverConfiguration(randomConcreteRolloverConditions),
                "my-data-stream",
                null
            );
            assertThat(rolloverRequest.getRolloverTarget(), equalTo("my-data-stream"));
            assertThat(rolloverRequest.getConditions(), equalTo(randomConcreteRolloverConditions));
            RolloverRequest rolloverRequestWithRetention = DataLifecycleService.getDefaultRolloverRequest(
                new RolloverConfiguration(randomConcreteRolloverConditions),
                "my-data-stream",
                TimeValue.timeValueDays(1)
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
            new DataLifecycleService.ForceMergeRequestWrapper(originalRequest),
            DataLifecycleServiceTests::copyForceMergeRequestWrapperRequest,
            DataLifecycleServiceTests::mutateForceMergeRequestWrapper
        );
    }

    private static DataLifecycleService.ForceMergeRequestWrapper copyForceMergeRequestWrapperRequest(
        DataLifecycleService.ForceMergeRequestWrapper original
    ) {
        return new DataLifecycleService.ForceMergeRequestWrapper(original);
    }

    private static DataLifecycleService.ForceMergeRequestWrapper mutateForceMergeRequestWrapper(
        DataLifecycleService.ForceMergeRequestWrapper original
    ) {
        switch (randomIntBetween(0, 4)) {
            case 0 -> {
                DataLifecycleService.ForceMergeRequestWrapper copy = copyForceMergeRequestWrapperRequest(original);
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
                DataLifecycleService.ForceMergeRequestWrapper copy = copyForceMergeRequestWrapperRequest(original);
                copy.onlyExpungeDeletes(original.onlyExpungeDeletes() == false);
                return copy;
            }
            case 2 -> {
                DataLifecycleService.ForceMergeRequestWrapper copy = copyForceMergeRequestWrapperRequest(original);
                copy.flush(original.flush() == false);
                return copy;
            }
            case 3 -> {
                DataLifecycleService.ForceMergeRequestWrapper copy = copyForceMergeRequestWrapperRequest(original);
                copy.maxNumSegments(original.maxNumSegments() + 1);
                return copy;
            }
            case 4 -> {
                DataLifecycleService.ForceMergeRequestWrapper copy = copyForceMergeRequestWrapperRequest(original);
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
        TimeValue minAge = randomBoolean() ? TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test") : null;
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
        return new DiscoveryNode(
            nodeId,
            nodeId,
            nodeId,
            "host",
            "host_address",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_HOT_NODE_ROLE, DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE),
            null
        );
    }

    /**
     * This method returns a client that keeps track of the requests it has seen in clientSeenRequests. By default it does nothing else
     * (it does not even notify the listener), but tests can provide an implementation of clientDelegate to provide any needed behavior.
     */
    private Client getTransportRequestsRecordingClient() {
        return new NoOpClient(getTestName()) {
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
