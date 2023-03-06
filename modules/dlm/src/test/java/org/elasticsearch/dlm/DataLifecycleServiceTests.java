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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class DataLifecycleServiceTests extends ESTestCase {

    private long now;
    private ThreadPool threadPool;
    private DataLifecycleService dataLifecycleService;
    private List<TransportRequest> clientSeenRequests;
    private NoOpClient client;

    @Before
    public void setupServices() {
        threadPool = new TestThreadPool(getTestName());
        Set<Setting<?>> builtInClusterSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        builtInClusterSettings.add(DataLifecycleService.DLM_POLL_INTERVAL_SETTING);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, builtInClusterSettings);
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, clusterSettings);

        now = System.currentTimeMillis();
        Clock clock = Clock.fixed(Instant.ofEpochMilli(now), ZoneId.of(randomFrom(ZoneId.getAvailableZoneIds())));
        clientSeenRequests = new CopyOnWriteArrayList<>();

        client = getTransportRequestsRecordingClient();
        dataLifecycleService = new DataLifecycleService(Settings.EMPTY, client, clusterService, clock, threadPool, () -> now);
        dataLifecycleService.init();
    }

    @After
    public void cleanup() {
        clientSeenRequests.clear();
        dataLifecycleService.close();
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
