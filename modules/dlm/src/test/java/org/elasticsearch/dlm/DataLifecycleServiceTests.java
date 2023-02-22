/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dlm;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataLifecycle;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DataLifecycleServiceTests extends ESTestCase {

    private ClusterService clusterService;
    private long now;
    private ThreadPool threadPool;
    private IndicesAdminClient indicesClient;
    private DataLifecycleService dataLifecycleService;
    ArgumentCaptor<RolloverRequest> rolloverRequestCaptor;
    ArgumentCaptor<DeleteIndexRequest> deleteRequestCaptor;

    @Before
    public void setupServices() {
        clusterService = mock(ClusterService.class);
        now = randomNonNegativeLong();
        Clock clock = Clock.fixed(Instant.ofEpochMilli(now), ZoneId.of(randomFrom(ZoneId.getAvailableZoneIds())));

        doAnswer(invocationOnMock -> null).when(clusterService).addListener(any());

        Settings settings = Settings.builder().put(DataLifecycleService.DLM_POLL_INTERVAL, "1s").build();
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(settings, Collections.singleton(DataLifecycleService.DLM_POLL_INTERVAL_SETTING))
        );
        when(clusterService.lifecycleState()).thenReturn(Lifecycle.State.STARTED);

        Client client = mock(Client.class);
        AdminClient adminClient = mock(AdminClient.class);
        indicesClient = mock(IndicesAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesClient);
        when(client.settings()).thenReturn(Settings.EMPTY);

        threadPool = new TestThreadPool(getTestName());
        dataLifecycleService = new DataLifecycleService(Settings.EMPTY, client, clusterService, clock, threadPool, () -> now);
        Mockito.verify(clusterService).addListener(dataLifecycleService);

        rolloverRequestCaptor = ArgumentCaptor.forClass(RolloverRequest.class);
        deleteRequestCaptor = ArgumentCaptor.forClass(DeleteIndexRequest.class);
    }

    @After
    public void cleanup() {
        when(clusterService.lifecycleState()).thenReturn(randomFrom(Lifecycle.State.STOPPED, Lifecycle.State.CLOSED));
        dataLifecycleService.close();
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
            settings(Version.CURRENT),
            new DataLifecycle(TimeValue.timeValueMillis(0))
        );
        builder.put(dataStream);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();

        dataLifecycleService.run(state);
        verify(indicesClient, times(1)).rolloverIndex(rolloverRequestCaptor.capture(), any());
        assertThat(rolloverRequestCaptor.getValue().getRolloverTarget(), is(dataStreamName));
        verify(indicesClient, times(2)).delete(deleteRequestCaptor.capture(), any());
        List<DeleteIndexRequest> deleteRequests = deleteRequestCaptor.getAllValues();
        assertThat(deleteRequests.get(0).indices()[0], is(dataStream.getIndices().get(0).getName()));
        assertThat(deleteRequests.get(1).indices()[0], is(dataStream.getIndices().get(1).getName()));

        // on the second run the rollover and delete requests should not execute anymore
        // i.e. the count should *remain* 1 for rollover and 2 for deletes
        dataLifecycleService.run(state);
        verify(indicesClient, times(1)).rolloverIndex(any(), any());
        verify(indicesClient, times(2)).delete(any(), any());
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
        verify(indicesClient, times(1)).rolloverIndex(any(), any());
        verify(indicesClient, times(0)).delete(any(), any());
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
        verify(indicesClient, times(1)).rolloverIndex(any(), any());
        verify(indicesClient, times(0)).delete(any(), any());
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
        verify(indicesClient, times(0)).rolloverIndex(any(), any());
        verify(indicesClient, times(0)).delete(any(), any());
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
        verify(indicesClient, times(0)).rolloverIndex(any(), any());
        verify(indicesClient, times(0)).delete(any(), any());
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
}
