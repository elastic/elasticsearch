/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class LogsPatternUsageServiceTests extends ESTestCase {

    public void testOnMaster() throws Exception {
        var nodeSettings = Settings.builder().put("logsdb.usage_check.max_period", "1s").build();
        var client = mock(Client.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<ClusterUpdateSettingsResponse> listener = (ActionListener<ClusterUpdateSettingsResponse>) invocationOnMock
                .getArguments()[2];
            var persistentSettings = Settings.builder().put("logsdb.prior_logs_usage", true).build();
            listener.onResponse(new ClusterUpdateSettingsResponse(true, Settings.EMPTY, persistentSettings));
            return null;
        }).when(client).execute(same(ClusterUpdateSettingsAction.INSTANCE), any(), any());

        try (var threadPool = new TestThreadPool(getTestName())) {
            when(client.threadPool()).thenReturn(threadPool);
            var clusterState = DataStreamTestHelper.getClusterStateWithDataStreams(List.of(new Tuple<>("logs-app1-prod", 1)), List.of());
            Supplier<Metadata> metadataSupplier = clusterState::metadata;

            var service = new LogsPatternUsageService(client, nodeSettings, threadPool, metadataSupplier);
            // pre-check:
            assertFalse(service.isMaster);
            assertFalse(service.hasPriorLogsUsage);
            assertNull(service.cancellable);
            // Trigger service:
            service.onMaster();
            assertBusy(() -> {
                assertTrue(service.isMaster);
                assertTrue(service.hasPriorLogsUsage);
                assertNull(service.cancellable);
            });
        }
    }

    public void testCheckHasUsage() {
        var nodeSettings = Settings.EMPTY;
        var client = mock(Client.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<ClusterUpdateSettingsResponse> listener = (ActionListener<ClusterUpdateSettingsResponse>) invocationOnMock
                .getArguments()[2];
            var persistentSettings = Settings.builder().put("logsdb.prior_logs_usage", true).build();
            listener.onResponse(new ClusterUpdateSettingsResponse(true, Settings.EMPTY, persistentSettings));
            return null;
        }).when(client).execute(same(ClusterUpdateSettingsAction.INSTANCE), any(), any());

        var threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        var scheduledCancellable = mock(Scheduler.ScheduledCancellable.class);
        when(threadPool.schedule(any(), any(), any())).thenReturn(scheduledCancellable);
        var clusterState = DataStreamTestHelper.getClusterStateWithDataStreams(List.of(new Tuple<>("logs-app1-prod", 1)), List.of());
        Supplier<Metadata> metadataSupplier = clusterState::metadata;

        LogsPatternUsageService service = new LogsPatternUsageService(client, nodeSettings, threadPool, metadataSupplier);
        service.onMaster();
        assertFalse(service.hasPriorLogsUsage);
        assertNotNull(service.cancellable);
        assertEquals(service.nextWaitTime, TimeValue.timeValueMinutes(1));
        service.check();
        assertTrue(service.hasPriorLogsUsage);
        assertNull(service.cancellable);
        assertEquals(service.nextWaitTime, TimeValue.timeValueMinutes(1));

        verify(threadPool, times(1)).schedule(any(), any(), any());
        verify(client, times(1)).execute(same(ClusterUpdateSettingsAction.INSTANCE), any(), any());
    }

    public void testCheckHasUsageNoMatch() {
        var nodeSettings = Settings.EMPTY;
        var client = mock(Client.class);

        var threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        var scheduledCancellable = mock(Scheduler.ScheduledCancellable.class);
        when(threadPool.schedule(any(), any(), any())).thenReturn(scheduledCancellable);
        var clusterState = DataStreamTestHelper.getClusterStateWithDataStreams(List.of(new Tuple<>("log-app1-prod", 1)), List.of());
        Supplier<Metadata> metadataSupplier = clusterState::metadata;

        LogsPatternUsageService service = new LogsPatternUsageService(client, nodeSettings, threadPool, metadataSupplier);
        service.onMaster();
        assertFalse(service.hasPriorLogsUsage);
        assertNotNull(service.cancellable);
        assertEquals(service.nextWaitTime, TimeValue.timeValueMinutes(1));
        service.check();
        assertFalse(service.hasPriorLogsUsage);
        assertNotNull(service.cancellable);
        assertEquals(service.nextWaitTime, TimeValue.timeValueMinutes(2));

        verify(threadPool, times(2)).schedule(any(), any(), any());
        verify(client, times(1)).threadPool();
    }

    public void testCheckPriorLogsUsageAlreadySet() {
        var nodeSettings = Settings.EMPTY;
        var client = mock(Client.class);

        var threadPool = mock(ThreadPool.class);
        var scheduledCancellable = mock(Scheduler.ScheduledCancellable.class);
        when(threadPool.schedule(any(), any(), any())).thenReturn(scheduledCancellable);
        var clusterState = DataStreamTestHelper.getClusterStateWithDataStreams(List.of(new Tuple<>("log-app1-prod", 1)), List.of());
        clusterState = ClusterState.builder(clusterState)
            .metadata(
                Metadata.builder(clusterState.getMetadata())
                    .persistentSettings(Settings.builder().put("logsdb.prior_logs_usage", true).build())
                    .build()
            )
            .build();
        Supplier<Metadata> metadataSupplier = clusterState::metadata;

        LogsPatternUsageService service = new LogsPatternUsageService(client, nodeSettings, threadPool, metadataSupplier);
        service.isMaster = true;
        assertFalse(service.hasPriorLogsUsage);
        assertNull(service.cancellable);
        service.check();
        assertTrue(service.hasPriorLogsUsage);
        assertNull(service.cancellable);

        verify(client, times(1)).threadPool();
        verifyNoInteractions(threadPool);
    }

    public void testCheckHasUsageUnexpectedResponse() {
        var nodeSettings = Settings.EMPTY;
        var client = mock(Client.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<ClusterUpdateSettingsResponse> listener = (ActionListener<ClusterUpdateSettingsResponse>) invocationOnMock
                .getArguments()[2];
            ClusterUpdateSettingsResponse response;
            if (randomBoolean()) {
                var persistentSettings = Settings.builder().put("logsdb.prior_logs_usage", true).build();
                response = new ClusterUpdateSettingsResponse(false, Settings.EMPTY, persistentSettings);
            } else {
                response = new ClusterUpdateSettingsResponse(true, Settings.EMPTY, Settings.EMPTY);
            }
            listener.onResponse(response);
            return null;
        }).when(client).execute(same(ClusterUpdateSettingsAction.INSTANCE), any(), any());

        var threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);
        var scheduledCancellable = mock(Scheduler.ScheduledCancellable.class);
        when(threadPool.schedule(any(), any(), any())).thenReturn(scheduledCancellable);
        var clusterState = DataStreamTestHelper.getClusterStateWithDataStreams(List.of(new Tuple<>("logs-app1-prod", 1)), List.of());
        Supplier<Metadata> metadataSupplier = clusterState::metadata;

        LogsPatternUsageService service = new LogsPatternUsageService(client, nodeSettings, threadPool, metadataSupplier);
        service.isMaster = true;
        assertFalse(service.hasPriorLogsUsage);
        assertNull(service.cancellable);
        service.check();
        assertFalse(service.hasPriorLogsUsage);
        assertNotNull(service.cancellable);

        verify(threadPool, times(1)).schedule(any(), any(), any());
        verify(client, times(1)).execute(same(ClusterUpdateSettingsAction.INSTANCE), any(), any());
    }

    public void testHasLogsUsage() {
        var metadata = DataStreamTestHelper.getClusterStateWithDataStreams(List.of(), List.of()).getMetadata();
        assertFalse(LogsPatternUsageService.hasLogsUsage(metadata));
        metadata = DataStreamTestHelper.getClusterStateWithDataStreams(List.of(new Tuple<>("log-app1", 1)), List.of()).getMetadata();
        assertFalse(LogsPatternUsageService.hasLogsUsage(metadata));
        metadata = DataStreamTestHelper.getClusterStateWithDataStreams(List.of(new Tuple<>("logs-app1", 1)), List.of()).getMetadata();
        assertFalse(LogsPatternUsageService.hasLogsUsage(metadata));
        metadata = DataStreamTestHelper.getClusterStateWithDataStreams(List.of(new Tuple<>("log-app1-prod", 1)), List.of()).getMetadata();
        assertFalse(LogsPatternUsageService.hasLogsUsage(metadata));
        metadata = DataStreamTestHelper.getClusterStateWithDataStreams(List.of(new Tuple<>("logs-app1-prod", 1)), List.of()).getMetadata();
        assertTrue(LogsPatternUsageService.hasLogsUsage(metadata));
        metadata = DataStreamTestHelper.getClusterStateWithDataStreams(
            List.of(new Tuple<>("log-app1-prod", 1), new Tuple<>("logs-app2-prod", 1)),
            List.of()
        ).getMetadata();
        assertTrue(LogsPatternUsageService.hasLogsUsage(metadata));
        metadata = DataStreamTestHelper.getClusterStateWithDataStreams(
            List.of(new Tuple<>("log-app1", 1), new Tuple<>("logs-app2-prod", 1)),
            List.of()
        ).getMetadata();
        assertTrue(LogsPatternUsageService.hasLogsUsage(metadata));
    }

}
