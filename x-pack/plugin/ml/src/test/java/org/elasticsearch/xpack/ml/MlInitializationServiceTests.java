/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.concurrent.ExecutorService;

import static org.elasticsearch.mock.orig.Mockito.doAnswer;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MlInitializationServiceTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("my_cluster");

    private ThreadPool threadPool;
    private ExecutorService executorService;
    private ClusterService clusterService;
    private Client client;
    private MlAssignmentNotifier mlAssignmentNotifier;

    @Before
    public void setUpMocks() {
        threadPool = mock(ThreadPool.class);
        executorService = mock(ExecutorService.class);
        clusterService = mock(ClusterService.class);
        client = mock(Client.class);
        mlAssignmentNotifier = mock(MlAssignmentNotifier.class);

        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(executorService);

        Scheduler.ScheduledCancellable scheduledCancellable = mock(Scheduler.ScheduledCancellable.class);
        when(threadPool.schedule(any(), any(), any())).thenReturn(scheduledCancellable);

        when(clusterService.getClusterName()).thenReturn(CLUSTER_NAME);
    }

    public void testInitialize() {
        MlInitializationService initializationService =
            new MlInitializationService(Settings.EMPTY, threadPool, clusterService, client, mlAssignmentNotifier);
        initializationService.onMaster();
        assertThat(initializationService.getDailyMaintenanceService().isStarted(), is(true));
    }

    public void testInitialize_noMasterNode() {
        MlInitializationService initializationService =
            new MlInitializationService(Settings.EMPTY, threadPool, clusterService, client, mlAssignmentNotifier);
        initializationService.offMaster();
        assertThat(initializationService.getDailyMaintenanceService().isStarted(), is(false));
    }

    public void testNodeGoesFromMasterToNonMasterAndBack() {
        MlDailyMaintenanceService initialDailyMaintenanceService = mock(MlDailyMaintenanceService.class);

        MlInitializationService initializationService = new MlInitializationService(client, initialDailyMaintenanceService, clusterService);
        initializationService.offMaster();
        verify(initialDailyMaintenanceService).stop();

        initializationService.onMaster();
        verify(initialDailyMaintenanceService).start();
    }
}
