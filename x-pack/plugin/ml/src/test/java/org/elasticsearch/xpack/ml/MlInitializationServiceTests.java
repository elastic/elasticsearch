/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MlInitializationServiceTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("my_cluster");

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private Client client;
    private MlAssignmentNotifier mlAssignmentNotifier;

    @Before
    public void setUpMocks() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        threadPool = deterministicTaskQueue.getThreadPool();
        clusterService = mock(ClusterService.class);
        client = mock(Client.class);
        mlAssignmentNotifier = mock(MlAssignmentNotifier.class);

        when(clusterService.getClusterName()).thenReturn(CLUSTER_NAME);

        @SuppressWarnings("unchecked")
        ActionFuture<GetSettingsResponse> getSettingsResponseActionFuture = mock(ActionFuture.class);
        when(getSettingsResponseActionFuture.actionGet()).thenReturn(new GetSettingsResponse(Map.of(), Map.of()));
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        when(indicesAdminClient.getSettings(any())).thenReturn(getSettingsResponseActionFuture);
        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(client.admin()).thenReturn(adminClient);
        @SuppressWarnings("unchecked")
        ActionFuture<GetSettingsResponse> actionFuture = mock(ActionFuture.class);
        when(actionFuture.actionGet()).thenReturn(new GetSettingsResponse(Map.of(), Map.of()));
        when(client.execute(eq(GetSettingsAction.INSTANCE), any())).thenReturn(actionFuture);
    }

    public void testInitialize() {
        MlInitializationService initializationService = new MlInitializationService(
            Settings.EMPTY,
            threadPool,
            clusterService,
            client,
            mlAssignmentNotifier,
            true,
            true,
            true
        );
        initializationService.onMaster();
        assertThat(initializationService.getDailyMaintenanceService().isStarted(), is(true));
    }

    public void testInitialize_noMasterNode() {
        MlInitializationService initializationService = new MlInitializationService(
            Settings.EMPTY,
            threadPool,
            clusterService,
            client,
            mlAssignmentNotifier,
            true,
            true,
            true
        );
        initializationService.offMaster();
        assertThat(initializationService.getDailyMaintenanceService().isStarted(), is(false));
    }

    public void testNodeGoesFromMasterToNonMasterAndBack() {
        MlDailyMaintenanceService initialDailyMaintenanceService = mock(MlDailyMaintenanceService.class);

        MlInitializationService initializationService = new MlInitializationService(
            client,
            threadPool,
            initialDailyMaintenanceService,
            clusterService
        );
        initializationService.offMaster();
        verify(initialDailyMaintenanceService).stop();

        initializationService.onMaster();
        verify(initialDailyMaintenanceService).start();
    }
}
