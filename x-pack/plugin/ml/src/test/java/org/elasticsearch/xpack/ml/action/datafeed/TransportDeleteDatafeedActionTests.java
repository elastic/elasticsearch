/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action.datafeed;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.action.IsolateDatafeedAction;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportDeleteDatafeedActionTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testForceDeleteIsolatesDatafeedOnceBeforeDeleting() {
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        ClusterService clusterService = mock(ClusterService.class);
        Settings settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(settings, Set.of(MachineLearning.REQUIRE_ROLLBACK_SNAPSHOT_BEFORE_SCOPE_CHANGE))
        );
        DatafeedManager datafeedManager = mock(DatafeedManager.class);
        doAnswer(invocation -> {
            ActionListener<IsolateDatafeedAction.Response> isolateListener = invocation.getArgument(2);
            isolateListener.onResponse(new IsolateDatafeedAction.Response(true));
            return null;
        }).when(client).execute(same(IsolateDatafeedAction.INSTANCE), any(IsolateDatafeedAction.Request.class), any(ActionListener.class));
        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> deleteListener = invocation.getArgument(2);
            deleteListener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(datafeedManager).deleteDatafeed(any(), any(), any());
        TransportDeleteDatafeedAction action = new TransportDeleteDatafeedAction(
            mock(TransportService.class),
            clusterService,
            threadPool,
            ActionFilters.EMPTY,
            client,
            mock(PersistentTasksService.class),
            datafeedManager,
            mock(ProjectResolver.class)
        );
        org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction.Request request =
            new org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction.Request("datafeed-id");
        request.setForce(true);

        AtomicBoolean completed = new AtomicBoolean();
        action.masterOperation(null, request, ClusterState.EMPTY_STATE, ActionListener.wrap(response -> completed.set(true), e -> {
            throw new AssertionError(e);
        }));
        assertTrue(completed.get());

        verify(client, times(1)).execute(
            same(IsolateDatafeedAction.INSTANCE),
            org.mockito.ArgumentMatchers.<IsolateDatafeedAction.Request>argThat(
                isolateRequest -> isolateRequest.getDatafeedId().equals("datafeed-id")
            ),
            any()
        );
        verify(datafeedManager, times(1)).deleteDatafeed(eq(request), any(), any());
    }
}
