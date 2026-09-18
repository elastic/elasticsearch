/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action.datafeed;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.action.IsolateDatafeedAction;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MachineLearningExtension;
import org.elasticsearch.xpack.ml.annotations.AnnotationPersister;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
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
        DatafeedManager datafeedManager = new DatafeedManager(
            mock(DatafeedConfigProvider.class),
            mock(JobConfigProvider.class),
            NamedXContentRegistry.EMPTY,
            settings,
            clusterService,
            client,
            mock(MachineLearningExtension.class),
            mock(AnomalyDetectionAuditor.class),
            mock(AnnotationPersister.class),
            mock(JobResultsProvider.class)
        );
        doAnswer(invocation -> { return null; }).when(client)
            .execute(same(IsolateDatafeedAction.INSTANCE), any(IsolateDatafeedAction.Request.class), any(ActionListener.class));
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

        action.masterOperation(null, request, org.elasticsearch.cluster.ClusterState.EMPTY_STATE, ActionListener.noop());

        verify(client, times(1)).execute(
            same(IsolateDatafeedAction.INSTANCE),
            org.mockito.ArgumentMatchers.<IsolateDatafeedAction.Request>argThat(
                isolateRequest -> isolateRequest.getDatafeedId().equals("datafeed-id")
            ),
            any()
        );
    }
}
