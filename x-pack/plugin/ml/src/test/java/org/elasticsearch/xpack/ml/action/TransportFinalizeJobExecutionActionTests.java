/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.FinalizeJobExecutionAction;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TransportFinalizeJobExecutionActionTests extends ESTestCase {

    public void testOperation_noJobsInClusterState() {
        ClusterService clusterService = mock(ClusterService.class);
        TransportFinalizeJobExecutionAction action = createAction(clusterService);

        ClusterState clusterState = ClusterState.builder(new ClusterName("finalize-job-action-tests")).build();

        FinalizeJobExecutionAction.Request request = new FinalizeJobExecutionAction.Request(new String[]{"index-job1", "index-job2"});
        AtomicReference<AcknowledgedResponse> ack = new AtomicReference<>();
        action.masterOperation(request, clusterState, ActionListener.wrap(
                ack::set,
                e -> fail(e.getMessage())
        ));

        assertTrue(ack.get().isAcknowledged());
        verify(clusterService, never()).submitStateUpdateTask(any(), any());
    }

    public void testOperation_jobInClusterState() {
        ClusterService clusterService = mock(ClusterService.class);
        TransportFinalizeJobExecutionAction action = createAction(clusterService);

        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        mlBuilder.putJob(BaseMlIntegTestCase.createFareQuoteJob("cs-job").build(new Date()), false);

        ClusterState clusterState = ClusterState.builder(new ClusterName("finalize-job-action-tests"))
                .metaData(MetaData.builder()
                        .putCustom(MlMetadata.TYPE, mlBuilder.build()))
                .build();

        FinalizeJobExecutionAction.Request request = new FinalizeJobExecutionAction.Request(new String[]{"cs-job"});
        AtomicReference<AcknowledgedResponse> ack = new AtomicReference<>();
        action.masterOperation(request, clusterState, ActionListener.wrap(
                ack::set,
                e -> fail(e.getMessage())
        ));

        verify(clusterService, times(1)).submitStateUpdateTask(any(), any());
    }

    private TransportFinalizeJobExecutionAction createAction(ClusterService clusterService) {
        return new TransportFinalizeJobExecutionAction(Settings.EMPTY, mock(TransportService.class), clusterService,
                mock(ThreadPool.class), mock(ActionFilters.class), mock(IndexNameExpressionResolver.class));

    }
}
