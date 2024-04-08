/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.PostDataAction;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.TimeRange;
import org.elasticsearch.xpack.ml.job.task.JobTask;

import java.io.InputStream;
import java.util.Optional;

public class TransportPostDataAction extends TransportJobTaskAction<PostDataAction.Request, PostDataAction.Response> {

    private final AnalysisRegistry analysisRegistry;

    @Inject
    public TransportPostDataAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        AutodetectProcessManager processManager,
        AnalysisRegistry analysisRegistry
    ) {
        super(
            PostDataAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            PostDataAction.Request::new,
            PostDataAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            processManager
        );
        // ThreadPool.Names.SAME, because operations is executed by autodetect worker thread
        this.analysisRegistry = analysisRegistry;
    }

    @Override
    protected void taskOperation(
        CancellableTask actionTask,
        PostDataAction.Request request,
        JobTask task,
        ActionListener<PostDataAction.Response> listener
    ) {
        TimeRange timeRange = TimeRange.builder().startTime(request.getResetStart()).endTime(request.getResetEnd()).build();
        DataLoadParams params = new DataLoadParams(timeRange, Optional.ofNullable(request.getDataDescription()));
        try (InputStream contentStream = request.getContent().streamInput()) {
            processManager.processData(task, analysisRegistry, contentStream, request.getXContentType(), params, (dataCounts, e) -> {
                if (dataCounts != null) {
                    listener.onResponse(new PostDataAction.Response(dataCounts));
                } else {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
