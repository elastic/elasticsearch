/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.UpdateProcessAction;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.job.process.autodetect.UpdateParams;

public class TransportUpdateProcessAction extends TransportJobTaskAction<UpdateProcessAction.Request, UpdateProcessAction.Response> {

    @Inject
    public TransportUpdateProcessAction(TransportService transportService, ClusterService clusterService,
                                        ActionFilters actionFilters, AutodetectProcessManager processManager) {
        super(UpdateProcessAction.NAME, clusterService, transportService, actionFilters,
            UpdateProcessAction.Request::new, UpdateProcessAction.Response::new, ThreadPool.Names.SAME, processManager);
        // ThreadPool.Names.SAME, because operations is executed by autodetect worker thread
    }

    @Override
    protected void taskOperation(UpdateProcessAction.Request request, TransportOpenJobAction.JobTask task,
                                 ActionListener<UpdateProcessAction.Response> listener) {
        UpdateParams updateParams = UpdateParams.builder(request.getJobId())
                .modelPlotConfig(request.getModelPlotConfig())
                .detectorUpdates(request.getDetectorUpdates())
                .filter(request.getFilter())
                .updateScheduledEvents(request.isUpdateScheduledEvents())
                .build();

        try {
            processManager.writeUpdateProcessMessage(task, updateParams,
                    e -> {
                        if (e == null) {
                            listener.onResponse(new UpdateProcessAction.Response());
                        } else {
                            listener.onFailure(e);
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
