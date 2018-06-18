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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.UpdateProcessAction;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.job.process.autodetect.UpdateParams;

import java.io.IOException;

public class TransportUpdateProcessAction extends TransportJobTaskAction<UpdateProcessAction.Request, UpdateProcessAction.Response> {

    @Inject
    public TransportUpdateProcessAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                                        ClusterService clusterService, ActionFilters actionFilters,
                                        AutodetectProcessManager processManager) {
        super(settings, UpdateProcessAction.NAME, threadPool, clusterService, transportService, actionFilters,
            UpdateProcessAction.Request::new, UpdateProcessAction.Response::new, ThreadPool.Names.SAME, processManager);
        // ThreadPool.Names.SAME, because operations is executed by autodetect worker thread
    }

    @Override
    protected UpdateProcessAction.Response readTaskResponse(StreamInput in) throws IOException {
        UpdateProcessAction.Response response = new UpdateProcessAction.Response();
        response.readFrom(in);
        return response;
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
