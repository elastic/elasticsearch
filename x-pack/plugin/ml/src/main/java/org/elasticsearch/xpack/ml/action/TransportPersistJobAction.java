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
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.PersistJobAction;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.job.task.JobTask;

public class TransportPersistJobAction extends TransportJobTaskAction<PersistJobAction.Request, PersistJobAction.Response> {

    @Inject
    public TransportPersistJobAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        AutodetectProcessManager processManager
    ) {
        super(
            PersistJobAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            PersistJobAction.Request::new,
            PersistJobAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            processManager
        );
        // ThreadPool.Names.SAME, because operations is executed by autodetect worker thread
    }

    @Override
    protected void taskOperation(
        CancellableTask actionTask,
        PersistJobAction.Request request,
        JobTask task,
        ActionListener<PersistJobAction.Response> listener
    ) {

        processManager.persistJob(task, e -> {
            if (e == null) {
                listener.onResponse(new PersistJobAction.Response(true));
            } else {
                listener.onFailure(e);
            }
        });
    }
}
