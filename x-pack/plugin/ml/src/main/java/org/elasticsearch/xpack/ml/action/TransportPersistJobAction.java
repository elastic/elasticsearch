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
import org.elasticsearch.xpack.core.ml.action.PersistJobAction;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;

public class TransportPersistJobAction extends TransportJobTaskAction<PersistJobAction.Request, PersistJobAction.Response> {

    @Inject
    public TransportPersistJobAction(TransportService transportService, ClusterService clusterService, ActionFilters actionFilters,
                                     AutodetectProcessManager processManager) {
        super(PersistJobAction.NAME, clusterService, transportService, actionFilters,
            PersistJobAction.Request::new, PersistJobAction.Response::new, ThreadPool.Names.SAME, processManager);
        // ThreadPool.Names.SAME, because operations is executed by autodetect worker thread
    }

    @Override
    protected void taskOperation(PersistJobAction.Request request, TransportOpenJobAction.JobTask task,
                                 ActionListener<PersistJobAction.Response> listener) {

        processManager.persistJob(task, e -> {
            if (e == null) {
                listener.onResponse(new PersistJobAction.Response(true));
            } else {
                listener.onFailure(e);
            }
        });
    }
}
