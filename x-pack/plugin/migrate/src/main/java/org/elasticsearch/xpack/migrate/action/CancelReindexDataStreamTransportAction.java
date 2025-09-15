/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.migrate.action.CancelReindexDataStreamAction.Request;

public class CancelReindexDataStreamTransportAction extends HandledTransportAction<Request, AcknowledgedResponse> {
    private final PersistentTasksService persistentTasksService;

    @Inject
    public CancelReindexDataStreamTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        PersistentTasksService persistentTasksService
    ) {
        super(CancelReindexDataStreamAction.NAME, transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<AcknowledgedResponse> listener) {
        String index = request.getIndex();
        String persistentTaskId = ReindexDataStreamAction.TASK_ID_PREFIX + index;
        /*
         * This removes the persistent task from the cluster state and results in the running task being cancelled (but not removed from
         * the task manager). The running task is removed from the task manager in ReindexDataStreamTask::onCancelled, which is called as
         * as result of this.
         */
        persistentTasksService.sendRemoveRequest(persistentTaskId, TimeValue.MAX_VALUE, new ActionListener<>() {
            @Override
            public void onResponse(PersistentTasksCustomMetadata.PersistentTask<?> persistentTask) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
