/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.ReindexDataStreamAction;
import org.elasticsearch.action.datastreams.ReindexDataStreamAction.ReindexDataStreamRequest;
import org.elasticsearch.action.datastreams.ReindexDataStreamAction.ReindexDataStreamResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class ReindexDataStreamTransportAction extends HandledTransportAction<ReindexDataStreamRequest, ReindexDataStreamResponse> {
    private final PersistentTasksService persistentTasksService;

    @Inject
    public ReindexDataStreamTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        PersistentTasksService persistentTasksService
    ) {
        super(
            ReindexDataStreamAction.NAME,
            true,
            transportService,
            actionFilters,
            ReindexDataStreamRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC)
        );
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    protected void doExecute(Task task, ReindexDataStreamRequest request, ActionListener<ReindexDataStreamResponse> listener) {
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        listener.onResponse(new ReindexDataStreamResponse());
    }
}
