/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportDeleteByQueryAction extends HandledTransportAction<DeleteByQueryRequest, BulkByScrollResponse> {

    private final Client client;
    private final ScriptService scriptService;
    private final TransportService transportService;

    @Inject
    public TransportDeleteByQueryAction(
        ActionFilters actionFilters,
        Client client,
        TransportService transportService,
        ScriptService scriptService
    ) {
        super(DeleteByQueryAction.NAME, transportService, actionFilters, DeleteByQueryRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.client = client;
        this.scriptService = scriptService;
        this.transportService = transportService;
    }

    @Override
    public void doExecute(Task task, DeleteByQueryRequest request, ActionListener<BulkByScrollResponse> listener) {
        BulkByScrollTask bulkByScrollTask = (BulkByScrollTask) task;
        BulkByScrollParallelizationHelper.startSlicedAction(
            request,
            bulkByScrollTask,
            DeleteByQueryAction.INSTANCE,
            listener,
            client,
            transportService.getLocalNode(),
            () -> {
                ParentTaskAssigningClient assigningClient = new ParentTaskAssigningClient(
                    client,
                    transportService.getLocalNode(),
                    bulkByScrollTask
                );
                new AsyncDeleteByQueryAction(bulkByScrollTask, logger, assigningClient, request, scriptService, listener).start();
            }
        );
    }
}
