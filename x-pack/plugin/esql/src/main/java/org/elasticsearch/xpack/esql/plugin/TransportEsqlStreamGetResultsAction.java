/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlStreamGetResultAction;
import org.elasticsearch.xpack.esql.streaming.GetStreamResultRequest;

/**
 * Reads a stored stream result on the node that receives the request. {@code RBACEngine} does not authorize this action against any
 * index, so it must check that the caller started the stream before reading it. Nothing is stored yet, so no stream exists and every
 * id is not found, which is also what an unknown stream will get once streams are stored.
 */
public class TransportEsqlStreamGetResultsAction extends TransportAction<GetStreamResultRequest, ActionResponse.Empty> {

    @Inject
    public TransportEsqlStreamGetResultsAction(TransportService transportService, ActionFilters actionFilters) {
        super(EsqlStreamGetResultAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
    }

    @Override
    protected void doExecute(Task task, GetStreamResultRequest request, ActionListener<ActionResponse.Empty> listener) {
        listener.onFailure(new ResourceNotFoundException(request.streamId()));
    }
}
