/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlStreamDeleteAction;
import org.elasticsearch.xpack.esql.streaming.DeleteStreamResultRequest;

/**
 * Deletes a stored stream, the counterpart of {@code TransportDeleteAsyncResultAction}. {@code RBACEngine} does not authorize this
 * action against any index, so it must check that the caller started the stream before deleting it. Nothing is stored yet, so no
 * stream exists and every id is not found, which is also what an unknown stream will get once streams are stored.
 */
public class TransportEsqlStreamDeleteAction extends HandledTransportAction<DeleteStreamResultRequest, AcknowledgedResponse> {

    @Inject
    public TransportEsqlStreamDeleteAction(TransportService transportService, ActionFilters actionFilters) {
        super(
            EsqlStreamDeleteAction.NAME,
            transportService,
            actionFilters,
            DeleteStreamResultRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    @Override
    protected void doExecute(Task task, DeleteStreamResultRequest request, ActionListener<AcknowledgedResponse> listener) {
        listener.onFailure(new ResourceNotFoundException(request.id()));
    }
}
