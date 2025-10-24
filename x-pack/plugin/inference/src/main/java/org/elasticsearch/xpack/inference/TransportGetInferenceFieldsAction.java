/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsAction;

import java.util.Map;

public class TransportGetInferenceFieldsAction extends HandledTransportAction<
    GetInferenceFieldsAction.Request,
    GetInferenceFieldsAction.Response> {

    private final Client client;

    @Inject
    public TransportGetInferenceFieldsAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(
            GetInferenceFieldsAction.NAME,
            transportService,
            actionFilters,
            GetInferenceFieldsAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = client;
    }

    @Override
    protected void doExecute(
        Task task,
        GetInferenceFieldsAction.Request request,
        ActionListener<GetInferenceFieldsAction.Response> listener
    ) {
        listener.onResponse(new GetInferenceFieldsAction.Response(Map.of(), Map.of()));
    }
}
