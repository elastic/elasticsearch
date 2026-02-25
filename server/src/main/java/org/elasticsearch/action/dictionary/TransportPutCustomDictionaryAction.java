/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.dictionary;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.dictionary.CustomDictionaryService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportPutCustomDictionaryAction extends HandledTransportAction<
    PutCustomDictionaryAction.Request,
    PutCustomDictionaryAction.Response> {

    private final CustomDictionaryService customDictionaryService;

    @Inject
    public TransportPutCustomDictionaryAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(
            PutCustomDictionaryAction.NAME,
            transportService,
            actionFilters,
            PutCustomDictionaryAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.customDictionaryService = new CustomDictionaryService(client);
    }

    @Override
    protected void doExecute(
        Task task,
        PutCustomDictionaryAction.Request request,
        ActionListener<PutCustomDictionaryAction.Response> listener
    ) {
        customDictionaryService.putDictionary(request.id(), request.content(), listener.map(PutCustomDictionaryAction.Response::new));
    }
}
