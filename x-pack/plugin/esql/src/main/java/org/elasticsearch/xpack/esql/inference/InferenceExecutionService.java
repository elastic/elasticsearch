/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

public class InferenceExecutionService {
    private final TransportService transportService;

    public InferenceExecutionService(TransportService transportService) {
        this.transportService = transportService;
    }

    public void doInference(InferenceAction.Request request, ActionListener<InferenceAction.Response> listener) {
        transportService.sendRequest(
            transportService.getLocalNode(),
            InferenceAction.NAME,
            request,
            new ActionListenerResponseHandler<>(listener, InferenceAction.Response::new, transportService.getThreadPool().executor(ThreadPool.Names.SEARCH))
        );
    }
}
