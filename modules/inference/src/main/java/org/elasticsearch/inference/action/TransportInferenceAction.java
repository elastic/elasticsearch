/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.results.SparseEmbeddingResult;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

public class TransportInferenceAction extends HandledTransportAction<InferenceAction.Request, InferenceAction.Response> {

    @Inject
    public TransportInferenceAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(InferenceAction.NAME, transportService, actionFilters, InferenceAction.Request::new);
    }

    @Override
    protected void doExecute(Task task, InferenceAction.Request request, ActionListener<InferenceAction.Response> listener) {
        listener.onResponse(new InferenceAction.Response(new SparseEmbeddingResult(List.of())));
    }
}
