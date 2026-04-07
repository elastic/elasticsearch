/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.telemetry.InferenceStats;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.action.task.StreamingTaskManager;
import org.elasticsearch.xpack.inference.registry.InferenceEndpointRegistry;

public class TransportInferenceAction extends BaseTransportInferenceAction<InferenceAction.Request> {

    @Inject
    public TransportInferenceAction(
        TransportService transportService,
        ActionFilters actionFilters,
        XPackLicenseState licenseState,
        InferenceEndpointRegistry inferenceEndpointRegistry,
        InferenceServiceRegistry serviceRegistry,
        InferenceStats inferenceStats,
        StreamingTaskManager streamingTaskManager,
        NodeClient nodeClient,
        ThreadPool threadPool
    ) {
        super(
            InferenceAction.NAME,
            transportService,
            actionFilters,
            licenseState,
            inferenceEndpointRegistry,
            serviceRegistry,
            inferenceStats,
            streamingTaskManager,
            InferenceAction.Request::new,
            nodeClient,
            threadPool
        );
    }

    @Override
    protected boolean isInvalidTaskTypeForInferenceEndpoint(InferenceAction.Request request, Model model) {
        return false;
    }

    @Override
    protected ElasticsearchStatusException createInvalidTaskTypeException(InferenceAction.Request request, Model model) {
        return null;
    }

    @Override
    protected void doInference(
        Model model,
        InferenceAction.Request request,
        InferenceService service,
        ActionListener<InferenceServiceResults> listener
    ) {
        service.infer(
            model,
            request.getQuery(),
            request.getReturnDocuments(),
            request.getTopN(),
            request.getInput(),
            request.isStreaming(),
            request.getTaskSettings(),
            request.getInputType(),
            request.getInferenceTimeout(),
            listener
        );
    }
}
