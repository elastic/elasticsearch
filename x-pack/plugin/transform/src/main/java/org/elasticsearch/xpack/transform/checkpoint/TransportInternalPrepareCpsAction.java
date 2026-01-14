/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;

import java.util.HashMap;
import java.util.Map;

public class TransportInternalPrepareCpsAction extends TransportAction<
    InternalPrepareCpsAction.Request,
    InternalPrepareCpsAction.Response> {
    private static final Logger logger = LogManager.getLogger(TransportInternalPrepareCpsAction.class);

    private final ThreadPool threadPool;

    @Inject
    public TransportInternalPrepareCpsAction(TransportService transportService, ActionFilters actionFilters, ThreadPool threadPool) {
        super(InternalPrepareCpsAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(
        Task task,
        InternalPrepareCpsAction.Request request,
        ActionListener<InternalPrepareCpsAction.Response> listener
    ) {
        final var transformConfig = request.getTransformConfig();
        if (transformConfig == null) {
            listener.onFailure(new IllegalStateException("InternalPrepareCpsAction must be executed locally"));
            return;
        }

        if (transformConfig.getHeaders().containsKey("_security_serverless_request_scoped_credential")) {
            logger.warn("Transform config contains serverless credential header, skipping...");
            listener.onResponse(InternalPrepareCpsAction.Response.INSTANCE);
            return;
        }

        final ThreadContext threadContext = threadPool.getThreadContext();
        final Map<String, String> previousHeaders = transformConfig.getHeaders();
        final Map<String, String> currentHeaders = ClientHelper.filterSecurityHeaders(threadContext.getHeaders());
        final Map<String, String> mergedHeaders = new HashMap<>(previousHeaders);
        mergedHeaders.putAll(currentHeaders);
        // mergedHeaders.remove(AuthenticationField.SECURITY_TASK_AUTHENTICATING_TOKEN_KEY);

        logger.info("Previous headers: {} and current headers {}", previousHeaders, currentHeaders);
        transformConfig.setHeaders(Map.copyOf(mergedHeaders));

        listener.onResponse(InternalPrepareCpsAction.Response.INSTANCE);
    }
}
