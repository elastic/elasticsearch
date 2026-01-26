/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

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
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.util.HashMap;
import java.util.Map;

public class TransportGetDatafeedCrossProjectHeadersAction extends TransportAction<
    GetDatafeedCrossProjectHeadersAction.Request,
    GetDatafeedCrossProjectHeadersAction.Response> {
    private static final Logger logger = LogManager.getLogger(TransportGetDatafeedCrossProjectHeadersAction.class);

    private final ThreadPool threadPool;

    @Inject
    public TransportGetDatafeedCrossProjectHeadersAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool
    ) {
        super(
            GetDatafeedCrossProjectHeadersAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(
        Task task,
        GetDatafeedCrossProjectHeadersAction.Request request,
        ActionListener<GetDatafeedCrossProjectHeadersAction.Response> listener
    ) {
        final DatafeedConfig datafeedConfig = request.getDatafeedConfig();

        if (datafeedConfig.getHeaders().containsKey("_security_serverless_request_scoped_credential")) {
            logger.warn("Datafeed config contains serverless credential header, skipping...");
            listener.onResponse(GetDatafeedCrossProjectHeadersAction.Response.INSTANCE);
            return;
        }

        final ThreadContext threadContext = threadPool.getThreadContext();
        final Map<String, String> previousHeaders = datafeedConfig.getHeaders();
        final String securityServerlessRequestScopedCredential = threadContext.getHeaders()
            .get("_security_serverless_request_scoped_credential");
        final Map<String, String> mergedHeaders = new HashMap<>(previousHeaders);
        if (securityServerlessRequestScopedCredential != null) {
            final Map<String, String> requestScopedCredential = Map.of(
                "_security_serverless_request_scoped_credential",
                securityServerlessRequestScopedCredential
            );
            mergedHeaders.putAll(requestScopedCredential);
        }

        logger.info("Previous headers: {} and current headers {}", previousHeaders, mergedHeaders);
        datafeedConfig.setHeaders(Map.copyOf(mergedHeaders));

        listener.onResponse(GetDatafeedCrossProjectHeadersAction.Response.INSTANCE);
    }
}
