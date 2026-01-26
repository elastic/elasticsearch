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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

import java.util.HashMap;
import java.util.Map;

public class TransportGetTransformCrossProjectHeadersAction extends TransportAction<
    GetTransformCrossProjectHeadersAction.Request,
    GetTransformCrossProjectHeadersAction.Response> {
    private static final Logger logger = LogManager.getLogger(TransportGetTransformCrossProjectHeadersAction.class);

    private final ThreadPool threadPool;

    @Inject
    public TransportGetTransformCrossProjectHeadersAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool
    ) {
        super(
            GetTransformCrossProjectHeadersAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.threadPool = threadPool;
    }

    public static void executeWithCrossProjectHeaders(Client client, TransformConfig transformConfig, ActionListener<Void> listener) {
        // force fresh token by removing any existing request-scoped credential
        Map<String, String> currentHeaders = copyWithoutRequestScopedCredential(transformConfig.getHeaders());
        ClientHelper.executeWithHeadersAsync(
            currentHeaders,
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            GetTransformCrossProjectHeadersAction.INSTANCE,
            new GetTransformCrossProjectHeadersAction.Request(transformConfig),
            ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure)
        );
    }

    private static Map<String, String> copyWithoutRequestScopedCredential(Map<String, String> headers) {
        if (headers.containsKey("_security_serverless_request_scoped_credential") == false) {
            return headers;
        }
        var copy = new HashMap<>(headers);
        copy.remove("_security_serverless_request_scoped_credential");
        return copy;
    }

    @Override
    protected void doExecute(
        Task task,
        GetTransformCrossProjectHeadersAction.Request request,
        ActionListener<GetTransformCrossProjectHeadersAction.Response> listener
    ) {
        final TransformConfig transformConfig = request.getTransformConfig();

        if (transformConfig.getHeaders().containsKey("_security_serverless_request_scoped_credential")) {
            logger.warn("Transform config contains serverless credential header, skipping...");
            listener.onResponse(GetTransformCrossProjectHeadersAction.Response.INSTANCE);
            return;
        }

        final ThreadContext threadContext = threadPool.getThreadContext();
        final Map<String, String> previousHeaders = transformConfig.getHeaders();
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
        transformConfig.setHeaders(Map.copyOf(mergedHeaders));

        listener.onResponse(GetTransformCrossProjectHeadersAction.Response.INSTANCE);
    }
}
