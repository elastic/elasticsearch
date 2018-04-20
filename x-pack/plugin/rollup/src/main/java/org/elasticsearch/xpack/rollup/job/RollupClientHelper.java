/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.job;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;
import org.elasticsearch.xpack.rollup.Rollup;

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;


/**
 * Helper class to execute actions with authentication headers cached in the rollup job (if they exist, otherwise Origin)
 */
public class RollupClientHelper {

    @SuppressWarnings("try")
    public static <Request extends ActionRequest, Response extends ActionResponse,
            RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void executeAsync(
            Client client, RollupJob job, Action<Request, Response, RequestBuilder> action, Request request,
            ActionListener<Response> listener) {

        Map<String, String> filteredHeaders = job.getHeaders().entrySet().stream()
                .filter(e -> Rollup.HEADER_FILTERS.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        final ThreadContext threadContext = client.threadPool().getThreadContext();

        // No headers (e.g. security not installed/in use) so execute as rollup origin
        if (filteredHeaders.isEmpty()) {
            ClientHelper.executeAsyncWithOrigin(client, ClientHelper.ROLLUP_ORIGIN, action, request, listener);
        } else {
            // Otherwise stash the context and copy in the saved headers before executing
            final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(false);
            try (ThreadContext.StoredContext ignore = stashWithHeaders(threadContext, filteredHeaders)) {
                client.execute(action, request, new ContextPreservingActionListener<>(supplier, listener));
            }
        }
    }

    private static ThreadContext.StoredContext stashWithHeaders(ThreadContext threadContext, Map<String, String> headers) {
        final ThreadContext.StoredContext storedContext = threadContext.stashContext();
        threadContext.copyHeaders(headers.entrySet());
        return storedContext;
    }
}
