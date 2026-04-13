/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.utils;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.transform.CpsCredentialService;

import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Wraps {@link ClientHelper#executeWithHeadersAsync} to additionally inject a CPS cloud token
 * transient into the stashed {@link ThreadContext}. The transient is only visible during the
 * action's execution on the coordinator node and is NOT serialized across nodes — the CPS
 * transport interceptor reads it locally to mint request-scoped tokens for cross-project requests.
 */
public final class CpsCredentialHelper {

    private CpsCredentialHelper() {}

    public static <Request extends ActionRequest, Response extends ActionResponse> void executeWithHeadersAsync(
        Map<String, String> headers,
        String origin,
        Client client,
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener,
        CpsCredentialService cpsCredentialService,
        @Nullable String storedCredential
    ) {
        if (storedCredential == null) {
            ClientHelper.executeWithHeadersAsync(headers, origin, client, action, request, listener);
            return;
        }

        var threadContext = client.threadPool().getThreadContext();
        ClientHelper.executeWithHeadersAsync(threadContext, headers, origin, request, listener, (r, l) -> {
            cpsCredentialService.injectCpsCredential(threadContext, storedCredential);
            client.execute(action, r, l);
        });
    }

    public static <Request, Response> void executeWithHeadersAsync(
        ThreadContext threadContext,
        Map<String, String> headers,
        String origin,
        Request request,
        ActionListener<Response> listener,
        BiConsumer<Request, ActionListener<Response>> consumer,
        CpsCredentialService cpsCredentialService,
        @Nullable String storedCredential
    ) {
        if (storedCredential == null) {
            ClientHelper.executeWithHeadersAsync(threadContext, headers, origin, request, listener, consumer);
            return;
        }

        ClientHelper.executeWithHeadersAsync(threadContext, headers, origin, request, listener, (r, l) -> {
            cpsCredentialService.injectCpsCredential(threadContext, storedCredential);
            consumer.accept(r, l);
        });
    }
}
