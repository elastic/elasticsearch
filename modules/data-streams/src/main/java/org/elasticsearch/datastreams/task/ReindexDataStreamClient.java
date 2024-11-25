/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.task;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.support.AbstractClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Assertions;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ReindexDataStreamClient extends AbstractClient {
    private static final String RUN_AS_USER_HEADER = "es-security-runas-user";
    private static final String AUTHENTICATION_KEY = "_xpack_security_authentication";
    private static final String THREAD_CTX_KEY = "_xpack_security_secondary_authc";

    private static final Set<String> SECURITY_HEADER_FILTERS = Set.of(RUN_AS_USER_HEADER, AUTHENTICATION_KEY, THREAD_CTX_KEY);

    private final Client client;
    private final Map<String, String> headers;

    public ReindexDataStreamClient(Client client, Map<String, String> headers) {
        super(client.settings(), client.threadPool());
        this.client = client;
        this.headers = headers;
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        executeWithHeadersAsync(headers, client, action, request, listener);
    }

    private static <Request extends ActionRequest, Response extends ActionResponse> void executeWithHeadersAsync(
        Map<String, String> headers,
        Client client,
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        executeWithHeadersAsync(client.threadPool().getThreadContext(), headers, request, listener, (r, l) -> client.execute(action, r, l));
    }

    private static <Request, Response> void executeWithHeadersAsync(
        ThreadContext threadContext,
        Map<String, String> headers,
        Request request,
        ActionListener<Response> listener,
        BiConsumer<Request, ActionListener<Response>> consumer
    ) {
        // No need to rewrite authentication header because it will be handled by Security Interceptor
        final Map<String, String> filteredHeaders = filterSecurityHeaders(headers);
        filteredHeaders.forEach((k, v) -> System.out.printf("%-15s : %s%n", k, v));
        // No headers (e.g. security not installed/in use) so execute as origin
        if (filteredHeaders.isEmpty()) {
            consumer.accept(request, listener);
        } else {
            // Otherwise stash the context and copy in the saved headers before executing
            final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(false);
            try (ThreadContext.StoredContext ignore = stashWithHeaders(threadContext, filteredHeaders)) {
                consumer.accept(request, new ContextPreservingActionListener<>(supplier, listener));
            }
        }
    }

    private static Map<String, String> filterSecurityHeaders(Map<String, String> headers) {
        if (SECURITY_HEADER_FILTERS.containsAll(headers.keySet())) {
            // fast-track to skip the artifice below
            return headers;
        } else {
            return Objects.requireNonNull(headers)
                .entrySet()
                .stream()
                .filter(e -> SECURITY_HEADER_FILTERS.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }

    private static ThreadContext.StoredContext stashWithHeaders(ThreadContext threadContext, Map<String, String> headers) {
        final ThreadContext.StoredContext storedContext = threadContext.stashContext();
        assertNoAuthorizationHeader(headers);
        threadContext.copyHeaders(headers.entrySet());
        return storedContext;
    }

    private static final Pattern authorizationHeaderPattern = Pattern.compile(
        "\\s*" + Pattern.quote("Authorization") + "\\s*",
        Pattern.CASE_INSENSITIVE
    );

    private static void assertNoAuthorizationHeader(Map<String, String> headers) {
        if (Assertions.ENABLED) {
            for (String header : headers.keySet()) {
                if (authorizationHeaderPattern.matcher(header).find()) {
                    assert false : "headers contain \"Authorization\"";
                }
            }
        }
    }
}
