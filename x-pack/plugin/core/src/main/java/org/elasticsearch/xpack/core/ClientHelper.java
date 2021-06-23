/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility class to help with the execution of requests made using a {@link Client} such that they
 * have the origin as a transient and listeners have the appropriate context upon invocation
 */
public final class ClientHelper {

    private static Pattern authorizationHeaderPattern = Pattern.compile("\\s*" + Pattern.quote("Authorization") + "\\s*",
            Pattern.CASE_INSENSITIVE);

    public static void assertNoAuthorizationHeader(Map<String, String> headers) {
        if (org.elasticsearch.Assertions.ENABLED) {
            for (String header : headers.keySet()) {
                if (authorizationHeaderPattern.matcher(header).find()) {
                    assert false : "headers contain \"Authorization\"";
                }
            }
        }
    }

    /**
     * List of headers that are related to security
     */
    public static final Set<String> SECURITY_HEADER_FILTERS =
        Sets.newHashSet(
            AuthenticationServiceField.RUN_AS_USER_HEADER,
            AuthenticationField.AUTHENTICATION_KEY,
            SecondaryAuthentication.THREAD_CTX_KEY);

    /**
     * Leaves only headers that are related to security and filters out the rest.
     *
     * @param headers Headers to be filtered
     * @return A portion of entries that are related to security
     */
    public static Map<String, String> filterSecurityHeaders(Map<String, String> headers) {
        if (SECURITY_HEADER_FILTERS.containsAll(headers.keySet())) {
            // fast-track to skip the artifice below
            return headers;
        } else {
            return Objects.requireNonNull(headers).entrySet().stream()
                    .filter(e -> SECURITY_HEADER_FILTERS.contains(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }

    /**
     * .
     * @deprecated use ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME
     */
    @Deprecated
    public static final String ACTION_ORIGIN_TRANSIENT_NAME = ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME;
    public static final String SECURITY_ORIGIN = "security";
    public static final String WATCHER_ORIGIN = "watcher";
    public static final String ML_ORIGIN = "ml";
    public static final String INDEX_LIFECYCLE_ORIGIN = "index_lifecycle";
    public static final String MONITORING_ORIGIN = "monitoring";
    public static final String DEPRECATION_ORIGIN = "deprecation";
    public static final String ROLLUP_ORIGIN = "rollup";
    public static final String ENRICH_ORIGIN = "enrich";
    public static final String TRANSFORM_ORIGIN = "transform";
    public static final String ASYNC_SEARCH_ORIGIN = "async_search";
    public static final String IDP_ORIGIN = "idp";
    public static final String STACK_ORIGIN = "stack";
    public static final String SEARCHABLE_SNAPSHOTS_ORIGIN = "searchable_snapshots";
    public static final String LOGSTASH_MANAGEMENT_ORIGIN = "logstash_management";
    public static final String FLEET_ORIGIN = "fleet";

    private ClientHelper() {}

    /**
     * Returns a client that will always set the appropriate origin and ensure the proper context is restored by listeners
     * @deprecated use {@link OriginSettingClient} instead
     */
    @Deprecated
    public static Client clientWithOrigin(Client client, String origin) {
        return new OriginSettingClient(client, origin);
    }

    /**
     * Executes a consumer after setting the origin and wrapping the listener so that the proper context is restored
     */
    public static <Request extends ActionRequest, Response extends ActionResponse> void executeAsyncWithOrigin(
            ThreadContext threadContext, String origin, Request request, ActionListener<Response> listener,
            BiConsumer<Request, ActionListener<Response>> consumer) {
        final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(false);
        try (ThreadContext.StoredContext ignore = threadContext.stashWithOrigin(origin)) {
            consumer.accept(request, new ContextPreservingActionListener<>(supplier, listener));
        }
    }

    /**
     * Executes an asynchronous action using the provided client. The origin is set in the context and the listener
     * is wrapped to ensure the proper context is restored
     */
    public static <Request extends ActionRequest, Response extends ActionResponse,
            RequestBuilder extends ActionRequestBuilder<Request, Response>> void executeAsyncWithOrigin(
        Client client, String origin, ActionType<Response> action, Request request,
        ActionListener<Response> listener) {
        final ThreadContext threadContext = client.threadPool().getThreadContext();
        final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(false);
        try (ThreadContext.StoredContext ignore = threadContext.stashWithOrigin(origin)) {
            client.execute(action, request, new ContextPreservingActionListener<>(supplier, listener));
        }
    }

    /**
     * Execute a client operation and return the response, try to run an action
     * with least privileges, when headers exist
     *
     * @param headers
     *            Request headers, ideally including security headers
     * @param origin
     *            The origin to fall back to if there are no security headers
     * @param client
     *            The client used to query
     * @param supplier
     *            The action to run
     * @return An instance of the response class
     */
    public static <T extends ActionResponse> T executeWithHeaders(Map<String, String> headers, String origin, Client client,
            Supplier<T> supplier) {
        Map<String, String> filteredHeaders = filterSecurityHeaders(headers);

        // no security headers, we will have to use the xpack internal user for
        // our execution by specifying the origin
        if (filteredHeaders.isEmpty()) {
            try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(origin)) {
                return supplier.get();
            }
        } else {
            try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashContext()) {
                client.threadPool().getThreadContext().copyHeaders(filteredHeaders.entrySet());
                return supplier.get();
            }
        }
    }

    /**
     * Execute a client operation asynchronously, try to run an action with
     * least privileges, when headers exist
     *
     * @param headers
     *            Request headers, ideally including security headers
     * @param origin
     *            The origin to fall back to if there are no security headers
     * @param action
     *            The action to execute
     * @param request
     *            The request object for the action
     * @param listener
     *            The listener to call when the action is complete
     */
    public static <Request extends ActionRequest, Response extends ActionResponse>
    void executeWithHeadersAsync(Map<String, String> headers, String origin, Client client, ActionType<Response> action, Request request,
                                 ActionListener<Response> listener) {
        final Map<String, String> filteredHeaders = filterSecurityHeaders(headers);
        final ThreadContext threadContext = client.threadPool().getThreadContext();
        // No headers (e.g. security not installed/in use) so execute as origin
        if (filteredHeaders.isEmpty()) {
            ClientHelper.executeAsyncWithOrigin(client, origin, action, request, listener);
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
        assertNoAuthorizationHeader(headers);
        threadContext.copyHeaders(headers.entrySet());
        return storedContext;
    }
}
