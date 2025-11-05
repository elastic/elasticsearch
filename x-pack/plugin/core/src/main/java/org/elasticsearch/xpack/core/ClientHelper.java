/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
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

    private static final Pattern authorizationHeaderPattern = Pattern.compile(
        "\\s*" + Pattern.quote("Authorization") + "\\s*",
        Pattern.CASE_INSENSITIVE
    );

    public static void assertNoAuthorizationHeader(Map<String, String> headers) {
        if (Assertions.ENABLED) {
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
    public static final Set<String> SECURITY_HEADER_FILTERS = Set.of(
        AuthenticationServiceField.RUN_AS_USER_HEADER,
        AuthenticationField.AUTHENTICATION_KEY,
        SecondaryAuthentication.THREAD_CTX_KEY
    );

    /**
     * Filters headers to include only those related to security.
     * <p>
     * This method extracts security-related headers (authentication, run-as user, and
     * secondary authentication) from the provided header map, discarding all other headers.
     * This is useful when you need to preserve security context while removing unrelated headers.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, String> allHeaders = threadContext.getHeaders();
     * Map<String, String> securityHeaders = ClientHelper.filterSecurityHeaders(allHeaders);
     * // securityHeaders now contains only authentication-related headers
     * }</pre>
     *
     * @param headers the headers to be filtered (must not be null)
     * @return a map containing only security-related headers from the input
     * @throws NullPointerException if headers is null
     */
    public static Map<String, String> filterSecurityHeaders(Map<String, String> headers) {
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

    /**
     * Filters security headers and ensures they are safe for persistence across cluster nodes.
     * <p>
     * In addition to {@link #filterSecurityHeaders}, this method checks the version of
     * Authentication objects and rewrites them using the minimum node version in the cluster.
     * This ensures the headers are safe to be persisted as index data and can be loaded
     * by all nodes in the cluster, even those running older versions.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, String> headers = ClientHelper.getPersistableSafeSecurityHeaders(
     *     threadContext,
     *     clusterState
     * );
     * // headers now contain version-compatible authentication data
     * document.setHeaders(headers);
     * }</pre>
     *
     * @param threadContext the thread context containing current headers
     * @param clusterState the cluster state used to determine minimum node version
     * @return security headers rewritten for safe persistence across all cluster nodes
     */
    public static Map<String, String> getPersistableSafeSecurityHeaders(ThreadContext threadContext, ClusterState clusterState) {
        return maybeRewriteAuthenticationHeadersForVersion(
            filterSecurityHeaders(threadContext.getHeaders()),
            key -> new AuthenticationContextSerializer(key).readFromContext(threadContext),
            clusterState.getMinTransportVersion()
        );
    }

    /**
     * Similar to {@link #getPersistableSafeSecurityHeaders(ThreadContext, ClusterState)},
     * but works on a Map of headers instead of ThreadContext.
     */
    public static Map<String, String> getPersistableSafeSecurityHeaders(Map<String, String> headers, ClusterState clusterState) {
        final CheckedFunction<String, Authentication, IOException> authenticationReader = key -> {
            final String authHeader = headers.get(key);
            return authHeader == null ? null : AuthenticationContextSerializer.decode(authHeader);
        };
        return maybeRewriteAuthenticationHeadersForVersion(
            filterSecurityHeaders(headers),
            authenticationReader,
            clusterState.getMinTransportVersion()
        );
    }

    private static Map<String, String> maybeRewriteAuthenticationHeadersForVersion(
        Map<String, String> filteredHeaders,
        CheckedFunction<String, Authentication, IOException> authenticationReader,
        TransportVersion minNodeVersion
    ) {
        Map<String, String> newHeaders = null;

        final String authHeader = maybeRewriteSingleAuthenticationHeaderForVersion(
            authenticationReader,
            AuthenticationField.AUTHENTICATION_KEY,
            minNodeVersion
        );
        if (authHeader != null) {
            newHeaders = new HashMap<>();
            newHeaders.put(AuthenticationField.AUTHENTICATION_KEY, authHeader);
        }

        final String secondaryHeader = maybeRewriteSingleAuthenticationHeaderForVersion(
            authenticationReader,
            SecondaryAuthentication.THREAD_CTX_KEY,
            minNodeVersion
        );
        if (secondaryHeader != null) {
            if (newHeaders == null) {
                newHeaders = new HashMap<>();
            }
            newHeaders.put(SecondaryAuthentication.THREAD_CTX_KEY, secondaryHeader);
        }

        if (newHeaders != null) {
            final HashMap<String, String> mutableHeaders = new HashMap<>(filteredHeaders);
            mutableHeaders.putAll(newHeaders);
            return Map.copyOf(mutableHeaders);
        } else {
            return filteredHeaders;
        }
    }

    private static String maybeRewriteSingleAuthenticationHeaderForVersion(
        CheckedFunction<String, Authentication, IOException> authenticationReader,
        String authenticationHeaderKey,
        TransportVersion minNodeVersion
    ) {
        try {
            final Authentication authentication = authenticationReader.apply(authenticationHeaderKey);
            if (authentication != null && authentication.getEffectiveSubject().getTransportVersion().after(minNodeVersion)) {
                return authentication.maybeRewriteForOlderVersion(minNodeVersion).encode();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read authentication with key [" + authenticationHeaderKey + "]", e);
        }
        return null;
    }

    /**
     * .
     * @deprecated use ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME
     */
    @Deprecated
    public static final String ACTION_ORIGIN_TRANSIENT_NAME = ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME;
    public static final String SECURITY_ORIGIN = "security";
    public static final String SECURITY_PROFILE_ORIGIN = "security_profile";
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
    public static final String PROFILING_ORIGIN = "profiling";
    public static final String STACK_ORIGIN = "stack";
    public static final String SEARCHABLE_SNAPSHOTS_ORIGIN = "searchable_snapshots";
    public static final String LOGSTASH_MANAGEMENT_ORIGIN = "logstash_management";
    public static final String FLEET_ORIGIN = "fleet";
    public static final String ENT_SEARCH_ORIGIN = "enterprise_search";
    public static final String CONNECTORS_ORIGIN = "connectors";
    public static final String INFERENCE_ORIGIN = "inference";
    public static final String APM_ORIGIN = "apm";
    public static final String OTEL_ORIGIN = "otel";
    public static final String REINDEX_DATA_STREAM_ORIGIN = "reindex_data_stream";
    public static final String ESQL_ORIGIN = "esql";

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
     * Executes an asynchronous operation with a specified origin, preserving thread context.
     * <p>
     * This method sets the action origin in the thread context before executing the consumer,
     * and wraps the listener to ensure the original thread context is restored when the
     * operation completes. This is essential for operations that need to run with system
     * privileges while properly handling callbacks.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ClientHelper.executeAsyncWithOrigin(
     *     threadContext,
     *     ClientHelper.SECURITY_ORIGIN,
     *     request,
     *     ActionListener.wrap(
     *         response -> processResponse(response),
     *         exception -> handleError(exception)
     *     ),
     *     (req, listener) -> performOperation(req, listener)
     * );
     * }</pre>
     *
     * @param <Request> the request type
     * @param <Response> the response type
     * @param threadContext the thread context to manage
     * @param origin the origin to set (e.g., "security", "ml", "watcher")
     * @param request the request object
     * @param listener the listener to call with the response or error
     * @param consumer the operation to execute with the request and wrapped listener
     */
    public static <Request, Response> void executeAsyncWithOrigin(
        ThreadContext threadContext,
        String origin,
        Request request,
        ActionListener<Response> listener,
        BiConsumer<Request, ActionListener<Response>> consumer
    ) {
        final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(false);
        try (ThreadContext.StoredContext ignore = threadContext.stashWithOrigin(origin)) {
            consumer.accept(request, new ContextPreservingActionListener<>(supplier, listener));
        }
    }

    /**
     * Executes an asynchronous client action with a specified origin.
     * <p>
     * This is a convenience method that sets the action origin in the thread context
     * before executing the action, and ensures the original context is restored when
     * the action completes. This is the most common way to execute client actions
     * with system privileges.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ClientHelper.executeAsyncWithOrigin(
     *     client,
     *     ClientHelper.ML_ORIGIN,
     *     GetAction.INSTANCE,
     *     getRequest,
     *     ActionListener.wrap(
     *         response -> logger.info("Got document: {}", response),
     *         error -> logger.error("Failed to get document", error)
     *     )
     * );
     * }</pre>
     *
     * @param <Request> the request type (must extend ActionRequest)
     * @param <Response> the response type (must extend ActionResponse)
     * @param client the client to execute the action with
     * @param origin the origin to set for this action (e.g., "ml", "security", "watcher")
     * @param action the action type to execute
     * @param request the request to send
     * @param listener the listener to notify when the action completes
     */
    public static <Request extends ActionRequest, Response extends ActionResponse> void executeAsyncWithOrigin(
        Client client,
        String origin,
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        executeAsyncWithOrigin(client.threadPool().getThreadContext(), origin, request, listener, (r, l) -> client.execute(action, r, l));
    }

    /**
     * Executes a synchronous client operation with least privilege, using headers when available.
     * <p>
     * This method attempts to execute the operation with the security context from the
     * provided headers. If security headers are present, the operation runs with those
     * credentials (least privilege). If no security headers are present, the operation
     * falls back to using the specified origin (system privileges).
     * <p>
     * <b>Important:</b> This is a blocking/synchronous operation. For asynchronous
     * operations, use {@link #executeWithHeadersAsync} instead.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, String> headers = request.headers();
     * GetResponse response = ClientHelper.executeWithHeaders(
     *     headers,
     *     ClientHelper.SECURITY_ORIGIN,
     *     client,
     *     () -> client.get(getRequest).actionGet()
     * );
     * }</pre>
     *
     * @param <T> the response type (must extend ActionResponse)
     * @param headers request headers, ideally including security headers
     * @param origin the origin to use if there are no security headers
     * @param client the client used to execute the operation
     * @param supplier the operation to execute
     * @return the response from the operation
     */
    public static <T extends ActionResponse> T executeWithHeaders(
        Map<String, String> headers,
        String origin,
        Client client,
        Supplier<T> supplier
    ) {
        // No need to rewrite authentication header because it will be handled by Security Interceptor
        Map<String, String> filteredHeaders = filterSecurityHeaders(headers);

        // no security headers, we will have to use the xpack internal user for
        // our execution by specifying the origin
        if (filteredHeaders.isEmpty()) {
            try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(origin)) {
                return supplier.get();
            }
        } else {
            try (var ignore = client.threadPool().getThreadContext().stashContext()) {
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
    public static <Request extends ActionRequest, Response extends ActionResponse> void executeWithHeadersAsync(
        Map<String, String> headers,
        String origin,
        Client client,
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        executeWithHeadersAsync(
            client.threadPool().getThreadContext(),
            headers,
            origin,
            request,
            listener,
            (r, l) -> client.execute(action, r, l)
        );
    }

    public static <Request, Response> void executeWithHeadersAsync(
        ThreadContext threadContext,
        Map<String, String> headers,
        String origin,
        Request request,
        ActionListener<Response> listener,
        BiConsumer<Request, ActionListener<Response>> consumer
    ) {
        // No need to rewrite authentication header because it will be handled by Security Interceptor
        final Map<String, String> filteredHeaders = filterSecurityHeaders(headers);
        // No headers (e.g. security not installed/in use) so execute as origin
        if (filteredHeaders.isEmpty()) {
            executeAsyncWithOrigin(threadContext, origin, request, listener, consumer);
        } else {
            // Otherwise stash the context and copy in the saved headers before executing
            final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(false);
            try (ThreadContext.StoredContext ignore = stashWithHeaders(threadContext, filteredHeaders)) {
                consumer.accept(request, new ContextPreservingActionListener<>(supplier, listener));
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
