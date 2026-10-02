/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.remote;

import org.apache.http.ContentTooLongException;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.RejectAwareActionListener;
import org.elasticsearch.index.reindex.RetryListener;
import org.elasticsearch.reindex.PaginatedHitSource;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.elasticsearch.reindex.remote.RemoteResponseParsers.MAIN_ACTION_PARSER;
import static org.elasticsearch.reindex.remote.RemoteResponseParsers.OPEN_PIT_PARSER;

/**
 * Utility methods for reindexing from remote Elasticsearch clusters.
 * Handles version lookup, HTTP execution with response parsing, and error handling.
 */
public class RemoteReindexingUtils {

    /**
     * Looks up the version of the remote Elasticsearch cluster by performing a GET request to the root path.
     *
     * @param listener  receives the parsed version on success, or failure/rejection on error
     * @param threadPool thread pool for preserving thread context across async callbacks
     * @param client    REST client for the remote cluster
     * @param breaker   REQUEST circuit breaker to consult while parsing the response
     * @param memoryAccountingThresholdBytes minimum local accumulation before flushing bytes to the breaker
     */
    public static void lookupRemoteVersion(
        RejectAwareActionListener<Version> listener,
        ThreadPool threadPool,
        RestClient client,
        CircuitBreaker breaker,
        long memoryAccountingThresholdBytes
    ) {
        execute(new Request("GET", "/"), MAIN_ACTION_PARSER, listener, threadPool, client, breaker, memoryAccountingThresholdBytes);
    }

    /**
     * Opens a point-in-time on the remote cluster. Requires remote version 7.10.0 or later.
     *
     * @param indices   indices to open PIT on
     * @param keepAlive PIT keep alive duration
     * @param remoteVersion remote cluster version (used to omit REST parameters the remote does not support)
     * @param listener  receives the PIT id on success, or failure/rejection on error
     * @param threadPool thread pool for preserving thread context
     * @param client   REST client for the remote cluster
     * @param breaker  REQUEST circuit breaker to consult while parsing the response
     * @param memoryAccountingThresholdBytes minimum local accumulation before flushing bytes to the breaker
     */
    public static void openPit(
        SearchRequest request,
        String[] indices,
        TimeValue keepAlive,
        Version remoteVersion,
        RejectAwareActionListener<BytesReference> listener,
        ThreadPool threadPool,
        RestClient client,
        CircuitBreaker breaker,
        long memoryAccountingThresholdBytes
    ) {
        // The routing and preference parameters can be set for a PIT request. However, scroll currently does not use these,
        // so for parity we assert here in case that changes
        assert request.routing() == null : "Routing is set in the search request, but is not being used when opening the PIT.";
        assert request.preference() == null : "Preference is set in the search request, but is not being used when opening the PIT.";
        assert request.allowPartialSearchResults() == null || request.allowPartialSearchResults() == false
            : "allow_partial_search_results must be false when opening a PIT to match scroll search behavior";
        execute(
            RemoteRequestBuilders.openPit(indices, keepAlive, request, remoteVersion),
            OPEN_PIT_PARSER,
            listener,
            threadPool,
            client,
            breaker,
            memoryAccountingThresholdBytes
        );
    }

    /**
     * Closes a point-in-time on the remote cluster.
     *
     * @param pitId    the PIT id to close
     * @param listener receives on success, or failure on error
     * @param threadPool thread pool for preserving thread context
     * @param client   REST client for the remote cluster
     * @param breaker  REQUEST circuit breaker to consult while parsing the response
     * @param memoryAccountingThresholdBytes minimum local accumulation before flushing bytes to the breaker
     */
    public static void closePit(
        BytesReference pitId,
        RejectAwareActionListener<Void> listener,
        ThreadPool threadPool,
        RestClient client,
        CircuitBreaker breaker,
        long memoryAccountingThresholdBytes
    ) {
        execute(RemoteRequestBuilders.closePit(pitId), (p, ctx) -> {
            try {
                if (p.nextToken() != null) {
                    p.skipChildren();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        },
            RejectAwareActionListener.withResponseHandler(listener, v -> listener.onResponse(null)),
            threadPool,
            client,
            breaker,
            memoryAccountingThresholdBytes
        );
    }

    /**
     * Looks up the remote cluster version with retries on rejection (e.g. 429 Too Many Requests).
     * Matches the retry behavior used by {@link RemoteScrollablePaginatedHitSource} when it looks up the version.
     *
     * @param logger       logger for retry messages
     * @param backoffPolicy policy for delay between retries
     * @param threadPool   thread pool for scheduling retries
     * @param client      REST client for the remote cluster
     * @param delegate    receives the version on success or failure after all retries exhausted
     * @param breaker     REQUEST circuit breaker to consult while parsing the response
     * @param memoryAccountingThresholdBytes minimum local accumulation before flushing bytes to the breaker
     */
    public static void lookupRemoteVersionWithRetries(
        Logger logger,
        BackoffPolicy backoffPolicy,
        ThreadPool threadPool,
        RestClient client,
        RejectAwareActionListener<Version> delegate,
        CircuitBreaker breaker,
        long memoryAccountingThresholdBytes
    ) {
        RetryListener<Version> retryListener = new RetryListener<>(
            logger,
            threadPool,
            backoffPolicy,
            listener -> lookupRemoteVersion(listener, threadPool, client, breaker, memoryAccountingThresholdBytes),
            delegate
        );
        lookupRemoteVersion(retryListener, threadPool, client, breaker, memoryAccountingThresholdBytes);
    }

    /**
     * Performs an async HTTP request to the remote cluster, parses the response, and notifies the listener.
     * Preserves thread context across the async callback. On 429 (Too Many Requests), invokes
     * {@link RejectAwareActionListener#onRejection} so callers can retry; other failures invoke
     * {@link RejectAwareActionListener#onFailure}.
     *
     * <p>The response is parsed through a {@link RemoteParseContext} that accumulates per-hit bytes
     * and incrementally charges the REQUEST circuit breaker. If the breaker trips during parsing, a
     * {@link CircuitBreakingException} is surfaced to the listener (HTTP 429) and any bytes already
     * registered are released. For hit-bearing responses ({@link PaginatedHitSource.Response}) the
     * context is handed off to the response so the reservation is held until the batch is cleaned
     * up; for small responses (version lookup, PIT open/close) the context is closed immediately.
     *
     * @param <T>      type of the parsed response
     * @param request  HTTP request to perform
     * @param parser   function to parse the response body into type T, receiving the parse context
     * @param listener receives the parsed result, or failure/rejection
     * @param threadPool thread pool for preserving thread context
     * @param client   REST client for the remote cluster
     * @param breaker  REQUEST circuit breaker to consult while parsing the response
     * @param memoryAccountingThresholdBytes minimum local accumulation before flushing bytes to the breaker
     */
    static <T> void execute(
        Request request,
        BiFunction<XContentParser, RemoteParseContext, T> parser,
        RejectAwareActionListener<? super T> listener,
        ThreadPool threadPool,
        RestClient client,
        CircuitBreaker breaker,
        long memoryAccountingThresholdBytes
    ) {
        // Account the raw Apache HTTP response buffer against the REQUEST breaker before the
        // response reaches this callback. RemoteParseContext accounts parsed hits later.
        request.setOptions(
            request.getOptions().toBuilder().setHttpAsyncResponseConsumerFactory(new BreakerAwareConsumerFactory(breaker)).build()
        );
        // Preserve the thread context so headers survive after the call
        Supplier<ThreadContext.StoredContext> contextSupplier = threadPool.getThreadContext().newRestorableContext(true);
        try {
            client.performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    // Restore the thread context to get the precious headers
                    try (ThreadContext.StoredContext ctx = contextSupplier.get()) {
                        assert ctx != null; // eliminates compiler warning
                        T parsedResponse;
                        RemoteParseContext parseContext = null;
                        // Armed with the parseContext while we own it; nulled out once handed off to the Response.
                        // The finally always closes whatever this points at (null is safe).
                        Releasable toCloseOnFailure = null;
                        Releasable responseEntityReleasable = null;
                        try {
                            HttpEntity responseEntity = response.getEntity();
                            if (responseEntity instanceof Releasable releasable) {
                                responseEntityReleasable = releasable;
                            }
                            InputStream content = responseEntity.getContent();
                            XContentType xContentType = null;
                            if (responseEntity.getContentType() != null) {
                                final String mimeType = ContentType.parse(responseEntity.getContentType().getValue()).getMimeType();
                                xContentType = XContentType.fromMediaType(mimeType);
                            }
                            if (xContentType == null) {
                                try {
                                    throw new ElasticsearchException(
                                        "Response didn't include Content-Type: " + bodyMessage(response.getEntity())
                                    );
                                } catch (IOException e) {
                                    ElasticsearchException ee = new ElasticsearchException("Error extracting body from response");
                                    ee.addSuppressed(e);
                                    throw ee;
                                }
                            }
                            parseContext = new RemoteParseContext(xContentType, breaker, memoryAccountingThresholdBytes);
                            toCloseOnFailure = parseContext;
                            // EMPTY is safe here because we don't call namedObject
                            try (
                                XContentParser xContentParser = xContentType.xContent()
                                    .createParser(
                                        XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
                                        content
                                    )
                            ) {
                                parsedResponse = parser.apply(xContentParser, parseContext);
                            } catch (XContentParseException e) {
                                // Surface the exception if during parsing circuit breaker is tripped from RemoteParseContext
                                CircuitBreakingException cbe = (CircuitBreakingException) ExceptionsHelper.unwrap(
                                    e,
                                    CircuitBreakingException.class
                                );
                                if (cbe != null) {
                                    throw cbe;
                                }
                                /* Because we're streaming the response we can't get a copy of it here. The best we can do is hint that it
                                 * is totally wrong and we're probably not talking to Elasticsearch. */
                                throw new ElasticsearchException(
                                    "Error parsing the response, remote is likely not an Elasticsearch instance",
                                    e
                                );
                            }
                            parseContext.flushRemaining();
                            if (parsedResponse instanceof PaginatedHitSource.Response r) {
                                r.setBodyReleasable(parseContext);
                                toCloseOnFailure = null;
                            }
                        } catch (CircuitBreakingException cbe) {
                            // parseContext is released by the `finally` (toCloseOnFailure was not nulled on this path).
                            listener.onFailure(cbe);
                            return;
                        } catch (IOException e) {
                            throw new ElasticsearchException(
                                "Error deserializing response, remote is likely not an Elasticsearch instance",
                                e
                            );
                        } finally {
                            Releasables.closeWhileHandlingException(toCloseOnFailure, responseEntityReleasable);
                        }
                        listener.onResponse(parsedResponse);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try (ThreadContext.StoredContext ctx = contextSupplier.get()) {
                        assert ctx != null; // eliminates compiler warning
                        if (e instanceof ResponseException re) {
                            int statusCode = re.getResponse().getStatusLine().getStatusCode();
                            HttpEntity responseEntity = re.getResponse().getEntity();
                            try {
                                e = wrapExceptionToPreserveStatus(statusCode, responseEntity, re);
                            } finally {
                                closeIfReleasable(responseEntity);
                            }
                            if (RestStatus.TOO_MANY_REQUESTS.getStatus() == statusCode) {
                                listener.onRejection(e);
                                return;
                            }
                        } else if (ExceptionsHelper.unwrap(e, CircuitBreakingException.class) instanceof CircuitBreakingException cbe) {
                            listener.onFailure(cbe);
                            return;
                        } else if (e instanceof ContentTooLongException) {
                            e = new IllegalArgumentException(
                                "Remote responded with a chunk that was too large. Use a smaller batch size.",
                                e
                            );
                        }
                        listener.onFailure(e);
                    }
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private static void closeIfReleasable(@Nullable HttpEntity entity) {
        if (entity instanceof Releasable releasable) {
            releasable.close();
        }
    }

    /**
     * Wrap the ResponseException in an exception that'll preserve its status code if possible, so we can send it back to the user. We might
     * not have a constant for the status code, so in that case, we just use 500 instead. We also extract make sure to include the response
     * body in the message so the user can figure out *why* the remote Elasticsearch service threw the error back to us.
     */
    static ElasticsearchStatusException wrapExceptionToPreserveStatus(int statusCode, @Nullable HttpEntity entity, Exception cause) {
        RestStatus status = RestStatus.fromCode(statusCode);
        String messagePrefix = "";
        if (status == null) {
            messagePrefix = "Couldn't extract status [" + statusCode + "]. ";
            status = RestStatus.INTERNAL_SERVER_ERROR;
        }
        try {
            return new ElasticsearchStatusException(messagePrefix + bodyMessage(entity), status, cause);
        } catch (IOException ioe) {
            ElasticsearchStatusException e = new ElasticsearchStatusException(messagePrefix + "Failed to extract body.", status, cause);
            e.addSuppressed(ioe);
            return e;
        }
    }

    /**
     * Extracts a readable string from an HTTP entity for use in error messages.
     *
     * @param entity HTTP entity, or null
     * @return "No error body." if entity is null, otherwise "body=" + entity content
     * @throws IOException if reading the entity fails
     */
    static String bodyMessage(@Nullable HttpEntity entity) throws IOException {
        if (entity == null) {
            return "No error body.";
        } else {
            return "body=" + EntityUtils.toString(entity);
        }
    }
}
