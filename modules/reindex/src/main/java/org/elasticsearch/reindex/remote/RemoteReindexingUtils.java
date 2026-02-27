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
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.reindex.RejectAwareActionListener;
import org.elasticsearch.index.reindex.RetryListener;
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
     */
    public static void lookupRemoteVersion(RejectAwareActionListener<Version> listener, ThreadPool threadPool, RestClient client) {
        execute(new Request("GET", "/"), MAIN_ACTION_PARSER, listener, threadPool, client);
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
     */
    public static void lookupRemoteVersionWithRetries(
        Logger logger,
        BackoffPolicy backoffPolicy,
        ThreadPool threadPool,
        RestClient client,
        RejectAwareActionListener<Version> delegate
    ) {
        RetryListener<Version> retryListener = new RetryListener<>(logger, threadPool, backoffPolicy, listener -> {
            lookupRemoteVersion(listener, threadPool, client);
        }, delegate);
        lookupRemoteVersion(retryListener, threadPool, client);
    }

    /**
     * Performs an async HTTP request to the remote cluster, parses the response, and notifies the listener.
     * Preserves thread context across the async callback. On 429 (Too Many Requests), invokes
     * {@link RejectAwareActionListener#onRejection} so callers can retry; other failures invoke
     * {@link RejectAwareActionListener#onFailure}.
     *
     * @param <T>      type of the parsed response
     * @param request  HTTP request to perform
     * @param parser   function to parse the response body into type T
     * @param listener receives the parsed result, or failure/rejection
     * @param threadPool thread pool for preserving thread context
     * @param client   REST client for the remote cluster
     */
    static <T> void execute(
        Request request,
        BiFunction<XContentParser, XContentType, T> parser,
        RejectAwareActionListener<? super T> listener,
        ThreadPool threadPool,
        RestClient client
    ) {
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
                        try {
                            HttpEntity responseEntity = response.getEntity();
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
                            // EMPTY is safe here because we don't call namedObject
                            try (
                                XContentParser xContentParser = xContentType.xContent()
                                    .createParser(
                                        XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
                                        content
                                    )
                            ) {
                                parsedResponse = parser.apply(xContentParser, xContentType);
                            } catch (XContentParseException e) {
                                /* Because we're streaming the response we can't get a copy of it here. The best we can do is hint that it
                                 * is totally wrong and we're probably not talking to Elasticsearch. */
                                throw new ElasticsearchException(
                                    "Error parsing the response, remote is likely not an Elasticsearch instance",
                                    e
                                );
                            }
                        } catch (IOException e) {
                            throw new ElasticsearchException(
                                "Error deserializing response, remote is likely not an Elasticsearch instance",
                                e
                            );
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
                            e = wrapExceptionToPreserveStatus(statusCode, re.getResponse().getEntity(), re);
                            if (RestStatus.TOO_MANY_REQUESTS.getStatus() == statusCode) {
                                listener.onRejection(e);
                                return;
                            }
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
