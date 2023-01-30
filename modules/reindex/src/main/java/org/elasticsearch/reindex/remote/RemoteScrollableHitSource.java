/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex.remote;

import org.apache.http.ContentTooLongException;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.RejectAwareActionListener;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueNanos;
import static org.elasticsearch.reindex.remote.RemoteResponseParsers.MAIN_ACTION_PARSER;
import static org.elasticsearch.reindex.remote.RemoteResponseParsers.RESPONSE_PARSER;

public class RemoteScrollableHitSource extends ScrollableHitSource {
    private final RestClient client;
    private final RemoteInfo remote;
    private final SearchRequest searchRequest;
    Version remoteVersion;

    public RemoteScrollableHitSource(
        Logger logger,
        BackoffPolicy backoffPolicy,
        ThreadPool threadPool,
        Runnable countSearchRetry,
        Consumer<AsyncResponse> onResponse,
        Consumer<Exception> fail,
        RestClient client,
        RemoteInfo remoteInfo,
        SearchRequest searchRequest
    ) {
        super(logger, backoffPolicy, threadPool, countSearchRetry, onResponse, fail);
        this.remote = remoteInfo;
        this.searchRequest = searchRequest;
        this.client = client;
    }

    @Override
    protected void doStart(RejectAwareActionListener<Response> searchListener) {
        lookupRemoteVersion(RejectAwareActionListener.withResponseHandler(searchListener, version -> {
            remoteVersion = version;
            execute(
                RemoteRequestBuilders.initialSearch(searchRequest, remote.getQuery(), remoteVersion),
                RESPONSE_PARSER,
                RejectAwareActionListener.withResponseHandler(searchListener, r -> onStartResponse(searchListener, r))
            );
        }));
    }

    void lookupRemoteVersion(RejectAwareActionListener<Version> listener) {
        execute(new Request("GET", ""), MAIN_ACTION_PARSER, listener);
    }

    private void onStartResponse(RejectAwareActionListener<Response> searchListener, Response response) {
        if (Strings.hasLength(response.getScrollId()) && response.getHits().isEmpty()) {
            logger.debug("First response looks like a scan response. Jumping right to the second. scroll=[{}]", response.getScrollId());
            doStartNextScroll(response.getScrollId(), timeValueMillis(0), searchListener);
        } else {
            searchListener.onResponse(response);
        }
    }

    @Override
    protected void doStartNextScroll(String scrollId, TimeValue extraKeepAlive, RejectAwareActionListener<Response> searchListener) {
        TimeValue keepAlive = timeValueNanos(searchRequest.scroll().keepAlive().nanos() + extraKeepAlive.nanos());
        execute(RemoteRequestBuilders.scroll(scrollId, keepAlive, remoteVersion), RESPONSE_PARSER, searchListener);
    }

    @Override
    protected void clearScroll(String scrollId, Runnable onCompletion) {
        client.performRequestAsync(RemoteRequestBuilders.clearScroll(scrollId, remoteVersion), new ResponseListener() {
            @Override
            public void onSuccess(org.elasticsearch.client.Response response) {
                logger.debug("Successfully cleared [{}]", scrollId);
                onCompletion.run();
            }

            @Override
            public void onFailure(Exception e) {
                logFailure(e);
                onCompletion.run();
            }

            private void logFailure(Exception e) {
                if (e instanceof ResponseException re) {
                    if (remoteVersion.before(Version.fromId(2000099)) && re.getResponse().getStatusLine().getStatusCode() == 404) {
                        logger.debug(
                            () -> format(
                                "Failed to clear scroll [%s] from pre-2.0 Elasticsearch. This is normal if the request terminated "
                                    + "normally as the scroll has already been cleared automatically.",
                                scrollId
                            ),
                            e
                        );
                        return;
                    }
                }
                logger.warn((Supplier<?>) () -> "Failed to clear scroll [" + scrollId + "]", e);
            }
        });
    }

    @Override
    protected void cleanup(Runnable onCompletion) {
        /* This is called on the RestClient's thread pool and attempting to close the client on its
         * own threadpool causes it to fail to close. So we always shutdown the RestClient
         * asynchronously on a thread in Elasticsearch's generic thread pool. */
        threadPool.generic().submit(() -> {
            try {
                client.close();
                logger.debug("Shut down remote connection");
                remote.close();
            } catch (IOException e) {
                logger.error("Failed to shutdown the remote connection", e);
            } finally {
                onCompletion.run();
            }
        });
    }

    private <T> void execute(
        Request request,
        BiFunction<XContentParser, XContentType, T> parser,
        RejectAwareActionListener<? super T> listener
    ) {
        // Preserve the thread context so headers survive after the call
        java.util.function.Supplier<ThreadContext.StoredContext> contextSupplier = threadPool.getThreadContext().newRestorableContext(true);
        try {
            client.performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(org.elasticsearch.client.Response response) {
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
     * Wrap the ResponseException in an exception that'll preserve its status code if possible so we can send it back to the user. We might
     * not have a constant for the status code so in that case we just use 500 instead. We also extract make sure to include the response
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

    private static String bodyMessage(@Nullable HttpEntity entity) throws IOException {
        if (entity == null) {
            return "No error body.";
        } else {
            return "body=" + EntityUtils.toString(entity);
        }
    }
}
