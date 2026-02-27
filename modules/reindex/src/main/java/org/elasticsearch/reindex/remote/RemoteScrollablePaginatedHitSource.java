/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.remote;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.PaginatedHitSource;
import org.elasticsearch.index.reindex.RejectAwareActionListener;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.index.reindex.ResumeInfo.ScrollWorkerResumeInfo;
import org.elasticsearch.index.reindex.ResumeInfo.WorkerResumeInfo;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Consumer;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueNanos;
import static org.elasticsearch.reindex.remote.RemoteReindexingUtils.execute;
import static org.elasticsearch.reindex.remote.RemoteReindexingUtils.lookupRemoteVersion;
import static org.elasticsearch.reindex.remote.RemoteResponseParsers.RESPONSE_PARSER;

/**
 * Scrollable search lets you retrieve large result sets by opening a search context and repeatedly requesting
 * the next batch using a {@code _scroll_id}, effectively acting like a cursor over a snapshot of the index at the time of the
 * initial search. It is no longer recommended for deep pagination due to resource costs and limits on open scrolls.
 * <p>
 * This implementation is a scrollable source of hits from a <i>remote</i> {@linkplain Client} instance. For local
 * clients, please use {@link org.elasticsearch.index.reindex.ClientScrollablePaginatedHitSource}
 */
public class RemoteScrollablePaginatedHitSource extends PaginatedHitSource {
    private final RestClient client;
    private final RemoteInfo remote;
    private final SearchRequest searchRequest;
    Version remoteVersion;

    public RemoteScrollablePaginatedHitSource(
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
        this(logger, backoffPolicy, threadPool, countSearchRetry, onResponse, fail, client, remoteInfo, searchRequest, null);
    }

    public RemoteScrollablePaginatedHitSource(
        Logger logger,
        BackoffPolicy backoffPolicy,
        ThreadPool threadPool,
        Runnable countSearchRetry,
        Consumer<AsyncResponse> onResponse,
        Consumer<Exception> fail,
        RestClient client,
        RemoteInfo remoteInfo,
        SearchRequest searchRequest,
        @Nullable Version initialRemoteVersion
    ) {
        super(logger, backoffPolicy, threadPool, countSearchRetry, onResponse, fail);
        this.remote = remoteInfo;
        this.searchRequest = searchRequest;
        this.client = client;
        this.remoteVersion = initialRemoteVersion;
    }

    @Override
    protected void doStart(RejectAwareActionListener<Response> searchListener) {
        if (remoteVersion != null) {
            execute(
                RemoteRequestBuilders.initialSearch(searchRequest, remote.getQuery(), remoteVersion),
                RESPONSE_PARSER,
                RejectAwareActionListener.withResponseHandler(searchListener, r -> onStartResponse(searchListener, r)),
                threadPool,
                client
            );
        } else {
            lookupRemoteVersion(RejectAwareActionListener.withResponseHandler(searchListener, version -> {
                remoteVersion = version;
                execute(
                    RemoteRequestBuilders.initialSearch(searchRequest, remote.getQuery(), remoteVersion),
                    RESPONSE_PARSER,
                    RejectAwareActionListener.withResponseHandler(searchListener, r -> onStartResponse(searchListener, r)),
                    threadPool,
                    client
                );
            }), threadPool, client);
        }
    }

    @Override
    public void restoreState(WorkerResumeInfo resumeInfo) {
        assert resumeInfo instanceof ScrollWorkerResumeInfo;
        var scrollResumeInfo = (ScrollWorkerResumeInfo) resumeInfo;
        remoteVersion = scrollResumeInfo.remoteVersion();
        assert remoteVersion != null : "remote cluster version must be set to resume remote reindex";
        setScroll(scrollResumeInfo.scrollId());
    }

    public Optional<Version> remoteVersion() {
        return Optional.ofNullable(remoteVersion);
    }

    // Exposed for testing
    void onStartResponse(RejectAwareActionListener<Response> searchListener, Response response) {
        if (Strings.hasLength(response.getScrollId()) && response.getHits().isEmpty()) {
            logger.debug("First response looks like a scan response. Jumping right to the second. scroll=[{}]", response.getScrollId());
            doStartNextScroll(response.getScrollId(), timeValueMillis(0), searchListener);
        } else {
            searchListener.onResponse(response);
        }
    }

    @Override
    protected void doStartNextScroll(String scrollId, TimeValue extraKeepAlive, RejectAwareActionListener<Response> searchListener) {
        TimeValue keepAlive = timeValueNanos(searchRequest.scroll().nanos() + extraKeepAlive.nanos());
        execute(RemoteRequestBuilders.scroll(scrollId, keepAlive, remoteVersion), RESPONSE_PARSER, searchListener, threadPool, client);
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
}
