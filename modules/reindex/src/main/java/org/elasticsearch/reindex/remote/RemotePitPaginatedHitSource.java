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
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.PaginatedHitSource;
import org.elasticsearch.index.reindex.PaginationCursor;
import org.elasticsearch.index.reindex.RejectAwareActionListener;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.index.reindex.ResumeInfo.PitWorkerResumeInfo;
import org.elasticsearch.index.reindex.ResumeInfo.WorkerResumeInfo;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.core.TimeValue.timeValueNanos;
import static org.elasticsearch.reindex.remote.RemoteReindexingUtils.execute;
import static org.elasticsearch.reindex.remote.RemoteResponseParsers.RESPONSE_PARSER;

/**
 * PIT-based paginated search for remote Elasticsearch clusters. Uses point-in-time and search_after
 * to retrieve large result sets. Requires remote version 7.10.0 or later.
 * <p>
 * The PIT must already be opened and injected into the search request before this hit source is used.
 */
public class RemotePitPaginatedHitSource extends PaginatedHitSource {
    private final RestClient client;
    private final RemoteInfo remote;
    private final SearchRequest searchRequest;
    private final AtomicReference<BytesReference> pitId;
    private final TimeValue baseKeepAlive;
    final Version remoteVersion;

    public RemotePitPaginatedHitSource(
        Logger logger,
        BackoffPolicy backoffPolicy,
        ThreadPool threadPool,
        Runnable countSearchRetry,
        Consumer<AsyncResponse> onResponse,
        Consumer<Exception> fail,
        RestClient client,
        RemoteInfo remoteInfo,
        SearchRequest searchRequest,
        Version remoteVersion
    ) {
        super(logger, backoffPolicy, threadPool, countSearchRetry, onResponse, fail);
        this.remote = remoteInfo;
        this.searchRequest = searchRequest;
        this.client = client;
        this.remoteVersion = remoteVersion;
        SearchSourceBuilder source = searchRequest.source();
        if (source == null || source.pointInTimeBuilder() == null) {
            throw new IllegalArgumentException("SearchRequest must have pointInTimeBuilder set for PIT-based remote pagination");
        }
        PointInTimeBuilder pitBuilder = source.pointInTimeBuilder();
        this.pitId = new AtomicReference<>(pitBuilder.getEncodedId());
        TimeValue keepAlive = pitBuilder.getKeepAlive();
        // TODO - https://github.com/elastic/elasticsearch-team/issues/2334
        this.baseKeepAlive = keepAlive != null ? keepAlive : TimeValue.timeValueMinutes(5);
    }

    @Override
    protected void doFirstSearch(RejectAwareActionListener<Response> searchListener) {
        execute(
            RemoteRequestBuilders.pitSearch(searchRequest, remote.getQuery(), pitId.get(), baseKeepAlive, null, remoteVersion),
            RESPONSE_PARSER,
            RejectAwareActionListener.withResponseHandler(searchListener, r -> onPitResponse(searchListener, r)),
            threadPool,
            client
        );
    }

    @Override
    protected void restoreState(WorkerResumeInfo resumeInfo) {
        assert resumeInfo instanceof PitWorkerResumeInfo;
        var pitResumeInfo = (PitWorkerResumeInfo) resumeInfo;
        pitId.set(pitResumeInfo.pitId());
        setSearchAfterValues(pitResumeInfo.searchAfterValues());
    }

    void onPitResponse(RejectAwareActionListener<Response> searchListener, Response response) {
        if (response.getPitId() != null) {
            pitId.set(response.getPitId());
        }
        searchListener.onResponse(response);
    }

    @Override
    protected void doNextSearch(PaginationCursor cursor, TimeValue extraKeepAlive, RejectAwareActionListener<Response> searchListener) {
        assert cursor.isSearchAfter() : "RemotePitPaginatedHitSource expects search_after cursor";
        // TODO - https://github.com/elastic/elasticsearch-team/issues/2334
        TimeValue keepAlive = timeValueNanos(baseKeepAlive.nanos() + extraKeepAlive.nanos());
        execute(
            RemoteRequestBuilders.pitSearch(searchRequest, remote.getQuery(), pitId.get(), keepAlive, cursor.searchAfter(), remoteVersion),
            RESPONSE_PARSER,
            RejectAwareActionListener.withResponseHandler(searchListener, r -> onPitResponse(searchListener, r)),
            threadPool,
            client
        );
    }

    @Override
    protected void releaseSearchContext(Runnable onCompletion) {
        // PIT is closed by Reindexer when the reindex completes
        onCompletion.run();
    }

    @Override
    protected void cleanup(Runnable onCompletion) {
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

    public Optional<Version> remoteVersion() {
        return Optional.of(remoteVersion);
    }
}
