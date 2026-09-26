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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.RejectAwareActionListener;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.index.reindex.ResumeInfo.PitWorkerResumeInfo;
import org.elasticsearch.reindex.PitPaginatedHitSource;
import org.elasticsearch.reindex.SearchContextKeepaliveDeadline;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;
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
public class RemotePitPaginatedHitSource extends PitPaginatedHitSource {
    private final RestClient client;
    private final RemoteInfo remote;
    private final SearchRequest searchRequest;
    private final AtomicReference<BytesReference> pitId;
    private final TimeValue baseKeepAlive;
    private final Version remoteVersion;
    private final SearchContextKeepaliveDeadline keepaliveDeadline;
    private final CircuitBreaker circuitBreaker;
    private final long memoryAccountingThresholdBytes;
    /**
     * Keep-alive sent with the PIT search HTTP request currently in flight.
     * Cleared after each successful response.
     */
    private final AtomicReference<TimeValue> currentKeepAlive = new AtomicReference<>();

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
        Version remoteVersion,
        SearchContextKeepaliveDeadline keepaliveDeadline,
        CircuitBreaker circuitBreaker,
        long memoryAccountingThresholdBytes
    ) {
        super(logger, backoffPolicy, threadPool, countSearchRetry, onResponse, fail);
        this.remote = remoteInfo;
        this.searchRequest = searchRequest;
        this.client = client;
        this.remoteVersion = remoteVersion;
        this.keepaliveDeadline = keepaliveDeadline;
        this.circuitBreaker = circuitBreaker;
        this.memoryAccountingThresholdBytes = memoryAccountingThresholdBytes;
        SearchSourceBuilder source = searchRequest.source();
        if (source == null || source.pointInTimeBuilder() == null) {
            throw new IllegalArgumentException("SearchRequest must have pointInTimeBuilder set for PIT-based remote pagination");
        }
        PointInTimeBuilder pitBuilder = source.pointInTimeBuilder();
        this.pitId = new AtomicReference<>(pitBuilder.getEncodedId());
        TimeValue keepAlive = pitBuilder.getKeepAlive();
        this.baseKeepAlive = keepAlive != null ? keepAlive : TimeValue.timeValueMinutes(5);
    }

    @Override
    protected void doFirstSearch(RejectAwareActionListener<Response> searchListener) {
        logger.debug("executing initial remote pit search");
        currentKeepAlive.set(baseKeepAlive);
        execute(
            RemoteRequestBuilders.pitSearch(searchRequest, remote.getQuery(), pitId.get(), baseKeepAlive, null, remoteVersion),
            RESPONSE_PARSER,
            wrapPitSearchListener(searchListener),
            threadPool,
            client,
            circuitBreaker,
            memoryAccountingThresholdBytes
        );
    }

    @Override
    public BytesReference getPitId() {
        return pitId.get();
    }

    @Override
    protected void restorePitState(PitWorkerResumeInfo resumeInfo) {
        pitId.set(resumeInfo.pitId());
        setSearchAfterValues(resumeInfo.searchAfterValues());
    }

    void onPitResponse(RejectAwareActionListener<Response> searchListener, Response response) {
        if (response.getPitId() != null) {
            pitId.set(response.getPitId());
        }
        // Substitute the cached total on follow-up batches whose response total is a placeholder.
        OptionalLong cachedTotal = getCachedTotalHits();
        Response delivered = response;
        if (cachedTotal.isPresent()) {
            delivered = new Response(
                response.isTimedOut(),
                response.getFailures(),
                cachedTotal.getAsLong(),
                response.getHits(),
                response.getScrollId(),
                response.getSearchAfterValues(),
                response.getPitId()
            );
            response.moveBodyReleasableTo(delivered);
        }
        searchListener.onResponse(delivered);
    }

    @Override
    protected void doNextPitSearch(Object[] searchAfter, TimeValue extraKeepAlive, RejectAwareActionListener<Response> searchListener) {
        TimeValue keepAlive = timeValueNanos(baseKeepAlive.nanos() + extraKeepAlive.nanos());
        currentKeepAlive.set(keepAlive);
        // Cache is seeded after the first batch, so drop track_total_hits on follow-ups to keep Max WAND active.
        SearchRequest nextRequest = searchRequest;
        if (getCachedTotalHits().isPresent()) {
            SearchSourceBuilder source = searchRequest.source().shallowCopy().trackTotalHits(false);
            nextRequest = new SearchRequest(searchRequest).source(source);
        }
        execute(
            RemoteRequestBuilders.pitSearch(nextRequest, remote.getQuery(), pitId.get(), keepAlive, searchAfter, remoteVersion),
            RESPONSE_PARSER,
            wrapPitSearchListener(searchListener),
            threadPool,
            client,
            circuitBreaker,
            memoryAccountingThresholdBytes
        );
    }

    private RejectAwareActionListener<Response> wrapPitSearchListener(RejectAwareActionListener<Response> searchListener) {
        return RejectAwareActionListener.withResponseHandler(searchListener, r -> {
            TimeValue keepAlive = currentKeepAlive.getAndSet(null);
            if (keepAlive != null) {
                keepaliveDeadline.recordSuccessfulExtension(keepAlive);
            }
            onPitResponse(searchListener, r);
        });
    }

    @Override
    protected void cleanup(Runnable onCompletion) {
        threadPool.generic().submit(() -> {
            try {
                client.close();
                logger.debug("Shut down remote connection");
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
