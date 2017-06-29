/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.security.SecurityLifecycleService;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.action.support.TransportActions.isShardNotAvailableException;

/**
 * Responsible for cleaning the invalidated tokens from the invalidated tokens index.
 */
final class ExpiredTokenRemover extends AbstractRunnable {

    private final InternalClient client;
    private final AtomicBoolean inProgress = new AtomicBoolean(false);
    private final Logger logger;
    private final TimeValue timeout;

    ExpiredTokenRemover(Settings settings, InternalClient internalClient) {
        this.client = internalClient;
        this.logger = Loggers.getLogger(getClass(), settings);
        this.timeout = TokenService.DELETE_TIMEOUT.get(settings);
    }

    @Override
    public void doRun() {
        SearchRequest searchRequest = new SearchRequest(SecurityLifecycleService.SECURITY_INDEX_NAME);
        DeleteByQueryRequest dbq = new DeleteByQueryRequest(searchRequest);
        if (timeout != TimeValue.MINUS_ONE) {
            dbq.setTimeout(timeout);
            searchRequest.source().timeout(timeout);
        }
        searchRequest.source()
                .query(QueryBuilders.boolQuery()
                        .filter(QueryBuilders.termQuery("doc_type", TokenService.DOC_TYPE))
                        .filter(QueryBuilders.rangeQuery("expiration_time").lte(Instant.now().toEpochMilli())));
        client.execute(DeleteByQueryAction.INSTANCE, dbq, ActionListener.wrap(r -> markComplete(),
                e -> {
                    if (isShardNotAvailableException(e) == false) {
                        logger.error("failed to delete expired tokens", e);
                    }
                    markComplete();
                }));
    }

    void submit(ThreadPool threadPool) {
        if (inProgress.compareAndSet(false, true)) {
            threadPool.executor(Names.GENERIC).submit(this);
        }
    }

    boolean isExpirationInProgress() {
        return inProgress.get();
    }

    @Override
    public void onFailure(Exception e) {
        logger.error("failed to delete expired tokens", e);
        markComplete();
    }

    private void markComplete() {
        if (inProgress.compareAndSet(true, false) == false) {
            throw new IllegalStateException("in progress was set to false but should have been true!");
        }
    }
}
