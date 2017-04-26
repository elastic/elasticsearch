/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollResponse;
import org.elasticsearch.action.bulk.byscroll.DeleteByQueryRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.xpack.common.action.XPackDeleteByQueryAction;
import org.elasticsearch.xpack.security.InternalClient;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.action.support.TransportActions.isShardNotAvailableException;

/**
 * Responsible for cleaning the invalidated tokens from the invalidated tokens index.
 */
final class ExpiredTokenRemover extends AbstractRunnable {

    private final InternalClient client;
    private final AtomicBoolean inProgress = new AtomicBoolean(false);
    private final Logger logger;

    ExpiredTokenRemover(Settings settings, InternalClient internalClient) {
        this.client = internalClient;
        this.logger = Loggers.getLogger(getClass(), settings);
    }

    @Override
    public void doRun() {
        SearchRequest searchRequest = new SearchRequest(TokenService.INDEX_NAME);
        DeleteByQueryRequest dbq = new DeleteByQueryRequest(searchRequest);
        searchRequest.source()
                .query(QueryBuilders.boolQuery()
                        .filter(QueryBuilders.termQuery("doc_type", TokenService.DOC_TYPE))
                        .filter(QueryBuilders.rangeQuery("expiration_time").lte(DateTime.now(DateTimeZone.UTC))));
        client.execute(XPackDeleteByQueryAction.INSTANCE, dbq, ActionListener.wrap(r -> markComplete(),
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
