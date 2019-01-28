/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.action.support.TransportActions.isShardNotAvailableException;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Responsible for cleaning the invalidated and expired tokens from the security index.
 * The document gets deleted if it was created more than 24 hours which is the maximum
 * lifetime of a refresh token
 */
final class ExpiredTokenRemover extends AbstractRunnable {
    private static final Logger logger = LogManager.getLogger(ExpiredTokenRemover.class);

    private final Client client;
    private final AtomicBoolean inProgress = new AtomicBoolean(false);
    private final TimeValue timeout;

    ExpiredTokenRemover(Settings settings, Client client) {
        this.client = client;
        this.timeout = TokenService.DELETE_TIMEOUT.get(settings);
    }

    @Override
    public void doRun() {
        DeleteByQueryRequest expiredDbq = new DeleteByQueryRequest(SecurityIndexManager.SECURITY_INDEX_NAME);
        if (timeout != TimeValue.MINUS_ONE) {
            expiredDbq.setTimeout(timeout);
            expiredDbq.getSearchRequest().source().timeout(timeout);
        }
        final Instant now = Instant.now();
        expiredDbq
            .setQuery(QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery("doc_type", "token"))
                .filter(QueryBuilders.rangeQuery("creation_time").lte(now.minus(24L, ChronoUnit.HOURS).toEpochMilli())));
        logger.trace(() -> new ParameterizedMessage("Removing old tokens: [{}]", Strings.toString(expiredDbq)));
        executeAsyncWithOrigin(client, SECURITY_ORIGIN, DeleteByQueryAction.INSTANCE, expiredDbq,
                ActionListener.wrap(r -> {
                    debugDbqResponse(r);
                    markComplete();
                }, this::onFailure));
    }

    void submit(ThreadPool threadPool) {
        if (inProgress.compareAndSet(false, true)) {
            threadPool.executor(Names.GENERIC).submit(this);
        }
    }

    private void debugDbqResponse(BulkByScrollResponse response) {
        if (logger.isDebugEnabled()) {
            logger.debug("delete by query of tokens finished with [{}] deletions, [{}] bulk failures, [{}] search failures",
                    response.getDeleted(), response.getBulkFailures().size(), response.getSearchFailures().size());
            for (BulkItemResponse.Failure failure : response.getBulkFailures()) {
                logger.debug(new ParameterizedMessage("deletion failed for index [{}], type [{}], id [{}]",
                        failure.getIndex(), failure.getType(), failure.getId()), failure.getCause());
            }
            for (ScrollableHitSource.SearchFailure failure : response.getSearchFailures()) {
                logger.debug(new ParameterizedMessage("search failed for index [{}], shard [{}] on node [{}]",
                        failure.getIndex(), failure.getShardId(), failure.getNodeId()), failure.getReason());
            }
        }
    }

    boolean isExpirationInProgress() {
        return inProgress.get();
    }

    @Override
    public void onFailure(Exception e) {
        if (isShardNotAvailableException(e)) {
            logger.debug("failed to delete expired tokens", e);
        } else {
            logger.error("failed to delete expired tokens", e);
        }
        markComplete();
    }

    private void markComplete() {
        if (inProgress.compareAndSet(true, false) == false) {
            throw new IllegalStateException("in progress was set to false but should have been true!");
        }
    }
}
