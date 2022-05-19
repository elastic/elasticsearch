/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.xpack.security.support.SecuritySystemIndices;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.action.support.TransportActions.isShardNotAvailableException;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Responsible for cleaning the invalidated and expired API keys from the security index.
 */
public final class ExpiredApiKeysRemover extends AbstractRunnable {
    public static final Duration EXPIRED_API_KEYS_RETENTION_PERIOD = Duration.ofDays(7L);

    private static final Logger logger = LogManager.getLogger(ExpiredApiKeysRemover.class);

    private final Client client;
    private final AtomicBoolean inProgress = new AtomicBoolean(false);
    private final TimeValue timeout;

    ExpiredApiKeysRemover(Settings settings, Client client) {
        this.client = client;
        this.timeout = ApiKeyService.DELETE_TIMEOUT.get(settings);
    }

    @Override
    public void doRun() {
        DeleteByQueryRequest expiredDbq = new DeleteByQueryRequest(SecuritySystemIndices.SECURITY_MAIN_ALIAS);
        if (timeout != TimeValue.MINUS_ONE) {
            expiredDbq.setTimeout(timeout);
            expiredDbq.getSearchRequest().source().timeout(timeout);
        }
        final Instant now = Instant.now();
        expiredDbq.setQuery(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery("doc_type", "api_key"))
                .should(QueryBuilders.termsQuery("api_key_invalidated", true))
                .should(QueryBuilders.rangeQuery("expiration_time").lte(now.minus(EXPIRED_API_KEYS_RETENTION_PERIOD).toEpochMilli()))
                .minimumShouldMatch(1)
        );

        executeAsyncWithOrigin(client, SECURITY_ORIGIN, DeleteByQueryAction.INSTANCE, expiredDbq, ActionListener.wrap(r -> {
            debugDbqResponse(r);
            markComplete();
        }, this::onFailure));
    }

    void submit(ThreadPool threadPool) {
        if (inProgress.compareAndSet(false, true)) {
            threadPool.executor(Names.GENERIC).submit(this);
        }
    }

    private static void debugDbqResponse(BulkByScrollResponse response) {
        if (logger.isDebugEnabled()) {
            logger.debug(
                "delete by query of api keys finished with [{}] deletions, [{}] bulk failures, [{}] search failures",
                response.getDeleted(),
                response.getBulkFailures().size(),
                response.getSearchFailures().size()
            );
            for (BulkItemResponse.Failure failure : response.getBulkFailures()) {
                logger.debug(
                    new ParameterizedMessage("deletion failed for index [{}], id [{}]", failure.getIndex(), failure.getId()),
                    failure.getCause()
                );
            }
            for (ScrollableHitSource.SearchFailure failure : response.getSearchFailures()) {
                logger.debug(
                    new ParameterizedMessage(
                        "search failed for index [{}], shard [{}] on node [{}]",
                        failure.getIndex(),
                        failure.getShardId(),
                        failure.getNodeId()
                    ),
                    failure.getReason()
                );
            }
        }
    }

    boolean isExpirationInProgress() {
        return inProgress.get();
    }

    @Override
    public void onFailure(Exception e) {
        if (isShardNotAvailableException(e)) {
            logger.debug("failed to delete expired or invalidated api keys", e);
        } else {
            logger.error("failed to delete expired or invalidated api keys", e);
        }
        markComplete();
    }

    private void markComplete() {
        if (inProgress.compareAndSet(true, false) == false) {
            throw new IllegalStateException("in progress was set to false but should have been true!");
        }
    }

}
