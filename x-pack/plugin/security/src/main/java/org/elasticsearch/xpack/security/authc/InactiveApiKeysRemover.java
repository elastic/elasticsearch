/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
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

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.action.support.TransportActions.isShardNotAvailableException;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Responsible for cleaning the invalidated and expired API keys from the security index.
 */
public final class InactiveApiKeysRemover extends AbstractRunnable {
    private static final Logger logger = LogManager.getLogger(InactiveApiKeysRemover.class);

    private final Client client;
    private final AtomicBoolean inProgress = new AtomicBoolean(false);
    private final TimeValue timeout;
    private final AtomicLong retentionPeriodInMs;

    InactiveApiKeysRemover(Settings settings, Client client, ClusterService clusterService) {
        this.client = client;
        this.timeout = ApiKeyService.DELETE_TIMEOUT.get(settings);
        this.retentionPeriodInMs = new AtomicLong(ApiKeyService.DELETE_RETENTION_PERIOD.get(settings).getMillis());
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                ApiKeyService.DELETE_RETENTION_PERIOD,
                newRetentionPeriod -> this.retentionPeriodInMs.set(newRetentionPeriod.getMillis())
            );
    }

    @Override
    public void doRun() {
        DeleteByQueryRequest expiredDbq = new DeleteByQueryRequest(SecuritySystemIndices.SECURITY_MAIN_ALIAS);
        if (timeout != TimeValue.MINUS_ONE) {
            expiredDbq.setTimeout(timeout);
            expiredDbq.getSearchRequest().source().timeout(timeout);
        }
        final Instant now = Instant.now();
        final long cutoffTimestamp = now.minusMillis(retentionPeriodInMs.get()).toEpochMilli();
        expiredDbq.setQuery(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery("doc_type", "api_key"))
                .should(QueryBuilders.rangeQuery("expiration_time").lte(cutoffTimestamp))
                .should(
                    QueryBuilders.boolQuery()
                        .filter(QueryBuilders.termsQuery("api_key_invalidated", true))
                        .should(QueryBuilders.rangeQuery("invalidation_time").lte(cutoffTimestamp))
                        .should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("invalidation_time")))
                        .minimumShouldMatch(1)
                )
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
                    () -> format("deletion failed for index [%s], id [%s]", failure.getIndex(), failure.getId()),
                    failure.getCause()
                );
            }
            for (ScrollableHitSource.SearchFailure failure : response.getSearchFailures()) {
                logger.debug(
                    () -> format(
                        "search failed for index [%s], shard [%s] on node [%s]",
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
