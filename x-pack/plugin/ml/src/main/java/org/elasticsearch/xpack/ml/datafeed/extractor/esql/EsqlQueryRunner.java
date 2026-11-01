/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

/** Runs ES|QL queries for datafeed validation probes and synchronous extraction with retry-once policy. */
interface EsqlQueryRunner {

    Logger LOGGER = LogManager.getLogger(EsqlQueryRunner.class);

    EsqlQueryRunner DEFAULT = EsqlQueryRunner::executeAsyncDefault;

    void executeAsync(
        Client client,
        String query,
        @Nullable QueryBuilder filter,
        Map<String, String> headers,
        @Nullable String projectRouting,
        List<EsqlQueryParam> params,
        ActionListener<EsqlQueryResponse> listener
    );

    static EsqlQueryResponse runWithSingleRetry(
        EsqlSyncQueryRunner syncQueryRunner,
        String query,
        QueryBuilder timeFilter,
        List<EsqlQueryParam> params,
        BooleanSupplier cancelled,
        String jobId
    ) {
        throwIfCancelled(cancelled);
        try {
            return syncQueryRunner.run(query, timeFilter, params);
        } catch (NodeClosedException | NodeDisconnectedException | NodeNotConnectedException | UnavailableShardsException e) {
            return retryAfterNodeFailure(syncQueryRunner, query, timeFilter, params, cancelled, jobId, e);
        } catch (ElasticsearchException e) {
            if (isRetryableNodeFailure(e) == false) {
                throw e;
            }
            return retryAfterNodeFailure(syncQueryRunner, query, timeFilter, params, cancelled, jobId, e);
        }
    }

    static boolean isRetryableNodeFailure(Exception e) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        return cause instanceof NodeClosedException
            || cause instanceof NodeDisconnectedException
            || cause instanceof NodeNotConnectedException
            || cause instanceof UnavailableShardsException;
    }

    private static EsqlQueryResponse retryAfterNodeFailure(
        EsqlSyncQueryRunner syncQueryRunner,
        String query,
        QueryBuilder timeFilter,
        List<EsqlQueryParam> params,
        BooleanSupplier cancelled,
        String jobId,
        Exception nodeFailure
    ) {
        if (cancelled.getAsBoolean()) {
            if (nodeFailure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new ElasticsearchException(nodeFailure);
        }
        LOGGER.debug(
            () -> "[" + jobId + "] ES|QL query failed due to a node or shard availability failure; retrying the same range",
            nodeFailure
        );
        throwIfCancelled(cancelled);
        return syncQueryRunner.run(query, timeFilter, params);
    }

    private static void throwIfCancelled(BooleanSupplier cancelled) {
        if (cancelled.getAsBoolean()) {
            throw new IllegalStateException(Messages.getMessage(Messages.DATAFEED_ESQL_QUERY_CANCELLED));
        }
    }

    private static void executeAsyncDefault(
        Client client,
        String query,
        @Nullable QueryBuilder filter,
        Map<String, String> headers,
        @Nullable String projectRouting,
        List<EsqlQueryParam> params,
        ActionListener<EsqlQueryResponse> listener
    ) {
        EsqlQueryExecutor.executeAsync(
            client,
            EsqlQueryExecutor.newRequest(client, query, filter, params, projectRouting),
            headers,
            listener
        );
    }
}
