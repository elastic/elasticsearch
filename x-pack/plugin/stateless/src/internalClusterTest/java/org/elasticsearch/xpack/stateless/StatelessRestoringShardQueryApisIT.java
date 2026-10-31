/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.encryption.EncryptionPlugin;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * The APIs that run a query over an index, on a stateless cluster, while the index is being restored from a snapshot: reindex, update and
 * delete by query, and ES|QL. A restore that asks for its shards to be reported makes them fail with a 403; one that does not leaves them
 * failing with a 503.
 */
public class StatelessRestoringShardQueryApisIT extends AbstractStatelessRestoringShardIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ReindexPlugin.class);
        plugins.add(EncryptionPlugin.class);
        plugins.add(EsqlPlugin.class);
        return plugins;
    }

    // _reindex

    /**
     * {@code POST /_reindex} from a restoring index, with and without a query. A query is what makes the request run the can-match phase
     * first, which is another place that finds the missing shard.
     */
    private static List<ReindexRequest> reindexRequests() {
        return List.of(
            new ReindexRequest().setSourceIndices(RESTORED_INDEX).setDestIndex("dest"),
            new ReindexRequest().setSourceIndices(RESTORED_INDEX).setDestIndex("dest").setSourceQuery(QueryBuilders.matchAllQuery())
        );
    }

    public void testReindexFromRestoringIndexFailsWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            for (ReindexRequest request : reindexRequests()) {
                assertBulkByQueryFailsWithShardRestoring(client().execute(ReindexAction.INSTANCE, request), restoreUuid);
            }
        });
    }

    public void testReindexFromRestoringIndexFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            for (ReindexRequest request : reindexRequests()) {
                assertBulkByQueryFailsWithServiceUnavailable(client().execute(ReindexAction.INSTANCE, request));
            }
        });
    }

    // _update_by_query

    public void testUpdateByQueryOverRestoringIndexFailsWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            assertBulkByQueryFailsWithShardRestoring(
                client().execute(UpdateByQueryAction.INSTANCE, new UpdateByQueryRequest(RESTORED_INDEX)),
                restoreUuid
            );
        });
    }

    public void testUpdateByQueryOverRestoringIndexFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            assertBulkByQueryFailsWithServiceUnavailable(
                client().execute(UpdateByQueryAction.INSTANCE, new UpdateByQueryRequest(RESTORED_INDEX))
            );
        });
    }

    // _delete_by_query

    public void testDeleteByQueryOverRestoringIndexFailsWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            assertBulkByQueryFailsWithShardRestoring(
                client().execute(
                    DeleteByQueryAction.INSTANCE,
                    new DeleteByQueryRequest(RESTORED_INDEX).setQuery(QueryBuilders.matchAllQuery())
                ),
                restoreUuid
            );
        });
    }

    public void testDeleteByQueryOverRestoringIndexFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            assertBulkByQueryFailsWithServiceUnavailable(
                client().execute(
                    DeleteByQueryAction.INSTANCE,
                    new DeleteByQueryRequest(RESTORED_INDEX).setQuery(QueryBuilders.matchAllQuery())
                )
            );
        });
    }

    /**
     * A reindex or by-query request whose search fails either throws the failure, or reports it in the response's search failures, each
     * with its own cause. Either way the failure is the restoring shard.
     */
    private static void assertBulkByQueryFailsWithShardRestoring(ActionFuture<BulkByPaginatedSearchResponse> future, String restoreUuid)
        throws Exception {
        final BulkByPaginatedSearchResponse response;
        try {
            response = future.get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            assertShardRestoringException(e.getCause(), restoreUuid);
            return;
        }
        assertThat(response.getSearchFailures().size(), greaterThan(0));
        for (var failure : response.getSearchFailures()) {
            assertThat(failure.getStatus(), equalTo(RestStatus.FORBIDDEN));
            assertShardRestoringException(failure.getReason(), restoreUuid);
        }
    }

    private static void assertBulkByQueryFailsWithServiceUnavailable(ActionFuture<BulkByPaginatedSearchResponse> future) throws Exception {
        final BulkByPaginatedSearchResponse response;
        try {
            response = future.get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            assertServiceUnavailableException(e.getCause());
            return;
        }
        assertThat(response.getSearchFailures().size(), greaterThan(0));
        for (var failure : response.getSearchFailures()) {
            assertThat(failure.getStatus(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
            assertServiceUnavailableException(failure.getReason());
        }
    }

    // ES|QL

    /**
     * An ES|QL query with partial results disallowed, sync and async.
     */
    private static List<EsqlQueryRequest> esqlQueries(String from) {
        final String query = "FROM " + from + " | LIMIT 1";
        final var async = EsqlQueryRequest.asyncEsqlQueryRequest(query).allowPartialResults(false);
        async.waitForCompletionTimeout(TimeValue.timeValueSeconds(30));
        return List.of(EsqlQueryRequest.syncEsqlQueryRequest(query).allowPartialResults(false), async);
    }

    private static Exception esqlFailure(EsqlQueryRequest request) {
        return expectThrows(Exception.class, () -> client().execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS));
    }

    /**
     * ES|QL over only the restoring index fails with a 403. Resolving the index runs {@code _field_caps}, which no index answers, and
     * which then fails with the restore because it is all that stops the request.
     */
    public void testEsqlQueryOfOnlyTheRestoringIndexFailsWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> {
            for (EsqlQueryRequest request : esqlQueries(RESTORED_INDEX)) {
                assertShardRestoringException(esqlFailure(request), restoreUuid);
            }
        });
    }

    public void testEsqlQueryOfOnlyTheRestoringIndexFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> {
            for (EsqlQueryRequest request : esqlQueries(RESTORED_INDEX)) {
                assertServiceUnavailableException(esqlFailure(request));
            }
        });
    }

    /**
     * ES|QL over the restoring index and a healthy one fails with a 403. Resolving the indices runs {@code _field_caps}, which reports
     * the restoring index as a 403 failure because the healthy one answered.
     */
    public void testEsqlQueryOverSeveralIndicesFailsWithShardRestoring() throws Exception {
        whileRestoring(true, true, restoreUuid -> {
            for (EsqlQueryRequest request : esqlQueries(RESTORED_INDEX + "," + HEALTHY_INDEX)) {
                assertShardRestoringException(esqlFailure(request), restoreUuid);
            }
        });
    }

    public void testEsqlQueryOverSeveralIndicesFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(true, false, restoreUuid -> {
            for (EsqlQueryRequest request : esqlQueries(RESTORED_INDEX + "," + HEALTHY_INDEX)) {
                assertServiceUnavailableException(esqlFailure(request));
            }
        });
    }
}
