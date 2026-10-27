/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
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
import org.elasticsearch.snapshots.ShardRestoringException;
import org.elasticsearch.xpack.encryption.EncryptionPlugin;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * The APIs that run a query over an index, on a stateless cluster, while the index is being restored by a restore that asked for its
 * shards to be reported: reindex, update and delete by query, and ES|QL.
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

    /**
     * {@code POST /_reindex} from a restoring index fails with a 403, with and without a query. A query is what makes the request run the
     * can-match phase first, which is another place that finds the missing shard.
     */
    public void testReindexFromRestoringIndexFailsWithShardRestoring() throws Exception {
        setUpSnapshotAndHealthyIndex();
        final String restoreUuid = blockAndStartRestore(true);
        try {
            for (ReindexRequest request : new ReindexRequest[] {
                new ReindexRequest().setSourceIndices(RESTORED_INDEX).setDestIndex("dest"),
                new ReindexRequest().setSourceIndices(RESTORED_INDEX)
                    .setDestIndex("dest")
                    .setSourceQuery(QueryBuilders.matchAllQuery()) }) {
                final var e = expectThrows(
                    ExecutionException.class,
                    () -> client().execute(ReindexAction.INSTANCE, request).get(30, TimeUnit.SECONDS)
                );
                assertForbiddenShardRestoring(e.getCause(), restoreUuid);
            }
        } finally {
            finishRestore();
        }
    }

    public void testUpdateByQueryOverRestoringIndexFailsWithShardRestoring() throws Exception {
        setUpSnapshotAndHealthyIndex();
        final String restoreUuid = blockAndStartRestore(true);
        try {
            assertBulkByScrollForbidden(
                client().execute(UpdateByQueryAction.INSTANCE, new UpdateByQueryRequest(RESTORED_INDEX)),
                restoreUuid
            );
        } finally {
            finishRestore();
        }
    }

    public void testDeleteByQueryOverRestoringIndexFailsWithShardRestoring() throws Exception {
        setUpSnapshotAndHealthyIndex();
        final String restoreUuid = blockAndStartRestore(true);
        try {
            assertBulkByScrollForbidden(
                client().execute(
                    DeleteByQueryAction.INSTANCE,
                    new DeleteByQueryRequest(RESTORED_INDEX).setQuery(QueryBuilders.matchAllQuery())
                ),
                restoreUuid
            );
        } finally {
            finishRestore();
        }
    }

    /**
     * The failure of a bulk-by-scroll request is either thrown, or reported in the response's failures, where each carries its status.
     */
    private static void assertBulkByScrollForbidden(ActionFuture<BulkByPaginatedSearchResponse> future, String restoreUuid)
        throws Exception {
        final BulkByPaginatedSearchResponse response;
        try {
            response = future.get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            assertForbiddenShardRestoring(e.getCause(), restoreUuid);
            return;
        }
        assertThat(response.getSearchFailures().size(), greaterThan(0));
        for (var failure : response.getSearchFailures()) {
            assertThat(failure.getStatus(), equalTo(RestStatus.FORBIDDEN));
        }
    }

    /**
     * ES|QL is not changed by the flag: a query over a restoring index fails with a 503, as it does without it. ES|QL has its own way of
     * looking for the shards and of reporting those it cannot find, and this change leaves it alone. This pins that down, so that a
     * change to it is a decision and not an accident.
     */
    public void testEsqlQueryOverRestoringIndexIsNotChangedByTheFlag() throws Exception {
        setUpSnapshotAndHealthyIndex();
        blockAndStartRestore(true);
        try {
            final var request = new EsqlQueryRequest().allowPartialResults(false).query("FROM " + RESTORED_INDEX + " | LIMIT 1");
            final var e = expectThrows(
                Exception.class,
                () -> client().execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS)
            );
            assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.SERVICE_UNAVAILABLE));
            assertThat(
                ExceptionsHelper.unwrapCausesAndSuppressed(e, t -> t instanceof ShardRestoringException).isPresent(),
                equalTo(false)
            );
        } finally {
            finishRestore();
        }
    }
}
