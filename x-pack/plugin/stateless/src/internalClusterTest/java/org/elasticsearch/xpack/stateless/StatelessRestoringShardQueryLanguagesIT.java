/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;
import org.elasticsearch.xpack.ql.plugin.QlPlugin;
import org.elasticsearch.xpack.sql.action.SqlQueryAction;
import org.elasticsearch.xpack.sql.action.SqlQueryRequestBuilder;
import org.elasticsearch.xpack.sql.action.SqlTranslateAction;
import org.elasticsearch.xpack.sql.action.SqlTranslateRequestBuilder;
import org.elasticsearch.xpack.sql.plugin.SqlPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * EQL and SQL, on a stateless cluster, while the index they query is being restored from a snapshot. Both resolve the index through
 * {@code _field_caps} before they run anything, and that is where a restoring index is found. A restore that asks for its shards to be
 * reported makes them fail with a 403; one that does not leaves them failing with a 503.
 */
public class StatelessRestoringShardQueryLanguagesIT extends AbstractStatelessRestoringShardIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(QlPlugin.class);
        plugins.add(EqlPlugin.class);
        plugins.add(SqlPlugin.class);
        return plugins;
    }

    private static Exception failure(ActionFuture<?> future) {
        return expectThrows(Exception.class, () -> future.actionGet(30, TimeUnit.SECONDS));
    }

    // EQL

    private ActionFuture<?> eqlSearch() {
        return client().execute(EqlSearchAction.INSTANCE, new EqlSearchRequest().indices(RESTORED_INDEX).query("any where true"));
    }

    public void testEqlSearchFailsWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> assertShardRestoringException(failure(eqlSearch()), restoreUuid));
    }

    public void testEqlSearchFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> assertServiceUnavailableException(failure(eqlSearch())));
    }

    // SQL

    private ActionFuture<?> sqlQuery() {
        return client().execute(
            SqlQueryAction.INSTANCE,
            new SqlQueryRequestBuilder(client()).query("SELECT * FROM \"" + RESTORED_INDEX + "\"").request()
        );
    }

    public void testSqlQueryFailsWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> assertShardRestoringException(failure(sqlQuery()), restoreUuid));
    }

    public void testSqlQueryFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> assertServiceUnavailableException(failure(sqlQuery())));
    }

    // SQL translate, which only resolves the index and does not search it

    private ActionFuture<?> sqlTranslate() {
        return client().execute(
            SqlTranslateAction.INSTANCE,
            new SqlTranslateRequestBuilder(client()).query("SELECT * FROM \"" + RESTORED_INDEX + "\"").request()
        );
    }

    public void testSqlTranslateFailsWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> assertShardRestoringException(failure(sqlTranslate()), restoreUuid));
    }

    public void testSqlTranslateFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> assertServiceUnavailableException(failure(sqlTranslate())));
    }
}
