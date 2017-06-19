/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.sql.analysis.catalog.EsCatalog;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.plugin.cli.http.CliUtils;
import org.elasticsearch.xpack.sql.session.RowSetCursor;

public class TestUtils {
    public static PlanExecutor planExecutor(Client client) {
        PlanExecutor executor = new PlanExecutor(client,
                () -> client.admin().cluster().prepareState().get(TimeValue.timeValueMinutes(1)).getState());
        ((EsCatalog) executor.catalog()).setIndexNameExpressionResolver(new IndexNameExpressionResolver(client.settings()));
        return executor;
    }

    public static void sqlOut(PlanExecutor executor, String sql) {
        executor.sql(sql, new ActionListener<RowSetCursor>() {

            @Override
            public void onResponse(RowSetCursor cursor) {
                System.out.println(CliUtils.toString(cursor));
            }

            @Override
            public void onFailure(Exception ex) {
                throw ex instanceof RuntimeException ? (RuntimeException) ex : new SqlException(ex);
            }
        });
    }
}