/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.sql.analysis.catalog.EsCatalog;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.plugin.SqlGetIndicesAction;

import java.util.function.BiConsumer;

public class TestUtils {
    public static PlanExecutor planExecutor(Client client) {
        BiConsumer<SqlGetIndicesAction.Request, ActionListener<SqlGetIndicesAction.Response>> getIndices = (request, listener) -> {
            ClusterState state = client.admin().cluster().prepareState().get(TimeValue.timeValueMinutes(1)).getState();
            SqlGetIndicesAction.operation(new IndexNameExpressionResolver(Settings.EMPTY), EsCatalog::new, request, state, listener);
        };
        PlanExecutor executor = new PlanExecutor(
                client,
                () -> client.admin().cluster().prepareState().get(TimeValue.timeValueMinutes(1)).getState(),
                getIndices,
                EsCatalog::new);
        return executor;
    }
}