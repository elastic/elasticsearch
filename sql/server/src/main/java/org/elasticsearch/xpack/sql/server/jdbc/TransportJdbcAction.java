/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.server.jdbc;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.sql.analysis.catalog.EsCatalog;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;

import static org.elasticsearch.xpack.sql.util.ActionUtils.chain;

public class TransportJdbcAction extends HandledTransportAction<JdbcRequest, JdbcResponse> {
    private final JdbcServer jdbcServer;

    @Inject
    public TransportJdbcAction(Settings settings, ThreadPool threadPool,
            TransportService transportService, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            ClusterService clusterService,
            PlanExecutor planExecutor) {
        super(settings, JdbcAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, JdbcRequest::new);

        // lazy init of the resolver
        ((EsCatalog) planExecutor.catalog()).setIndexNameExpressionResolver(indexNameExpressionResolver);
        // NOCOMMIT indexNameExpressionResolver should be available some other way
        this.jdbcServer = new JdbcServer(planExecutor, clusterService.getClusterName().value(), () -> clusterService.localNode().getName(),
                Version.CURRENT, Build.CURRENT);
    }

    @Override
    protected void doExecute(JdbcRequest request, ActionListener<JdbcResponse> listener) {
        // NOCOMMIT looks like this runs on the netty threadpool which might be bad. If we go async immediately it is ok, but we don't.
        jdbcServer.handle(request.request(), chain(listener, JdbcResponse::new));
    }
}