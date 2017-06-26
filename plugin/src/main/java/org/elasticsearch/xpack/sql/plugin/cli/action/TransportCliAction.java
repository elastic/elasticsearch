/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.cli.action;

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
import org.elasticsearch.xpack.sql.plugin.cli.server.CliServer;

import static org.elasticsearch.xpack.sql.util.ActionUtils.chain;

public class TransportCliAction extends HandledTransportAction<CliRequest, CliResponse> {
    private final CliServer cliServer;

    @Inject
    public TransportCliAction(Settings settings, ThreadPool threadPool,
            TransportService transportService, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            ClusterService clusterService,
            PlanExecutor planExecutor) {
        super(settings, CliAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, CliRequest::new);

        // lazy init of the resolver
        // NOCOMMIT indexNameExpressionResolver should be available some other way
        ((EsCatalog) planExecutor.catalog()).setIndexNameExpressionResolver(indexNameExpressionResolver);
        this.cliServer = new CliServer(planExecutor, clusterService.getClusterName().value(), () -> clusterService.localNode().getName(),
                Version.CURRENT, Build.CURRENT);
    }

    @Override
    protected void doExecute(CliRequest request, ActionListener<CliResponse> listener) {
        cliServer.handle(request.request(), chain(listener, CliResponse::new));
    }
}