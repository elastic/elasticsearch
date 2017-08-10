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
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.plugin.SqlLicenseChecker;
import org.elasticsearch.xpack.sql.plugin.cli.CliServer;
import org.elasticsearch.xpack.sql.protocol.shared.Request;

import java.io.IOException;

import static org.elasticsearch.xpack.sql.util.ActionUtils.chain;

public class TransportCliAction extends HandledTransportAction<CliRequest, CliResponse> {
    private final CliServer cliServer;
    private final SqlLicenseChecker sqlLicenseChecker;

    @Inject
    public TransportCliAction(Settings settings, ThreadPool threadPool,
                              TransportService transportService, ActionFilters actionFilters,
                              IndexNameExpressionResolver indexNameExpressionResolver,
                              ClusterService clusterService,
                              PlanExecutor planExecutor,
                              SqlLicenseChecker sqlLicenseChecker) {
        super(settings, CliAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, CliRequest::new);
        this.sqlLicenseChecker = sqlLicenseChecker;
        this.cliServer = new CliServer(planExecutor, clusterService.getClusterName().value(), () -> clusterService.localNode().getName(), Version.CURRENT, Build.CURRENT);
    }

    @Override
    protected void doExecute(CliRequest cliRequest, ActionListener<CliResponse> listener) {
        sqlLicenseChecker.checkIfSqlAllowed();
        final Request request;
        try {
            request = cliRequest.request();
        } catch (IOException ex) {
            listener.onFailure(ex);
            return;
        }
        // NOCOMMIT we need to pass the protocol version of the client to the response here
        cliServer.handle(request, chain(listener, CliResponse::new));
    }
}