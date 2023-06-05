/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;

import java.util.Objects;

public class TransportCcrStatsAction extends TransportMasterNodeAction<CcrStatsAction.Request, CcrStatsAction.Response> {

    private final Client client;
    private final CcrLicenseChecker ccrLicenseChecker;
    private final AutoFollowCoordinator autoFollowCoordinator;

    @Inject
    public TransportCcrStatsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        AutoFollowCoordinator autoFollowCoordinator,
        CcrLicenseChecker ccrLicenseChecker,
        Client client
    ) {
        super(
            CcrStatsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            CcrStatsAction.Request::new,
            indexNameExpressionResolver,
            CcrStatsAction.Response::new,
            Ccr.CCR_THREAD_POOL_NAME
        );
        this.client = client;
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker);
        this.autoFollowCoordinator = Objects.requireNonNull(autoFollowCoordinator);
    }

    @Override
    protected void doExecute(Task task, CcrStatsAction.Request request, ActionListener<CcrStatsAction.Response> listener) {
        if (ccrLicenseChecker.isCcrAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException("ccr"));
            return;
        }
        super.doExecute(task, request, listener);
    }

    @Override
    protected void masterOperation(
        Task task,
        CcrStatsAction.Request request,
        ClusterState state,
        ActionListener<CcrStatsAction.Response> listener
    ) throws Exception {
        FollowStatsAction.StatsRequest statsRequest = new FollowStatsAction.StatsRequest();
        statsRequest.setParentTask(clusterService.localNode().getId(), task.getId());
        client.execute(FollowStatsAction.INSTANCE, statsRequest, listener.wrapFailure((l, statsResponse) -> {
            AutoFollowStats stats = autoFollowCoordinator.getStats();
            l.onResponse(new CcrStatsAction.Response(stats, statsResponse));
        }));
    }

    @Override
    protected ClusterBlockException checkBlock(CcrStatsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
