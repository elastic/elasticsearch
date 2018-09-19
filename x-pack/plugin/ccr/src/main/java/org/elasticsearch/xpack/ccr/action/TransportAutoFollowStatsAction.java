/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats;
import org.elasticsearch.xpack.core.ccr.action.AutoFollowStatsAction;

import java.util.Objects;

public class TransportAutoFollowStatsAction
    extends TransportMasterNodeAction<AutoFollowStatsAction.Request, AutoFollowStatsAction.Response> {

    private final CcrLicenseChecker ccrLicenseChecker;
    private final AutoFollowCoordinator autoFollowCoordinator;

    @Inject
    public TransportAutoFollowStatsAction(
            Settings settings,
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            AutoFollowCoordinator autoFollowCoordinator,
            CcrLicenseChecker ccrLicenseChecker
    ) {
        super(
            settings,
            AutoFollowStatsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            AutoFollowStatsAction.Request::new,
            indexNameExpressionResolver
        );
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker);
        this.autoFollowCoordinator = Objects.requireNonNull(autoFollowCoordinator);
    }

    @Override
    protected String executor() {
        return Ccr.CCR_THREAD_POOL_NAME;
    }

    @Override
    protected AutoFollowStatsAction.Response newResponse() {
        return new AutoFollowStatsAction.Response();
    }

    @Override
    protected void doExecute(Task task, AutoFollowStatsAction.Request request, ActionListener<AutoFollowStatsAction.Response> listener) {
        if (ccrLicenseChecker.isCcrAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException("ccr"));
            return;
        }
        super.doExecute(task, request, listener);
    }

    @Override
    protected void masterOperation(
            AutoFollowStatsAction.Request request,
            ClusterState state,
            ActionListener<AutoFollowStatsAction.Response> listener
    ) throws Exception {
        AutoFollowStats stats = autoFollowCoordinator.getStats();
        listener.onResponse(new AutoFollowStatsAction.Response(stats));
    }

    @Override
    protected ClusterBlockException checkBlock(AutoFollowStatsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
