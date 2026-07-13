/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action.datafeed;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;

import java.util.Optional;

public class TransportPutDatafeedAction extends TransportMasterNodeAction<PutDatafeedAction.Request, PutDatafeedAction.Response> {

    private final XPackLicenseState licenseState;
    private final SecurityContext securityContext;
    private final DatafeedManager datafeedManager;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportPutDatafeedAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        XPackLicenseState licenseState,
        ActionFilters actionFilters,
        DatafeedManager datafeedManager,
        ProjectResolver projectResolver
    ) {
        super(
            PutDatafeedAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutDatafeedAction.Request::new,
            PutDatafeedAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.licenseState = licenseState;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? new SecurityContext(settings, threadPool.getThreadContext())
            : null;
        this.datafeedManager = datafeedManager;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void masterOperation(
        Task task,
        PutDatafeedAction.Request request,
        ClusterState state,
        ActionListener<PutDatafeedAction.Response> listener
    ) {
        Optional<String> unsupportedReason = checkClusterSupportsDatafeedConfig(request.getDatafeed(), state);
        if (unsupportedReason.isPresent()) {
            listener.onFailure(
                ExceptionsHelper.badRequestException(
                    "Cannot create datafeed [{}] while a cluster upgrade is in progress ({}); "
                        + "wait for the cluster to finish upgrading and try again.",
                    request.getDatafeed().getId(),
                    unsupportedReason.get()
                )
            );
            return;
        }
        datafeedManager.putDatafeed(request, state, securityContext, threadPool, listener);
    }

    /**
     * Rejects datafeed creation when the datafeed requires a minimum transport version that the
     * cluster has not yet reached. This guards against a datafeed being created while a rolling
     * upgrade is still in progress — a datafeed with such a requirement must never be routed to a
     * node that predates the feature it depends on.
     *
     * @return the reason the datafeed requires a newer transport version, or empty if the cluster
     * already supports it
     */
    static Optional<String> checkClusterSupportsDatafeedConfig(DatafeedConfig datafeed, ClusterState state) {
        var minReq = datafeed.minRequiredTransportVersion();
        if (minReq.isPresent() && state.getMinTransportVersion().supports(minReq.get().v1()) == false) {
            return Optional.of(minReq.get().v2());
        }
        return Optional.empty();
    }

    @Override
    protected ClusterBlockException checkBlock(PutDatafeedAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void doExecute(Task task, PutDatafeedAction.Request request, ActionListener<PutDatafeedAction.Response> listener) {
        final ActionListener<PutDatafeedAction.Response> releasingListener = ActionListener.releaseAfter(listener, request);
        if (MachineLearningField.ML_API_FEATURE.check(licenseState)) {
            CloudCredential callerCredential = datafeedManager.currentCallerCredential(threadPool, securityContext);
            if (callerCredential != null) {
                request.setCloudCredential(callerCredential);
            }
            super.doExecute(task, request, releasingListener);
        } else {
            releasingListener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
        }
    }
}
