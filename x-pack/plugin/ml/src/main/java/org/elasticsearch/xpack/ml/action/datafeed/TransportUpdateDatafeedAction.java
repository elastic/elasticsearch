/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action.datafeed;

import org.elasticsearch.ElasticsearchStatusException;
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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;

import java.util.Optional;

public class TransportUpdateDatafeedAction extends TransportMasterNodeAction<UpdateDatafeedAction.Request, PutDatafeedAction.Response> {

    private final DatafeedManager datafeedManager;
    private final SecurityContext securityContext;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportUpdateDatafeedAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        DatafeedManager datafeedManager,
        ProjectResolver projectResolver
    ) {
        super(
            UpdateDatafeedAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            UpdateDatafeedAction.Request::new,
            PutDatafeedAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        this.datafeedManager = datafeedManager;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? new SecurityContext(settings, threadPool.getThreadContext())
            : null;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void masterOperation(
        Task task,
        UpdateDatafeedAction.Request request,
        ClusterState state,
        ActionListener<PutDatafeedAction.Response> listener
    ) {
        Optional<String> unsupportedReason = checkClusterSupportsDatafeedUpdate(request.getUpdate(), state);
        if (unsupportedReason.isPresent()) {
            listener.onFailure(unsupportedDatafeedUpdateException(request.getUpdate(), unsupportedReason.get()));
            return;
        }
        datafeedManager.updateDatafeed(request, state, securityContext, threadPool, listener);
    }

    static Optional<String> checkClusterSupportsDatafeedUpdate(DatafeedUpdate update, ClusterState state) {
        var minReq = update.minRequiredTransportVersion();
        if (minReq.isPresent() && state.getMinTransportVersion().supports(minReq.get().v1()) == false) {
            return Optional.of(minReq.get().v2());
        }
        return Optional.empty();
    }

    private static ElasticsearchStatusException unsupportedDatafeedUpdateException(DatafeedUpdate update, String unsupportedReason) {
        return ExceptionsHelper.badRequestException(
            "Cannot update datafeed [{}] while a cluster upgrade is in progress ({}); "
                + "wait for the cluster to finish upgrading and try again.",
            update.getId(),
            unsupportedReason
        );
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateDatafeedAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void doExecute(Task task, UpdateDatafeedAction.Request request, ActionListener<PutDatafeedAction.Response> listener) {
        final ActionListener<PutDatafeedAction.Response> releasingListener = ActionListener.releaseAfter(listener, request);
        Optional<String> unsupportedReason = checkClusterSupportsDatafeedUpdate(request.getUpdate(), clusterService.state());
        if (unsupportedReason.isPresent()) {
            releasingListener.onFailure(unsupportedDatafeedUpdateException(request.getUpdate(), unsupportedReason.get()));
            return;
        }
        CloudCredential callerCredential = datafeedManager.currentCallerCredential(threadPool, securityContext);
        if (callerCredential != null) {
            request.setCloudCredential(callerCredential);
        }
        super.doExecute(task, request, releasingListener);
    }
}
