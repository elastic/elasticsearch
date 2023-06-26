/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.snapshots.SnapshotsInfoService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.autoscaling.AutoscalingLicenseChecker;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCalculateCapacityService;
import org.elasticsearch.xpack.autoscaling.capacity.nodeinfo.AutoscalingNodeInfoService;

import java.util.Objects;

public class TransportGetAutoscalingCapacityAction extends TransportMasterNodeAction<
    GetAutoscalingCapacityAction.Request,
    GetAutoscalingCapacityAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetAutoscalingCapacityAction.class);

    private final AutoscalingCalculateCapacityService capacityService;
    private final ClusterInfoService clusterInfoService;
    private final SnapshotsInfoService snapshotsInfoService;
    private final AutoscalingNodeInfoService nodeInfoService;
    private final AutoscalingLicenseChecker autoscalingLicenseChecker;
    private final CapacityResponseCache<GetAutoscalingCapacityAction.Response> responseCache = new CapacityResponseCache<>(
        run -> threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(run),
        this::computeCapacity
    );

    @Inject
    public TransportGetAutoscalingCapacityAction(
        final TransportService transportService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final AutoscalingCalculateCapacityService.Holder capacityServiceHolder,
        final ClusterInfoService clusterInfoService,
        final SnapshotsInfoService snapshotsInfoService,
        final AutoscalingNodeInfoService nodeInfoService,
        final AutoscalingLicenseChecker autoscalingLicenseChecker
    ) {
        super(
            GetAutoscalingCapacityAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetAutoscalingCapacityAction.Request::new,
            indexNameExpressionResolver,
            GetAutoscalingCapacityAction.Response::new,
            ThreadPool.Names.SAME
        );
        this.snapshotsInfoService = snapshotsInfoService;
        this.nodeInfoService = nodeInfoService;
        this.capacityService = capacityServiceHolder.get();
        this.clusterInfoService = clusterInfoService;
        this.autoscalingLicenseChecker = Objects.requireNonNull(autoscalingLicenseChecker);
        assert this.capacityService != null;
    }

    @Override
    protected void masterOperation(
        final Task task,
        final GetAutoscalingCapacityAction.Request request,
        final ClusterState state,
        final ActionListener<GetAutoscalingCapacityAction.Response> listener
    ) {
        if (autoscalingLicenseChecker.isAutoscalingAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException("autoscaling"));
            return;
        }

        assert task instanceof CancellableTask;
        final CancellableTask cancellableTask = (CancellableTask) task;

        responseCache.get(cancellableTask::isCancelled, listener);
    }

    private GetAutoscalingCapacityAction.Response computeCapacity(Runnable ensureNotCancelled) {
        GetAutoscalingCapacityAction.Response response = new GetAutoscalingCapacityAction.Response(
            capacityService.calculate(
                clusterService.state(),
                clusterInfoService.getClusterInfo(),
                snapshotsInfoService.snapshotShardSizes(),
                nodeInfoService.snapshot(),
                ensureNotCancelled
            )
        );
        logger.debug("autoscaling capacity response [{}]", response);
        return response;
    }

    @Override
    protected ClusterBlockException checkBlock(final GetAutoscalingCapacityAction.Request request, final ClusterState state) {
        return null;
    }

    // for tests
    int responseCacheQueueSize() {
        return responseCache.jobQueueSize();
    }
}
