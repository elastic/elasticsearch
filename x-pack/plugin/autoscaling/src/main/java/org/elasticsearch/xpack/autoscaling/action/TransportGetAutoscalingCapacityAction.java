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
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.snapshots.SnapshotsInfoService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.autoscaling.AutoscalingLicenseChecker;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCalculateCapacityService;
import org.elasticsearch.xpack.autoscaling.capacity.memory.AutoscalingMemoryInfoService;

import java.util.Objects;

public class TransportGetAutoscalingCapacityAction extends TransportMasterNodeAction<
    GetAutoscalingCapacityAction.Request,
    GetAutoscalingCapacityAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetAutoscalingCapacityAction.class);

    private final AutoscalingCalculateCapacityService capacityService;
    private final ClusterInfoService clusterInfoService;
    private final SnapshotsInfoService snapshotsInfoService;
    private final AutoscalingMemoryInfoService memoryInfoService;
    private final AutoscalingLicenseChecker autoscalingLicenseChecker;

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
        final AutoscalingMemoryInfoService memoryInfoService,
        final AllocationDeciders allocationDeciders,
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
        this.memoryInfoService = memoryInfoService;
        this.capacityService = capacityServiceHolder.get(allocationDeciders);
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

        GetAutoscalingCapacityAction.Response response = new GetAutoscalingCapacityAction.Response(
            capacityService.calculate(
                state,
                clusterInfoService.getClusterInfo(),
                snapshotsInfoService.snapshotShardSizes(),
                memoryInfoService.snapshot()
            )
        );
        logger.debug("autoscaling capacity response [{}]", response);
        listener.onResponse(response);
    }

    @Override
    protected ClusterBlockException checkBlock(final GetAutoscalingCapacityAction.Request request, final ClusterState state) {
        return null;
    }

}
