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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.autoscaling.AutoscalingLicenseChecker;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCalculateCapacityService;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicyMetadata;

import java.util.Collections;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

public class TransportPutAutoscalingPolicyAction extends AcknowledgedTransportMasterNodeAction<PutAutoscalingPolicyAction.Request> {

    private static final Logger LOGGER = LogManager.getLogger(TransportPutAutoscalingPolicyAction.class);

    private final PolicyValidator policyValidator;
    private final AutoscalingLicenseChecker autoscalingLicenseChecker;

    @Inject
    public TransportPutAutoscalingPolicyAction(
        final TransportService transportService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final AllocationDeciders allocationDeciders,
        final AutoscalingCalculateCapacityService.Holder policyValidatorHolder,
        final AutoscalingLicenseChecker autoscalingLicenseChecker
    ) {
        this(
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver,
            policyValidatorHolder.get(allocationDeciders),
            autoscalingLicenseChecker
        );
    }

    TransportPutAutoscalingPolicyAction(
        final TransportService transportService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final PolicyValidator policyValidator,
        final AutoscalingLicenseChecker autoscalingLicenseChecker
    ) {
        super(
            PutAutoscalingPolicyAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutAutoscalingPolicyAction.Request::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );
        this.policyValidator = policyValidator;
        this.autoscalingLicenseChecker = Objects.requireNonNull(autoscalingLicenseChecker);
    }

    @Override
    protected void masterOperation(
        final Task task,
        final PutAutoscalingPolicyAction.Request request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        if (autoscalingLicenseChecker.isAutoscalingAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException("autoscaling"));
            return;
        }

        clusterService.submitStateUpdateTask("put-autoscaling-policy", new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(final ClusterState currentState) {
                return putAutoscalingPolicy(currentState, request, policyValidator, LOGGER);
            }
        }, newExecutor());
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private static <T extends ClusterStateUpdateTask> ClusterStateTaskExecutor<T> newExecutor() {
        return ClusterStateTaskExecutor.unbatched();
    }

    @Override
    protected ClusterBlockException checkBlock(final PutAutoscalingPolicyAction.Request request, final ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    static ClusterState putAutoscalingPolicy(
        final ClusterState currentState,
        final PutAutoscalingPolicyAction.Request request,
        final PolicyValidator policyValidator,
        final Logger logger
    ) {
        // we allow putting policies with roles that not all nodes in the cluster may understand currently (but the current master must
        // know it). The expectation is that the mixed cluster situation will be healed soon. See also
        // AutoscalingCalculateCapacityService#hasUnknownRoles where we shortcut decision making if master node does not know all roles.
        final ClusterState.Builder builder = ClusterState.builder(currentState);
        final AutoscalingMetadata currentMetadata;
        if (currentState.metadata().custom(AutoscalingMetadata.NAME) != null) {
            currentMetadata = currentState.metadata().custom(AutoscalingMetadata.NAME);
        } else {
            currentMetadata = AutoscalingMetadata.EMPTY;
        }
        final AutoscalingPolicy updatedPolicy;
        AutoscalingPolicyMetadata existingPolicyMetadata = currentMetadata.policies().get(request.name());
        if (existingPolicyMetadata == null) {
            if (request.roles() == null) {
                throw new IllegalArgumentException(
                    "new policy " + request.name() + " with no roles defined, must provide empty list for no roles"
                );
            }
            updatedPolicy = new AutoscalingPolicy(
                request.name(),
                request.roles(),
                request.deciders() != null ? request.deciders() : Collections.emptySortedMap()
            );
        } else {
            AutoscalingPolicy existing = existingPolicyMetadata.policy();
            updatedPolicy = new AutoscalingPolicy(
                request.name(),
                request.roles() != null ? request.roles() : existing.roles(),
                request.deciders() != null ? request.deciders() : existing.deciders()
            );
        }

        policyValidator.validate(updatedPolicy);

        final SortedMap<String, AutoscalingPolicyMetadata> newPolicies = new TreeMap<>(currentMetadata.policies());
        final AutoscalingPolicyMetadata newPolicyMetadata = new AutoscalingPolicyMetadata(updatedPolicy);
        final AutoscalingPolicyMetadata oldPolicyMetadata = newPolicies.put(request.name(), newPolicyMetadata);
        if (oldPolicyMetadata == null) {
            logger.info("adding autoscaling policy [{}]", request.name());
        } else if (oldPolicyMetadata.equals(newPolicyMetadata)) {
            logger.info("skipping updating autoscaling policy [{}] due to no change in policy", request.name());
            return currentState;
        } else {
            logger.info("updating autoscaling policy [{}]", request.name());
        }
        final AutoscalingMetadata newMetadata = new AutoscalingMetadata(newPolicies);
        builder.metadata(Metadata.builder(currentState.getMetadata()).putCustom(AutoscalingMetadata.NAME, newMetadata).build());
        return builder.build();
    }
}
