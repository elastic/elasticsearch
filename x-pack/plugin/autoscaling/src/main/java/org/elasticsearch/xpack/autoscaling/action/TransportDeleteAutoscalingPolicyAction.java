/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicyMetadata;

import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class TransportDeleteAutoscalingPolicyAction extends AcknowledgedTransportMasterNodeAction<DeleteAutoscalingPolicyAction.Request> {

    private static final Logger LOGGER = LogManager.getLogger(TransportPutAutoscalingPolicyAction.class);

    @Inject
    public TransportDeleteAutoscalingPolicyAction(
        final TransportService transportService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            DeleteAutoscalingPolicyAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteAutoscalingPolicyAction.Request::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );
    }

    @Override
    protected void masterOperation(
        final Task task,
        final DeleteAutoscalingPolicyAction.Request request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        // no license check, we will allow deleting policies even if the license is out of compliance, for cleanup purposes

        submitUnbatchedTask("delete-autoscaling-policy", new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(final ClusterState currentState) {
                return deleteAutoscalingPolicy(currentState, request.name(), LOGGER);
            }
        });
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    @Override
    protected ClusterBlockException checkBlock(final DeleteAutoscalingPolicyAction.Request request, final ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    /**
     * Used by the reserved cluster state action handler for autoscaling policy
     */
    static ClusterState deleteAutoscalingPolicy(final ClusterState currentState, final String name) {
        return deleteAutoscalingPolicy(currentState, name, LOGGER);
    }

    static ClusterState deleteAutoscalingPolicy(final ClusterState currentState, final String name, final Logger logger) {
        final ClusterState.Builder builder = ClusterState.builder(currentState);
        final AutoscalingMetadata currentMetadata;
        if (currentState.metadata().custom(AutoscalingMetadata.NAME) != null) {
            currentMetadata = currentState.metadata().custom(AutoscalingMetadata.NAME);
        } else {
            // we will reject the request below when we try to look up the policy by name
            currentMetadata = AutoscalingMetadata.EMPTY;
        }
        boolean wildcard = Regex.isSimpleMatchPattern(name);
        if (wildcard == false && currentMetadata.policies().containsKey(name) == false) {
            throw new ResourceNotFoundException("autoscaling policy with name [" + name + "] does not exist");
        }

        final SortedMap<String, AutoscalingPolicyMetadata> newPolicies = new TreeMap<>(currentMetadata.policies());
        if (wildcard) {
            newPolicies.keySet().removeIf(key -> Regex.simpleMatch(name, key));
            logger.info("deleting [{}] autoscaling policies", currentMetadata.policies().size() - newPolicies.size());
        } else {
            final AutoscalingPolicyMetadata policy = newPolicies.remove(name);
            assert policy != null : name;
            logger.info("deleting autoscaling policy [{}]", name);
        }
        final AutoscalingMetadata newMetadata = new AutoscalingMetadata(newPolicies);
        builder.metadata(Metadata.builder(currentState.getMetadata()).putCustom(AutoscalingMetadata.NAME, newMetadata).build());
        return builder.build();
    }

    @Override
    public Optional<String> reservedStateHandlerName() {
        return Optional.of(ReservedAutoscalingPolicyAction.NAME);
    }

    @Override
    public Set<String> modifiedKeys(DeleteAutoscalingPolicyAction.Request request) {
        return Set.of(request.name());
    }
}
