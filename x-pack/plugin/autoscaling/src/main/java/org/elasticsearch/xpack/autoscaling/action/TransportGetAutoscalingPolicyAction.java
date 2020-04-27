/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;

import java.io.IOException;

public class TransportGetAutoscalingPolicyAction extends TransportMasterNodeAction<
    GetAutoscalingPolicyAction.Request,
    GetAutoscalingPolicyAction.Response> {

    @Inject
    public TransportGetAutoscalingPolicyAction(
        final TransportService transportService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetAutoscalingPolicyAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetAutoscalingPolicyAction.Request::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetAutoscalingPolicyAction.Response read(final StreamInput in) throws IOException {
        return new GetAutoscalingPolicyAction.Response(in);
    }

    @Override
    protected void masterOperation(
        final Task task,
        final GetAutoscalingPolicyAction.Request request,
        final ClusterState state,
        final ActionListener<GetAutoscalingPolicyAction.Response> listener
    ) {
        listener.onResponse(new GetAutoscalingPolicyAction.Response(getAutoscalingPolicy(state, request.name())));
    }

    static AutoscalingPolicy getAutoscalingPolicy(final ClusterState state, final String name) {
        final AutoscalingMetadata metadata;
        if (state.metadata().custom(AutoscalingMetadata.NAME) != null) {
            metadata = state.metadata().custom(AutoscalingMetadata.NAME);
        } else {
            // we will reject the request below when we try to look up the policy by name
            metadata = AutoscalingMetadata.EMPTY;
        }
        if (metadata.policies().containsKey(name) == false) {
            throw new ResourceNotFoundException("autoscaling policy with name [" + name + "] does not exist");
        }
        return metadata.policies().get(name).policy();
    }

    @Override
    protected ClusterBlockException checkBlock(final GetAutoscalingPolicyAction.Request request, final ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

}
