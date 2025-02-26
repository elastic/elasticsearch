/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.local.TransportLocalClusterStateAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.EnrichStore;

import java.util.HashMap;
import java.util.Map;

public class TransportGetEnrichPolicyAction extends TransportLocalClusterStateAction<
    GetEnrichPolicyAction.Request,
    GetEnrichPolicyAction.Response> {

    /**
     * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC it must be registered with the TransportService until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    @SuppressWarnings("this-escape")
    @Inject
    public TransportGetEnrichPolicyAction(TransportService transportService, ClusterService clusterService, ActionFilters actionFilters) {
        super(
            GetEnrichPolicyAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        transportService.registerRequestHandler(
            actionName,
            executor,
            false,
            true,
            GetEnrichPolicyAction.Request::new,
            (request, channel, task) -> executeDirect(task, request, new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        GetEnrichPolicyAction.Request request,
        ClusterState state,
        ActionListener<GetEnrichPolicyAction.Response> listener
    ) throws Exception {
        Map<String, EnrichPolicy> policies;
        if (request.getNames() == null || request.getNames().isEmpty()) {
            policies = EnrichStore.getPolicies(state);
        } else {
            policies = new HashMap<>();
            for (String name : request.getNames()) {
                if (name.isEmpty() == false) {
                    EnrichPolicy policy = EnrichStore.getPolicy(name, state);
                    if (policy != null) {
                        policies.put(name, policy);
                    }
                }
            }
        }
        ((CancellableTask) task).ensureNotCancelled();
        listener.onResponse(new GetEnrichPolicyAction.Response(policies));
    }

    @Override
    protected ClusterBlockException checkBlock(GetEnrichPolicyAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
