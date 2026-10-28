/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.stats;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.stats.GetSecurityStatsAction;
import org.elasticsearch.xpack.core.security.action.stats.GetSecurityStatsNodeRequest;
import org.elasticsearch.xpack.core.security.action.stats.GetSecurityStatsNodeResponse;
import org.elasticsearch.xpack.core.security.action.stats.GetSecurityStatsNodesRequest;
import org.elasticsearch.xpack.core.security.action.stats.GetSecurityStatsNodesResponse;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.io.IOException;
import java.util.List;

public class TransportSecurityStatsAction extends TransportNodesAction<
    GetSecurityStatsNodesRequest,
    GetSecurityStatsNodesResponse,
    GetSecurityStatsNodeRequest,
    GetSecurityStatsNodeResponse,
    Void> {

    @Nullable
    private final CompositeRolesStore rolesStore;

    @Inject
    public TransportSecurityStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        CompositeRolesStore rolesStore
    ) {
        super(
            GetSecurityStatsAction.INSTANCE.name(),
            clusterService,
            transportService,
            actionFilters,
            GetSecurityStatsNodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.rolesStore = rolesStore;
    }

    @Override
    protected GetSecurityStatsNodesResponse newResponse(
        final GetSecurityStatsNodesRequest request,
        final List<GetSecurityStatsNodeResponse> responses,
        final List<FailedNodeException> failures
    ) {
        return new GetSecurityStatsNodesResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected GetSecurityStatsNodeRequest newNodeRequest(final GetSecurityStatsNodesRequest request) {
        return new GetSecurityStatsNodeRequest();
    }

    @Override
    protected GetSecurityStatsNodeResponse newNodeResponse(final StreamInput in, final DiscoveryNode node) throws IOException {
        return new GetSecurityStatsNodeResponse(in);
    }

    @Override
    protected GetSecurityStatsNodeResponse nodeOperation(final GetSecurityStatsNodeRequest request, final Task task) {
        return new GetSecurityStatsNodeResponse(clusterService.localNode(), rolesStore == null ? null : rolesStore.usageStatsWithJustDls());
    }
}
