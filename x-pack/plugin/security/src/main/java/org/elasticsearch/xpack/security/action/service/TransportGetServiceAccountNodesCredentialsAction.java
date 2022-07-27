/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.service;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsNodesRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsNodesResponse;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountNodesCredentialsAction;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.security.authc.service.FileServiceAccountTokenStore;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;

import java.io.IOException;
import java.util.List;

/**
 * This action handler is to retrieve service account credentials that are local to the node.
 * Currently this means file-backed service tokens.
 */
public class TransportGetServiceAccountNodesCredentialsAction extends TransportNodesAction<
    GetServiceAccountCredentialsNodesRequest,
    GetServiceAccountCredentialsNodesResponse,
    GetServiceAccountCredentialsNodesRequest.Node,
    GetServiceAccountCredentialsNodesResponse.Node> {

    private final FileServiceAccountTokenStore fileServiceAccountTokenStore;

    @Inject
    public TransportGetServiceAccountNodesCredentialsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        FileServiceAccountTokenStore fileServiceAccountTokenStore
    ) {
        super(
            GetServiceAccountNodesCredentialsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            GetServiceAccountCredentialsNodesRequest::new,
            GetServiceAccountCredentialsNodesRequest.Node::new,
            ThreadPool.Names.SAME,
            GetServiceAccountCredentialsNodesResponse.Node.class
        );
        this.fileServiceAccountTokenStore = fileServiceAccountTokenStore;
    }

    @Override
    protected GetServiceAccountCredentialsNodesResponse newResponse(
        GetServiceAccountCredentialsNodesRequest request,
        List<GetServiceAccountCredentialsNodesResponse.Node> nodes,
        List<FailedNodeException> failures
    ) {
        return new GetServiceAccountCredentialsNodesResponse(clusterService.getClusterName(), nodes, failures);
    }

    @Override
    protected GetServiceAccountCredentialsNodesRequest.Node newNodeRequest(GetServiceAccountCredentialsNodesRequest request) {
        return new GetServiceAccountCredentialsNodesRequest.Node(request);
    }

    @Override
    protected GetServiceAccountCredentialsNodesResponse.Node newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new GetServiceAccountCredentialsNodesResponse.Node(in);
    }

    @Override
    protected GetServiceAccountCredentialsNodesResponse.Node nodeOperation(
        GetServiceAccountCredentialsNodesRequest.Node request,
        Task task
    ) {
        final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
        final List<TokenInfo> tokenInfos = fileServiceAccountTokenStore.findTokensFor(accountId);
        return new GetServiceAccountCredentialsNodesResponse.Node(
            clusterService.localNode(),
            tokenInfos.stream().map(TokenInfo::getName).toArray(String[]::new)
        );
    }
}
