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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountFileTokensAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountFileTokensRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountFileTokensResponse;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.security.authc.service.FileServiceAccountTokenStore;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;

import java.io.IOException;
import java.util.List;

public class TransportGetServiceAccountFileTokensAction extends TransportNodesAction<
    GetServiceAccountFileTokensRequest, GetServiceAccountFileTokensResponse,
    GetServiceAccountFileTokensRequest.Node, GetServiceAccountFileTokensResponse.Node> {

    private final FileServiceAccountTokenStore fileServiceAccountTokenStore;

    @Inject
    public TransportGetServiceAccountFileTokensAction(ThreadPool threadPool, ClusterService clusterService,
                                                      TransportService transportService, ActionFilters actionFilters,
                                                      FileServiceAccountTokenStore fileServiceAccountTokenStore) {
        super(GetServiceAccountFileTokensAction.NAME, threadPool, clusterService, transportService, actionFilters,
            GetServiceAccountFileTokensRequest::new, GetServiceAccountFileTokensRequest.Node::new,
            ThreadPool.Names.SAME, GetServiceAccountFileTokensResponse.Node.class);
        this.fileServiceAccountTokenStore = fileServiceAccountTokenStore;
    }

    @Override
    protected GetServiceAccountFileTokensResponse newResponse(
        GetServiceAccountFileTokensRequest request,
        List<GetServiceAccountFileTokensResponse.Node> nodes,
        List<FailedNodeException> failures) {
        return new GetServiceAccountFileTokensResponse(clusterService.getClusterName(), nodes, failures);
    }

    @Override
    protected GetServiceAccountFileTokensRequest.Node newNodeRequest(GetServiceAccountFileTokensRequest request) {
        return new GetServiceAccountFileTokensRequest.Node(request);
    }

    @Override
    protected GetServiceAccountFileTokensResponse.Node newNodeResponse(StreamInput in) throws IOException {
        return new GetServiceAccountFileTokensResponse.Node(in);
    }

    @Override
    protected GetServiceAccountFileTokensResponse.Node nodeOperation(GetServiceAccountFileTokensRequest.Node request, Task task) {
        final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
        final List<TokenInfo> tokenInfos = fileServiceAccountTokenStore.findTokensFor(accountId);
        return new GetServiceAccountFileTokensResponse.Node(
            clusterService.localNode(),
            tokenInfos.stream().map(TokenInfo::getName).toArray(String[]::new));
    }
}
