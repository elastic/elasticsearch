/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.version;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

// @UpdateForV10 // this can be removed in v10. It may be called by v8 nodes to v9 nodes.
public class TransportSystemIndexMappingsVersionsAction extends TransportNodesAction<
    SystemIndexMappingsVersionsRequest,
    SystemIndexMappingsVersionsResponse,
    TransportSystemIndexMappingsVersionsAction.Request,
    SystemIndexMappingsVersions,
    Void> {

    public static final ActionType<SystemIndexMappingsVersionsResponse> TYPE = new ActionType<>("cluster:monitor/versions");

    private final CompatibilityVersions compatibilityVersions;

    @Inject
    public TransportSystemIndexMappingsVersionsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        CompatibilityVersions compatibilityVersions
    ) {
        super(
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            TransportSystemIndexMappingsVersionsAction.Request::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.compatibilityVersions = compatibilityVersions;
    }

    @Override
    protected SystemIndexMappingsVersionsResponse newResponse(
        SystemIndexMappingsVersionsRequest request,
        List<SystemIndexMappingsVersions> responses,
        List<FailedNodeException> failures
    ) {
        return new SystemIndexMappingsVersionsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected Request newNodeRequest(SystemIndexMappingsVersionsRequest request) {
        return new Request();
    }

    @Override
    protected SystemIndexMappingsVersions newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new SystemIndexMappingsVersions(in);
    }

    @Override
    protected SystemIndexMappingsVersions nodeOperation(Request request, Task task) {
        return new SystemIndexMappingsVersions(compatibilityVersions.systemIndexMappingsVersion(), transportService.getLocalNode());
    }

    public static class Request extends TransportRequest {
        public Request(StreamInput in) throws IOException {
            super(in);
        }

        public Request() {}
    }
}
