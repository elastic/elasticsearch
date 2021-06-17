/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportNodeDeprecationCheckAction extends TransportNodesAction<NodesDeprecationCheckRequest,
    NodesDeprecationCheckResponse,
    NodesDeprecationCheckAction.NodeRequest,
    NodesDeprecationCheckAction.NodeResponse> {

    private final Settings settings;
    private final PluginsService pluginsService;

    @Inject
    public TransportNodeDeprecationCheckAction(Settings settings, ThreadPool threadPool,
                                               ClusterService clusterService, TransportService transportService,
                                               PluginsService pluginsService, ActionFilters actionFilters) {
        super(NodesDeprecationCheckAction.NAME, threadPool, clusterService, transportService, actionFilters,
            NodesDeprecationCheckRequest::new,
            NodesDeprecationCheckAction.NodeRequest::new,
            ThreadPool.Names.GENERIC,
            NodesDeprecationCheckAction.NodeResponse.class);
        this.settings = settings;
        this.pluginsService = pluginsService;
    }

    @Override
    protected NodesDeprecationCheckResponse newResponse(NodesDeprecationCheckRequest request,
                                                        List<NodesDeprecationCheckAction.NodeResponse> nodeResponses,
                                                        List<FailedNodeException> failures) {
        return new NodesDeprecationCheckResponse(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected NodesDeprecationCheckAction.NodeRequest newNodeRequest(NodesDeprecationCheckRequest request) {
        return new NodesDeprecationCheckAction.NodeRequest(request);
    }

    @Override
    protected NodesDeprecationCheckAction.NodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new NodesDeprecationCheckAction.NodeResponse(in);
    }

    @Override
    protected NodesDeprecationCheckAction.NodeResponse nodeOperation(NodesDeprecationCheckAction.NodeRequest request, Task task) {
        List<DeprecationIssue> issues = DeprecationInfoAction.filterChecks(DeprecationChecks.NODE_SETTINGS_CHECKS,
            (c) -> c.apply(settings, pluginsService.info()));

        return new NodesDeprecationCheckAction.NodeResponse(transportService.getLocalNode(), issues);
    }


}
