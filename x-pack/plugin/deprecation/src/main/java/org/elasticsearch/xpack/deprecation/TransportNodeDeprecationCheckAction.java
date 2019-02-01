/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.deprecation.DeprecationInfoAction;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.deprecation.NodesDeprecationCheckAction;
import org.elasticsearch.xpack.core.deprecation.NodesDeprecationCheckRequest;
import org.elasticsearch.xpack.core.deprecation.NodesDeprecationCheckResponse;

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
                                               PluginsService pluginsService, ActionFilters actionFilters,
                                               IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, NodesDeprecationCheckAction.NAME, threadPool, clusterService, transportService, actionFilters,
            indexNameExpressionResolver,
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
    protected NodesDeprecationCheckAction.NodeRequest newNodeRequest(String nodeId, NodesDeprecationCheckRequest request) {
        return new NodesDeprecationCheckAction.NodeRequest(nodeId, request);
    }

    @Override
    protected NodesDeprecationCheckAction.NodeResponse newNodeResponse() {
        return new NodesDeprecationCheckAction.NodeResponse();
    }

    @Override
    protected NodesDeprecationCheckAction.NodeResponse nodeOperation(NodesDeprecationCheckAction.NodeRequest request) {
        List<DeprecationIssue> issues = DeprecationInfoAction.filterChecks(DeprecationChecks.NODE_SETTINGS_CHECKS,
            (c) -> c.apply(settings, pluginsService.info()));

        return new NodesDeprecationCheckAction.NodeResponse(transportService.getLocalNode(), issues);
    }


}
