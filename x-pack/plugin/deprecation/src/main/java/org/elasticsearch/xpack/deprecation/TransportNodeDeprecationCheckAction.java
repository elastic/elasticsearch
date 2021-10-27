/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

public class TransportNodeDeprecationCheckAction extends TransportNodesAction<NodesDeprecationCheckRequest,
    NodesDeprecationCheckResponse,
    NodesDeprecationCheckAction.NodeRequest,
    NodesDeprecationCheckAction.NodeResponse> {

    private final Settings settings;
    private final PluginsService pluginsService;
    private volatile List<String> skipTheseDeprecations;

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
        skipTheseDeprecations = DeprecationChecks.SKIP_DEPRECATIONS_SETTING.get(settings);
        // Safe to register this here because it happens synchronously before the cluster service is started:
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DeprecationChecks.SKIP_DEPRECATIONS_SETTING,
            this::setSkipDeprecations);
    }

    private <T> void setSkipDeprecations(List<String> skipDeprecations) {
        this.skipTheseDeprecations = Collections.unmodifiableList(skipDeprecations);
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
    protected NodesDeprecationCheckAction.NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodesDeprecationCheckAction.NodeResponse(in);
    }

    @Override
    protected NodesDeprecationCheckAction.NodeResponse nodeOperation(NodesDeprecationCheckAction.NodeRequest request, Task task) {
        return nodeOperation(request, DeprecationChecks.NODE_SETTINGS_CHECKS);
    }

    NodesDeprecationCheckAction.NodeResponse nodeOperation(NodesDeprecationCheckAction.NodeRequest request,
                                                           List<BiFunction<Settings, PluginsAndModules,
                                                               DeprecationIssue>> nodeSettingsChecks) {
        Settings filteredSettings = settings.filter(setting -> Regex.simpleMatch(skipTheseDeprecations, setting) == false);
        List<DeprecationIssue> issues = DeprecationInfoAction.filterChecks(nodeSettingsChecks,
            (c) -> c.apply(filteredSettings, pluginsService.info()));

        return new NodesDeprecationCheckAction.NodeResponse(transportService.getLocalNode(), issues);
    }

}
