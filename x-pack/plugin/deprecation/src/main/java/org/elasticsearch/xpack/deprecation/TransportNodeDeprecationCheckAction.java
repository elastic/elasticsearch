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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TransportNodeDeprecationCheckAction extends TransportNodesAction<
    NodesDeprecationCheckRequest,
    NodesDeprecationCheckResponse,
    NodesDeprecationCheckAction.NodeRequest,
    NodesDeprecationCheckAction.NodeResponse,
    Void> {

    private final Settings settings;
    private final XPackLicenseState licenseState;
    private final PluginsService pluginsService;
    private volatile List<String> skipTheseDeprecations;

    @Inject
    public TransportNodeDeprecationCheckAction(
        Settings settings,
        ThreadPool threadPool,
        XPackLicenseState licenseState,
        ClusterService clusterService,
        TransportService transportService,
        PluginsService pluginsService,
        ActionFilters actionFilters
    ) {
        super(
            NodesDeprecationCheckAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            NodesDeprecationCheckAction.NodeRequest::new,
            threadPool.executor(ThreadPool.Names.GENERIC)
        );
        this.settings = settings;
        this.pluginsService = pluginsService;
        this.licenseState = licenseState;
        skipTheseDeprecations = TransportDeprecationInfoAction.SKIP_DEPRECATIONS_SETTING.get(settings);
        // Safe to register this here because it happens synchronously before the cluster service is started:
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(TransportDeprecationInfoAction.SKIP_DEPRECATIONS_SETTING, this::setSkipDeprecations);
    }

    private <T> void setSkipDeprecations(List<String> skipDeprecations) {
        this.skipTheseDeprecations = Collections.unmodifiableList(skipDeprecations);
    }

    @Override
    protected NodesDeprecationCheckResponse newResponse(
        NodesDeprecationCheckRequest request,
        List<NodesDeprecationCheckAction.NodeResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        return new NodesDeprecationCheckResponse(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected NodesDeprecationCheckAction.NodeRequest newNodeRequest(NodesDeprecationCheckRequest request) {
        return new NodesDeprecationCheckAction.NodeRequest();
    }

    @Override
    protected NodesDeprecationCheckAction.NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodesDeprecationCheckAction.NodeResponse(in);
    }

    @Override
    protected NodesDeprecationCheckAction.NodeResponse nodeOperation(NodesDeprecationCheckAction.NodeRequest request, Task task) {
        return nodeOperation(NodeDeprecationChecks.SINGLE_NODE_CHECKS);
    }

    NodesDeprecationCheckAction.NodeResponse nodeOperation(
        List<
            NodeDeprecationChecks.NodeDeprecationCheck<
                Settings,
                PluginsAndModules,
                ClusterState,
                XPackLicenseState,
                DeprecationIssue>> nodeSettingsChecks
    ) {
        Settings filteredNodeSettings = settings.filter(setting -> Regex.simpleMatch(skipTheseDeprecations, setting) == false);

        Metadata metadata = clusterService.state().metadata();
        Settings transientSettings = metadata.transientSettings()
            .filter(setting -> Regex.simpleMatch(skipTheseDeprecations, setting) == false);
        Settings persistentSettings = metadata.persistentSettings()
            .filter(setting -> Regex.simpleMatch(skipTheseDeprecations, setting) == false);
        ClusterState filteredClusterState = ClusterState.builder(clusterService.state())
            .metadata(Metadata.builder(metadata).transientSettings(transientSettings).persistentSettings(persistentSettings).build())
            .build();

        List<DeprecationIssue> issues = nodeSettingsChecks.stream()
            .map(c -> c.apply(filteredNodeSettings, pluginsService.info(), filteredClusterState, licenseState))
            .filter(Objects::nonNull)
            .toList();
        return new NodesDeprecationCheckAction.NodeResponse(transportService.getLocalNode(), issues);
    }
}
