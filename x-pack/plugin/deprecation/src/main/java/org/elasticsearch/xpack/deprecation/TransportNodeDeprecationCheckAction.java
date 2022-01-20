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
import org.elasticsearch.cluster.metadata.DiffableStringMap;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class TransportNodeDeprecationCheckAction extends TransportNodesAction<
    NodesDeprecationCheckRequest,
    NodesDeprecationCheckResponse,
    NodesDeprecationCheckAction.NodeRequest,
    NodesDeprecationCheckAction.NodeResponse> {

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
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            NodesDeprecationCheckRequest::new,
            NodesDeprecationCheckAction.NodeRequest::new,
            ThreadPool.Names.GENERIC,
            NodesDeprecationCheckAction.NodeResponse.class
        );
        this.settings = settings;
        this.pluginsService = pluginsService;
        this.licenseState = licenseState;
        skipTheseDeprecations = DeprecationChecks.SKIP_DEPRECATIONS_SETTING.get(settings);
        // Safe to register this here because it happens synchronously before the cluster service is started:
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DeprecationChecks.SKIP_DEPRECATIONS_SETTING, this::setSkipDeprecations);
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
        return new NodesDeprecationCheckAction.NodeRequest(request);
    }

    @Override
    protected NodesDeprecationCheckAction.NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodesDeprecationCheckAction.NodeResponse(in);
    }

    @Override
    protected NodesDeprecationCheckAction.NodeResponse nodeOperation(NodesDeprecationCheckAction.NodeRequest request) {
        return nodeOperation(request, DeprecationChecks.NODE_SETTINGS_CHECKS);
    }

    NodesDeprecationCheckAction.NodeResponse nodeOperation(
        NodesDeprecationCheckAction.NodeRequest request,
        List<
            DeprecationChecks.NodeDeprecationCheck<
                Settings,
                PluginsAndModules,
                ClusterState,
                XPackLicenseState,
                DeprecationIssue>> nodeSettingsChecks
    ) {
        Settings filteredNodeSettings = settings.filter(setting -> Regex.simpleMatch(skipTheseDeprecations, setting) == false);
        List<DeprecationIssue> issues = DeprecationInfoAction.filterChecks(
            nodeSettingsChecks,
            (c) -> c.apply(
                filteredNodeSettings,
                pluginsService.info(),
                new FilteredClusterState(clusterService.state(), skipTheseDeprecations),
                licenseState
            )
        );

        return new NodesDeprecationCheckAction.NodeResponse(transportService.getLocalNode(), issues);
    }

    private static final class FilteredClusterState extends ClusterState {
        private final Metadata filteredMetadata;

        FilteredClusterState(ClusterState state, List<String> skipTheseDeprecations) {
            super(state.version(), state.stateUUID(), state);
            this.filteredMetadata = new FilteredMetadata(super.metadata(), skipTheseDeprecations);
        }

        @Override
        public Metadata metadata() {
            return this.filteredMetadata;
        }
    }

    private static final class FilteredMetadata extends Metadata {
        FilteredMetadata(Metadata originalMetadata, List<String> skipTheseDeprecations) {
            super(
                originalMetadata.clusterUUID(),
                originalMetadata.clusterUUIDCommitted(),
                originalMetadata.version(),
                originalMetadata.coordinationMetadata(),
                originalMetadata.transientSettings() == null
                    ? null
                    : originalMetadata.transientSettings().filter(setting -> Regex.simpleMatch(skipTheseDeprecations, setting) == false),
                originalMetadata.persistentSettings() == null
                    ? null
                    : originalMetadata.persistentSettings().filter(setting -> Regex.simpleMatch(skipTheseDeprecations, setting) == false),
                originalMetadata.settings() == null
                    ? null
                    : originalMetadata.settings().filter(setting -> Regex.simpleMatch(skipTheseDeprecations, setting) == false),
                (DiffableStringMap) originalMetadata.hashesOfConsistentSettings(),
                originalMetadata.getTotalNumberOfShards(),
                originalMetadata.getTotalOpenIndexShards(),
                originalMetadata.indices(),
                originalMetadata.templates(),
                originalMetadata.customs(),
                originalMetadata.getConcreteAllIndices(),
                originalMetadata.getConcreteVisibleIndices(),
                originalMetadata.getConcreteAllOpenIndices(),
                originalMetadata.getConcreteVisibleOpenIndices(),
                originalMetadata.getConcreteAllClosedIndices(),
                originalMetadata.getConcreteVisibleClosedIndices(),
                originalMetadata.getIndicesLookup(),
                originalMetadata.oldestIndexVersion()
            );
        }
    }

}
