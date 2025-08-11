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
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING;

public class TransportNodeDeprecationCheckAction extends TransportNodesAction<
    NodesDeprecationCheckRequest,
    NodesDeprecationCheckResponse,
    NodesDeprecationCheckAction.NodeRequest,
    NodesDeprecationCheckAction.NodeResponse> {

    private final Settings settings;
    private final XPackLicenseState licenseState;
    private final PluginsService pluginsService;
    private final ClusterInfoService clusterInfoService;
    private volatile List<String> skipTheseDeprecations;

    @Inject
    public TransportNodeDeprecationCheckAction(
        Settings settings,
        ThreadPool threadPool,
        XPackLicenseState licenseState,
        ClusterService clusterService,
        TransportService transportService,
        PluginsService pluginsService,
        ActionFilters actionFilters,
        ClusterInfoService clusterInfoService
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
        this.clusterInfoService = clusterInfoService;
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

        Metadata metadata = clusterService.state().metadata();
        Settings transientSettings = metadata.transientSettings()
            .filter(setting -> Regex.simpleMatch(skipTheseDeprecations, setting) == false);
        Settings persistentSettings = metadata.persistentSettings()
            .filter(setting -> Regex.simpleMatch(skipTheseDeprecations, setting) == false);
        ClusterState filteredClusterState = ClusterState.builder(clusterService.state())
            .metadata(Metadata.builder(metadata).transientSettings(transientSettings).persistentSettings(persistentSettings).build())
            .build();

        List<DeprecationIssue> issues = DeprecationInfoAction.filterChecks(
            nodeSettingsChecks,
            (c) -> c.apply(filteredNodeSettings, pluginsService.info(), filteredClusterState, licenseState)
        );
        DeprecationIssue watermarkIssue = checkDiskLowWatermark(
            filteredNodeSettings,
            filteredClusterState.metadata().settings(),
            clusterInfoService.getClusterInfo(),
            clusterService.getClusterSettings(),
            transportService.getLocalNode().getId()
        );
        if (watermarkIssue != null) {
            issues.add(watermarkIssue);
        }
        return new NodesDeprecationCheckAction.NodeResponse(transportService.getLocalNode(), issues);
    }

    static DeprecationIssue checkDiskLowWatermark(
        Settings nodeSettings,
        Settings dynamicSettings,
        ClusterInfo clusterInfo,
        ClusterSettings clusterSettings,
        String nodeId
    ) {
        DiskUsage usage = clusterInfo.getNodeMostAvailableDiskUsages().get(nodeId);
        if (usage != null) {
            long freeBytes = usage.getFreeBytes();
            double freeDiskPercentage = usage.getFreeDiskAsPercentage();
            if (exceedsLowWatermark(nodeSettings, clusterSettings, freeBytes, freeDiskPercentage)
                || exceedsLowWatermark(dynamicSettings, clusterSettings, freeBytes, freeDiskPercentage)) {
                return new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "Disk usage exceeds low watermark",
                    "https://ela.st/es-deprecation-7-disk-watermark-exceeded",
                    String.format(
                        Locale.ROOT,
                        "Disk usage exceeds low watermark, which will prevent reindexing indices during upgrade. Get disk usage on "
                            + "all nodes below the value specified in %s",
                        CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey()
                    ),
                    false,
                    null
                );
            }
        }
        return null;
    }

    private static boolean exceedsLowWatermark(
        Settings settingsToCheck,
        ClusterSettings clusterSettings,
        long freeBytes,
        double freeDiskPercentage
    ) {
        DiskThresholdSettings diskThresholdSettings = new DiskThresholdSettings(settingsToCheck, clusterSettings);
        if (freeBytes < diskThresholdSettings.getFreeBytesThresholdLow().getBytes()
            || freeDiskPercentage < diskThresholdSettings.getFreeDiskThresholdLow()) {
            return true;
        }
        return false;
    }

}
