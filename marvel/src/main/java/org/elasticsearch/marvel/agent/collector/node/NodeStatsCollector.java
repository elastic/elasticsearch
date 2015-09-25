/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.node;


import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.inject.ConfigurationException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.ProvisionException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.marvel.agent.collector.AbstractCollector;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.license.LicenseService;
import org.elasticsearch.node.service.NodeService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Collector for nodes statistics.
 * <p>
 * This collector runs on every non-client node and collect
 * a {@link NodeStatsMarvelDoc} document for each node of the cluster.
 */
public class NodeStatsCollector extends AbstractCollector<NodeStatsCollector> {

    public static final String NAME = "node-stats-collector";
    public static final String TYPE = "node_stats";

    private final NodeService nodeService;
    private final DiscoveryService discoveryService;
    private final NodeEnvironment nodeEnvironment;

    // Use a provider in order to avoid Guice circular injection
    // issues because AllocationDecider is not an interface and cannot be proxied
    private final Provider<DiskThresholdDecider> diskThresholdDeciderProvider;

    @Inject
    public NodeStatsCollector(Settings settings, ClusterService clusterService, MarvelSettings marvelSettings, LicenseService licenseService,
                              NodeService nodeService, DiscoveryService discoveryService, NodeEnvironment nodeEnvironment,
                              Provider<DiskThresholdDecider> diskThresholdDeciderProvider) {
        super(settings, NAME, clusterService, marvelSettings, licenseService);
        this.nodeService = nodeService;
        this.discoveryService = discoveryService;
        this.nodeEnvironment = nodeEnvironment;
        this.diskThresholdDeciderProvider = diskThresholdDeciderProvider;
    }

    @Override
    protected boolean canCollect() {
        return super.canCollect() && nodeEnvironment.hasNodeFile();
    }

    @Override
    protected Collection<MarvelDoc> doCollect() throws Exception {
        List<MarvelDoc> results = new ArrayList<>(1);

        NodeStats nodeStats = nodeService.stats();

        DiskThresholdDecider diskThresholdDecider = null;
        try {
            diskThresholdDecider = diskThresholdDeciderProvider.get();
        } catch (ProvisionException | ConfigurationException e) {
            logger.warn("unable to retrieve disk threshold decider information", e);
        }

        // Here we are calling directly the DiskThresholdDecider to retrieve the high watermark value
        // It would be nicer to use a settings API like documented in #6732
        Double diskThresholdWatermarkHigh = (diskThresholdDecider != null) ? 100.0 - diskThresholdDecider.getFreeDiskThresholdHigh() : -1;
        boolean diskThresholdDeciderEnabled = (diskThresholdDecider != null) && diskThresholdDecider.isEnabled();

        results.add(new NodeStatsMarvelDoc(clusterUUID(), TYPE, System.currentTimeMillis(),
                discoveryService.localNode().id(), isLocalNodeMaster(), nodeStats,
                BootstrapInfo.isMemoryLocked(), diskThresholdWatermarkHigh, diskThresholdDeciderEnabled));

        return Collections.unmodifiableCollection(results);
    }
}
