/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.cluster;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.action.XPackUsageRequestBuilder;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.collector.Collector;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.security.InternalClient;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Collector for cluster stats.
 * <p>
 * This collector runs on the master node. It collect data about the current
 * license and also retrieves the cluster stats.
 * <p>
 * the license and cluster stats are indexed in the data index in a "cluster_info"
 * document; the cluster stats are also indexed in the timestamped index in a
 * "cluster_stats" document.
 */
public class ClusterStatsCollector extends Collector {

    private final LicenseService licenseService;
    private final Client client;

    public ClusterStatsCollector(Settings settings, ClusterService clusterService,
                                 MonitoringSettings monitoringSettings,
                                 XPackLicenseState licenseState, InternalClient client,
                                 LicenseService licenseService) {
        super(settings, ClusterStatsMonitoringDoc.TYPE, clusterService, monitoringSettings, licenseState);
        this.client = client;
        this.licenseService = licenseService;
    }

    @Override
    protected boolean shouldCollect() {
        // This collector can always collect data on the master node
        return isLocalNodeMaster();
    }

    @Override
    protected Collection<MonitoringDoc> doCollect() throws Exception {
        final Supplier<ClusterStatsResponse> clusterStatsSupplier =
                () -> client.admin().cluster().prepareClusterStats()
                        .get(monitoringSettings.clusterStatsTimeout());
        final Supplier<List<XPackFeatureSet.Usage>> usageSupplier =
                () -> new XPackUsageRequestBuilder(client).get().getUsages();

        final ClusterStatsResponse clusterStats = clusterStatsSupplier.get();

        final long timestamp = System.currentTimeMillis();
        final String clusterUUID = clusterUUID();
        final DiscoveryNode sourceNode = localNode();
        final String clusterName = clusterService.getClusterName().value();
        final String version = Version.CURRENT.toString();
        final ClusterState clusterState = clusterService.state();
        final License license = licenseService.getLicense();
        final List<XPackFeatureSet.Usage> usage = collect(usageSupplier);

        // Adds a cluster stats document
        return Collections.singleton(
                new ClusterStatsMonitoringDoc(monitoringId(), monitoringVersion(),
                                              clusterUUID, timestamp, sourceNode, clusterName, version, license, usage,
                                              clusterStats, clusterState, clusterStats.getStatus()));
    }

    @Nullable
    private <T> T collect(final Supplier<T> supplier) {
        try {
            return supplier.get();
        } catch (ElasticsearchSecurityException e) {
            if (LicenseUtils.isLicenseExpiredException(e)) {
                logger.trace((Supplier<?>) () -> new ParameterizedMessage("collector [{}] - " +
                        "unable to collect data because of expired license", name()), e);
            } else {
                throw e;
            }
        }

        return null;
    }

}
