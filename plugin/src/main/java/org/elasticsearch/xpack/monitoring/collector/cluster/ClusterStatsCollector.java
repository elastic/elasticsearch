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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.action.XPackUsageRequestBuilder;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.collector.Collector;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.security.InternalClient;

import java.util.ArrayList;
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

    public static final String NAME = "cluster-stats-collector";

    private final LicenseService licenseService;
    private final Client client;

    public ClusterStatsCollector(Settings settings, ClusterService clusterService,
                                 MonitoringSettings monitoringSettings, XPackLicenseState licenseState, InternalClient client,
                                 LicenseService licenseService) {
        super(settings, NAME, clusterService, monitoringSettings, licenseState);
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
                () -> client.admin().cluster().prepareClusterStats().get(monitoringSettings.clusterStatsTimeout());
        final Supplier<List<XPackFeatureSet.Usage>> usageSupplier = () -> new XPackUsageRequestBuilder(client).get().getUsages();

        final ClusterStatsResponse clusterStats = clusterStatsSupplier.get();

        final long timestamp = System.currentTimeMillis();
        final String clusterUUID = clusterUUID();
        final DiscoveryNode sourceNode = localNode();

        final List<MonitoringDoc> results = new ArrayList<>(1);

        // Adds a cluster info document
        ClusterInfoMonitoringDoc clusterInfoDoc = new ClusterInfoMonitoringDoc(monitoringId(), monitoringVersion());
        clusterInfoDoc.setClusterUUID(clusterUUID);
        clusterInfoDoc.setTimestamp(timestamp);
        clusterInfoDoc.setSourceNode(sourceNode);
        clusterInfoDoc.setClusterName(clusterService.getClusterName().value());
        clusterInfoDoc.setVersion(Version.CURRENT.toString());
        clusterInfoDoc.setLicense(licenseService.getLicense());
        clusterInfoDoc.setClusterStats(clusterStats);
        clusterInfoDoc.setUsage(collect(usageSupplier));
        results.add(clusterInfoDoc);

        // Adds a cluster stats document
        if (super.shouldCollect()) {
            ClusterStatsMonitoringDoc clusterStatsDoc = new ClusterStatsMonitoringDoc(monitoringId(), monitoringVersion());
            clusterStatsDoc.setClusterUUID(clusterUUID);
            clusterStatsDoc.setTimestamp(timestamp);
            clusterStatsDoc.setSourceNode(sourceNode);
            clusterStatsDoc.setClusterStats(clusterStats);
            results.add(clusterStatsDoc);
        }

        return Collections.unmodifiableCollection(results);
    }

    @Nullable
    private <T> T collect(final Supplier<T> supplier) {
        try {
            return supplier.get();
        } catch (ElasticsearchSecurityException e) {
            if (LicenseUtils.isLicenseExpiredException(e)) {
                logger.trace((Supplier<?>) () -> new ParameterizedMessage(
                                "collector [{}] - unable to collect data because of expired license", name()), e);
            } else {
                throw e;
            }
        }

        return null;
    }

}
