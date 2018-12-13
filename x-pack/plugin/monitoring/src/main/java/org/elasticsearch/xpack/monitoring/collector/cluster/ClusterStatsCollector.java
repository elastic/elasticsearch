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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.action.XPackUsageRequestBuilder;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.Collector;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.core.XPackSettings.SECURITY_ENABLED;
import static org.elasticsearch.xpack.core.XPackSettings.TRANSPORT_SSL_ENABLED;

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

    /**
     * Timeout value when collecting the cluster stats information (default to 10s)
     */
    public static final Setting<TimeValue> CLUSTER_STATS_TIMEOUT = collectionTimeoutSetting("cluster.stats.timeout");

    private final Settings settings;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final LicenseService licenseService;
    private final Client client;

    public ClusterStatsCollector(final Settings settings,
                                 final ClusterService clusterService,
                                 final XPackLicenseState licenseState,
                                 final Client client,
                                 final LicenseService licenseService) {
        this(settings, clusterService, licenseState, client, licenseService, new IndexNameExpressionResolver());
    }

    ClusterStatsCollector(final Settings settings,
                          final ClusterService clusterService,
                          final XPackLicenseState licenseState,
                          final Client client,
                          final LicenseService licenseService,
                          final IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ClusterStatsMonitoringDoc.TYPE, clusterService, CLUSTER_STATS_TIMEOUT, licenseState);
        this.settings = settings;
        this.client = client;
        this.licenseService = licenseService;
        this.indexNameExpressionResolver = Objects.requireNonNull(indexNameExpressionResolver);
    }

    @Override
    protected boolean shouldCollect(final boolean isElectedMaster) {
        // This collector can always collect data on the master node
        return isElectedMaster;
    }

    @Override
    protected Collection<MonitoringDoc> doCollect(final MonitoringDoc.Node node,
                                                  final long interval,
                                                  final ClusterState clusterState) throws Exception {
        final Supplier<ClusterStatsResponse> clusterStatsSupplier =
                () -> client.admin().cluster().prepareClusterStats().get(getCollectionTimeout());
        final Supplier<List<XPackFeatureSet.Usage>> usageSupplier =
                () -> new XPackUsageRequestBuilder(client).get().getUsages();

        final ClusterStatsResponse clusterStats = clusterStatsSupplier.get();

        final String clusterName = clusterService.getClusterName().value();
        final String clusterUuid = clusterUuid(clusterState);
        final String version = Version.CURRENT.toString();
        final License license = licenseService.getLicense();
        final List<XPackFeatureSet.Usage> xpackUsage = collect(usageSupplier);
        final boolean apmIndicesExist = doAPMIndicesExist(clusterState);
        // if they have any other type of license, then they are either okay or already know
        final boolean clusterNeedsTLSEnabled = license.operationMode() == License.OperationMode.TRIAL &&
                                               settings.hasValue(SECURITY_ENABLED.getKey()) &&
                                               SECURITY_ENABLED.get(settings) &&
                                               TRANSPORT_SSL_ENABLED.get(settings) == false;

        // Adds a cluster stats document
        return Collections.singleton(
                new ClusterStatsMonitoringDoc(clusterUuid, timestamp(), interval, node, clusterName, version,  clusterStats.getStatus(),
                                              license, apmIndicesExist, xpackUsage, clusterStats, clusterState,
                                              clusterNeedsTLSEnabled));
    }

    boolean doAPMIndicesExist(final ClusterState clusterState) {
        try {
            final Index[] indices =
                indexNameExpressionResolver.concreteIndices(clusterState, IndicesOptions.lenientExpandOpen(), "apm-*");

            return indices.length > 0;
        } catch (IndexNotFoundException | IllegalArgumentException e) {
            return false;
        }
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
