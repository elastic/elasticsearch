/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.monitoring.collector.ccr;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.Collector;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.MONITORING_ORIGIN;
import static org.elasticsearch.xpack.monitoring.collector.ccr.FollowStatsMonitoringDoc.TYPE;

public final class StatsCollector extends Collector {

    public static final Setting<TimeValue> CCR_STATS_TIMEOUT = collectionTimeoutSetting("ccr.stats.timeout");

    private final Settings settings;
    private final ThreadContext threadContext;
    private final Client client;

    public StatsCollector(
            final Settings settings,
            final ClusterService clusterService,
            final XPackLicenseState licenseState,
            final Client client) {
        this(settings, clusterService, licenseState, client, client.threadPool().getThreadContext());
    }

    StatsCollector(
            final Settings settings,
            final ClusterService clusterService,
            final XPackLicenseState licenseState,
            final Client client,
            final ThreadContext threadContext) {
        super(TYPE, clusterService, CCR_STATS_TIMEOUT, licenseState);
        this.settings = settings;
        this.client = client;
        this.threadContext = threadContext;
    }

    @Override
    protected boolean shouldCollect(final boolean isElectedMaster) {
        // this can only run when monitoring is allowed and CCR is enabled and allowed, but also only on the elected master node
        return isElectedMaster
                && super.shouldCollect(isElectedMaster)
                && XPackSettings.CCR_ENABLED_SETTING.get(settings)
                && licenseState.isAllowed(XPackLicenseState.Feature.CCR);
    }


    @Override
    protected Collection<MonitoringDoc> doCollect(
            final MonitoringDoc.Node node,
            final long interval,
            final ClusterState clusterState) throws Exception {
        try (ThreadContext.StoredContext ignore = threadContext.stashWithOrigin(MONITORING_ORIGIN)) {
            final long timestamp = timestamp();
            final String clusterUuid = clusterUuid(clusterState);

            final CcrStatsAction.Request request = new CcrStatsAction.Request();
            final CcrStatsAction.Response response = client.execute(CcrStatsAction.INSTANCE, request).actionGet(getCollectionTimeout());

            final AutoFollowStatsMonitoringDoc autoFollowStatsDoc =
                new AutoFollowStatsMonitoringDoc(clusterUuid, timestamp, interval, node, response.getAutoFollowStats());

            Set<String> collectionIndices = new HashSet<>(Arrays.asList(getCollectionIndices()));
            List<MonitoringDoc> docs = response
                .getFollowStats()
                .getStatsResponses()
                .stream()
                .filter(statsResponse -> collectionIndices.isEmpty() || collectionIndices.contains(statsResponse.status().followerIndex()))
                .map(stats -> new FollowStatsMonitoringDoc(clusterUuid, timestamp, interval, node, stats.status()))
                .collect(Collectors.toList());
            docs.add(autoFollowStatsDoc);
            return docs;
        }
    }
}
