/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.monitoring.collector.ccr;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackClient;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.ccr.client.CcrClient;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.Collector;

import java.util.Collection;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.MONITORING_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.stashWithOrigin;
import static org.elasticsearch.xpack.monitoring.collector.ccr.CcrStatsMonitoringDoc.TYPE;

public class CcrStatsCollector extends Collector {

    public static final Setting<TimeValue> CCR_STATS_TIMEOUT = collectionTimeoutSetting("ccr.stats.timeout");

    private final ThreadContext threadContext;
    private final CcrClient ccrClient;

    public CcrStatsCollector(
            final Settings settings,
            final ClusterService clusterService,
            final XPackLicenseState licenseState,
            final Client client) {
        this(settings, clusterService, licenseState, new XPackClient(client).ccr(), client.threadPool().getThreadContext());
    }

    CcrStatsCollector(
            final Settings settings,
            final ClusterService clusterService,
            final XPackLicenseState licenseState,
            final CcrClient ccrClient,
            final ThreadContext threadContext) {
        super(settings, TYPE, clusterService, CCR_STATS_TIMEOUT, licenseState);
        this.ccrClient = ccrClient;
        this.threadContext = threadContext;
    }

    @Override
    protected boolean shouldCollect(final boolean isElectedMaster) {
        // this can only run when monitoring is allowed and CCR is enabled and allowed, but also only on the elected master node
        return isElectedMaster
                && super.shouldCollect(isElectedMaster)
                && XPackSettings.CCR_ENABLED_SETTING.get(settings)
                && licenseState.isCcrAllowed();
    }


    @Override
    protected Collection<MonitoringDoc> doCollect(
            final MonitoringDoc.Node node,
            final long interval,
            final ClusterState clusterState) throws Exception {
        try (ThreadContext.StoredContext ignore = stashWithOrigin(threadContext, MONITORING_ORIGIN)) {
            final CcrStatsAction.StatsRequest request = new CcrStatsAction.StatsRequest();
            request.setIndices(Strings.EMPTY_ARRAY);
            final CcrStatsAction.StatsResponses responses = ccrClient.stats(request).actionGet(getCollectionTimeout());

            final long timestamp = timestamp();
            final String clusterUuid = clusterUuid(clusterState);

            return responses
                    .getStatsResponses()
                    .stream()
                    .map(stats -> new CcrStatsMonitoringDoc(clusterUuid, timestamp, interval, node, stats.status()))
                    .collect(Collectors.toList());
        }
    }

}
