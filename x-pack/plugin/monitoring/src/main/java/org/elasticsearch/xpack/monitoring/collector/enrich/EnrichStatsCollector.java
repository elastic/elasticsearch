/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.enrich;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.Collector;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.MONITORING_ORIGIN;

public final class EnrichStatsCollector extends Collector {

    public static final Setting<TimeValue> STATS_TIMEOUT = collectionTimeoutSetting("enrich.stats.timeout");

    private final Client client;
    private final ThreadContext threadContext;

    public EnrichStatsCollector(ClusterService clusterService,
                                XPackLicenseState licenseState,
                                Client client) {
        this(clusterService, licenseState, client, client.threadPool().getThreadContext());
    }

    EnrichStatsCollector(ClusterService clusterService,
                         XPackLicenseState licenseState,
                         Client client,
                         ThreadContext threadContext) {
        super(EnrichCoordinatorDoc.TYPE, clusterService, STATS_TIMEOUT, licenseState);
        this.client = client;
        this.threadContext = threadContext;
    }

    @Override
    protected boolean shouldCollect(final boolean isElectedMaster) {
        return isElectedMaster
            && super.shouldCollect(isElectedMaster)
            && licenseState.isAllowed(XPackLicenseState.Feature.ENRICH);
    }

    @Override
    protected Collection<MonitoringDoc> doCollect(MonitoringDoc.Node node, long interval, ClusterState clusterState) throws Exception {
        try (ThreadContext.StoredContext ignore = threadContext.stashWithOrigin(MONITORING_ORIGIN)) {
            final long timestamp = timestamp();
            final String clusterUuid = clusterUuid(clusterState);

            final EnrichStatsAction.Request request = new EnrichStatsAction.Request();
            final EnrichStatsAction.Response response =
                client.execute(EnrichStatsAction.INSTANCE, request).actionGet(getCollectionTimeout());

            final List<MonitoringDoc> docs = response.getCoordinatorStats().stream()
                .map(stats -> new EnrichCoordinatorDoc(clusterUuid, timestamp, interval, node, stats))
                .collect(Collectors.toList());

            response.getExecutingPolicies().stream()
                .map(stats -> new ExecutingPolicyDoc(clusterUuid, timestamp, interval, node, stats))
                .forEach(docs::add);

            return docs;
        }
    }
}
