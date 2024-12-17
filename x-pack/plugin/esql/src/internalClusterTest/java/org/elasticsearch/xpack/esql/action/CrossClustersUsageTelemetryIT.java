/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry.ASYNC_FEATURE;
import static org.hamcrest.Matchers.equalTo;

public class CrossClustersUsageTelemetryIT extends AbstractCrossClustersUsageTelemetryIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(CrossClustersQueryIT.InternalExchangePlugin.class);
        return plugins;
    }

    public void testLocalRemote() throws Exception {
        setupClusters();
        var telemetry = getTelemetryFromQuery("from logs-*,c*:logs-* | stats sum (v)", "kibana");

        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(1L));
        assertThat(telemetry.getFailureReasons().size(), equalTo(0));
        assertThat(telemetry.getTook().count(), equalTo(1L));
        assertThat(telemetry.getTookMrtFalse().count(), equalTo(0L));
        assertThat(telemetry.getTookMrtTrue().count(), equalTo(0L));
        assertThat(telemetry.getRemotesPerSearchAvg(), equalTo(2.0));
        assertThat(telemetry.getRemotesPerSearchMax(), equalTo(2L));
        assertThat(telemetry.getSearchCountWithSkippedRemotes(), equalTo(0L));
        assertThat(telemetry.getClientCounts().size(), equalTo(1));
        assertThat(telemetry.getClientCounts().get("kibana"), equalTo(1L));
        assertThat(telemetry.getFeatureCounts().get(ASYNC_FEATURE), equalTo(null));

        var perCluster = telemetry.getByRemoteCluster();
        assertThat(perCluster.size(), equalTo(3));
        for (String clusterAlias : remoteClusterAlias()) {
            var clusterTelemetry = perCluster.get(clusterAlias);
            assertThat(clusterTelemetry.getCount(), equalTo(1L));
            assertThat(clusterTelemetry.getSkippedCount(), equalTo(0L));
            assertThat(clusterTelemetry.getTook().count(), equalTo(1L));
        }

    }
}
