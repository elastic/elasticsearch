/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class CrossClusterUsageTelemetryNoLicenseIT extends AbstractCrossClusterUsageTelemetryIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPluginWithNonEnterpriseOrExpiredLicense.class);
        plugins.add(CrossClusterQueryIT.InternalExchangePlugin.class);
        return plugins;
    }

    public void testLicenseFailure() throws Exception {
        setupClusters();
        var telemetry = getTelemetryFromFailedQuery("from logs-*,c*:logs-* | stats sum (v)");

        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(0L));
        assertThat(telemetry.getTook().count(), equalTo(0L));
        assertThat(telemetry.getRemotesPerSearchAvg(), equalTo(2.0));
        assertThat(telemetry.getRemotesPerSearchMax(), equalTo(2L));
        Map<String, Long> expectedFailure = Map.of(CCSUsageTelemetry.Result.LICENSE.getName(), 1L);
        assertThat(telemetry.getFailureReasons(), equalTo(expectedFailure));
    }
}
