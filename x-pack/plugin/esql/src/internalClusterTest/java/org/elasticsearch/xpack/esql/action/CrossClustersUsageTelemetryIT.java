/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.admin.cluster.stats.CCSTelemetrySnapshot;
import org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.SkipUnavailableRule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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

    public void assertPerClusterCount(CCSTelemetrySnapshot.PerClusterCCSTelemetry perCluster, long count) {
        assertThat(perCluster.getCount(), equalTo(count));
        assertThat(perCluster.getSkippedCount(), equalTo(0L));
        assertThat(perCluster.getTook().count(), equalTo(count));
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
            assertPerClusterCount(perCluster.get(clusterAlias), 1L);
        }
        assertPerClusterCount(perCluster.get(LOCAL_CLUSTER), 1L);

        telemetry = getTelemetryFromQuery("from logs-*,c*:logs-* | stats sum (v)", "kibana");
        assertThat(telemetry.getTotalCount(), equalTo(2L));
        assertThat(telemetry.getClientCounts().get("kibana"), equalTo(2L));
        perCluster = telemetry.getByRemoteCluster();
        assertThat(perCluster.size(), equalTo(3));
        for (String clusterAlias : remoteClusterAlias()) {
            assertPerClusterCount(perCluster.get(clusterAlias), 2L);
        }
        assertPerClusterCount(perCluster.get(LOCAL_CLUSTER), 2L);
    }

    public void testLocalOnly() throws Exception {
        setupClusters();
        // Should not produce any usage info since it's a local search
        var telemetry = getTelemetryFromQuery("from logs-* | stats sum (v)", "kibana");

        assertThat(telemetry.getTotalCount(), equalTo(0L));
        assertThat(telemetry.getSuccessCount(), equalTo(0L));
        assertThat(telemetry.getByRemoteCluster().size(), equalTo(0));
    }

    @SkipUnavailableRule.NotSkipped(aliases = REMOTE1)
    public void testFailed() throws Exception {
        setupClusters();
        // Should not produce any usage info since it's a local search
        var telemetry = getTelemetryFromFailedQuery("from no_such_index | stats sum (v)");

        assertThat(telemetry.getTotalCount(), equalTo(0L));
        assertThat(telemetry.getSuccessCount(), equalTo(0L));
        assertThat(telemetry.getByRemoteCluster().size(), equalTo(0));

        // Errors from both remotes
        telemetry = getTelemetryFromFailedQuery("from logs-*,c*:no_such_index | stats sum (v)");

        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(0L));
        assertThat(telemetry.getByRemoteCluster().size(), equalTo(0));
        assertThat(telemetry.getRemotesPerSearchAvg(), equalTo(2.0));
        assertThat(telemetry.getRemotesPerSearchMax(), equalTo(2L));
        assertThat(telemetry.getSearchCountWithSkippedRemotes(), equalTo(0L));
        Map<String, Long> expectedFailure = Map.of(CCSUsageTelemetry.Result.NOT_FOUND.getName(), 1L);
        assertThat(telemetry.getFailureReasons(), equalTo(expectedFailure));

        // this is only for cluster-a so no skipped remotes
        telemetry = getTelemetryFromFailedQuery("from logs-*,cluster-a:no_such_index | stats sum (v)");
        assertThat(telemetry.getTotalCount(), equalTo(2L));
        assertThat(telemetry.getSuccessCount(), equalTo(0L));
        assertThat(telemetry.getByRemoteCluster().size(), equalTo(0));
        assertThat(telemetry.getRemotesPerSearchAvg(), equalTo(2.0));
        assertThat(telemetry.getRemotesPerSearchMax(), equalTo(2L));
        assertThat(telemetry.getSearchCountWithSkippedRemotes(), equalTo(0L));
        expectedFailure = Map.of(CCSUsageTelemetry.Result.NOT_FOUND.getName(), 2L);
        assertThat(telemetry.getFailureReasons(), equalTo(expectedFailure));
        assertThat(telemetry.getByRemoteCluster().size(), equalTo(0));
    }

    // TODO: enable when skip-un patch is merged
    // public void testSkipAllRemotes() throws Exception {
    // var telemetry = getTelemetryFromQuery("from logs-*,c*:no_such_index | stats sum (v)", "unknown");
    //
    // assertThat(telemetry.getTotalCount(), equalTo(1L));
    // assertThat(telemetry.getSuccessCount(), equalTo(1L));
    // assertThat(telemetry.getFailureReasons().size(), equalTo(0));
    // assertThat(telemetry.getTook().count(), equalTo(1L));
    // assertThat(telemetry.getTookMrtFalse().count(), equalTo(0L));
    // assertThat(telemetry.getTookMrtTrue().count(), equalTo(0L));
    // assertThat(telemetry.getRemotesPerSearchAvg(), equalTo(2.0));
    // assertThat(telemetry.getRemotesPerSearchMax(), equalTo(2L));
    // assertThat(telemetry.getSearchCountWithSkippedRemotes(), equalTo(1L));
    // assertThat(telemetry.getClientCounts().size(), equalTo(0));
    //
    // var perCluster = telemetry.getByRemoteCluster();
    // assertThat(perCluster.size(), equalTo(3));
    // for (String clusterAlias : remoteClusterAlias()) {
    // var clusterData = perCluster.get(clusterAlias);
    // assertThat(clusterData.getCount(), equalTo(0L));
    // assertThat(clusterData.getSkippedCount(), equalTo(1L));
    // assertThat(clusterData.getTook().count(), equalTo(0L));
    // }
    // assertPerClusterCount(perCluster.get(LOCAL_CLUSTER), 1L);
    // }

    public void testRemoteOnly() throws Exception {
        setupClusters();
        var telemetry = getTelemetryFromQuery("from c*:logs-* | stats sum (v)", "kibana");

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
        assertThat(perCluster.size(), equalTo(2));
        for (String clusterAlias : remoteClusterAlias()) {
            assertPerClusterCount(perCluster.get(clusterAlias), 1L);
        }
        assertThat(telemetry.getByRemoteCluster().size(), equalTo(2));
    }

    public void testAsync() throws Exception {
        setupClusters();
        var telemetry = getTelemetryFromAsyncQuery("from logs-*,c*:logs-* | stats sum (v)");

        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(1L));
        assertThat(telemetry.getFailureReasons().size(), equalTo(0));
        assertThat(telemetry.getTook().count(), equalTo(1L));
        assertThat(telemetry.getTookMrtFalse().count(), equalTo(0L));
        assertThat(telemetry.getTookMrtTrue().count(), equalTo(0L));
        assertThat(telemetry.getRemotesPerSearchAvg(), equalTo(2.0));
        assertThat(telemetry.getRemotesPerSearchMax(), equalTo(2L));
        assertThat(telemetry.getSearchCountWithSkippedRemotes(), equalTo(0L));
        assertThat(telemetry.getClientCounts().size(), equalTo(0));
        assertThat(telemetry.getFeatureCounts().get(ASYNC_FEATURE), equalTo(1L));

        var perCluster = telemetry.getByRemoteCluster();
        assertThat(perCluster.size(), equalTo(3));
        for (String clusterAlias : remoteClusterAlias()) {
            assertPerClusterCount(perCluster.get(clusterAlias), 1L);
        }
        assertPerClusterCount(perCluster.get(LOCAL_CLUSTER), 1L);

        // do it again
        telemetry = getTelemetryFromAsyncQuery("from logs-*,c*:logs-* |  stats sum (v)");
        assertThat(telemetry.getTotalCount(), equalTo(2L));
        assertThat(telemetry.getFeatureCounts().get(ASYNC_FEATURE), equalTo(2L));
        perCluster = telemetry.getByRemoteCluster();
        assertThat(perCluster.size(), equalTo(3));
        for (String clusterAlias : remoteClusterAlias()) {
            assertPerClusterCount(perCluster.get(clusterAlias), 2L);
        }
        assertPerClusterCount(perCluster.get(LOCAL_CLUSTER), 2L);
    }

    public void testNoSuchCluster() throws Exception {
        setupClusters();
        // This is not recognized as a cross-cluster search
        var telemetry = getTelemetryFromFailedQuery("from c*:logs*, nocluster:nomatch | stats sum (v)");

        assertThat(telemetry.getTotalCount(), equalTo(0L));
        assertThat(telemetry.getSuccessCount(), equalTo(0L));
        assertThat(telemetry.getByRemoteCluster().size(), equalTo(0));
    }

    @SkipUnavailableRule.NotSkipped(aliases = REMOTE1)
    public void testDisconnect() throws Exception {
        setupClusters();
        // Disconnect remote1
        cluster(REMOTE1).close();
        var telemetry = getTelemetryFromFailedQuery("from logs-*,cluster-a:logs-* | stats sum (v)");

        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(0L));
        Map<String, Long> expectedFailure = Map.of(CCSUsageTelemetry.Result.REMOTES_UNAVAILABLE.getName(), 1L);
        assertThat(telemetry.getFailureReasons(), equalTo(expectedFailure));
    }

}
