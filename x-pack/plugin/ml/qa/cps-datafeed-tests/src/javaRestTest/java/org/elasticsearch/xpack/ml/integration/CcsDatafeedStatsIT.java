/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests that verify cross-cluster search (CCS) stats are correctly
 * populated and exposed in the datafeed stats API when a datafeed targets
 * indices on a remote cluster via CCS.
 *
 * <h2>Live scope detection</h2>
 *
 * The stashed security headers in {@code ClientHelper.executeWithHeaders} contain only
 * authentication identity — they do not encode which remote clusters exist. The {@code *:}
 * CCS wildcard is resolved at search time by {@code TransportSearchAction} using the live
 * {@code RemoteClusterService} state. This means a running datafeed using a wildcard pattern
 * will automatically pick up clusters added or removed via {@code _cluster/settings} on its
 * next search cycle, without needing a restart.
 *
 * <p>Changing the datafeed's own index pattern (e.g. narrowing from two explicit clusters to
 * one) requires a stop/update/restart cycle because the datafeed config is immutable while
 * running.
 */
public class CcsDatafeedStatsIT extends ESRestTestCase {

    private static final String USER = "test_admin";
    private static final String PASS = "x-pack-test-password";

    public static ElasticsearchCluster remoteClusterA = ElasticsearchCluster.local()
        .name("remote_cluster_a")
        .distribution(DistributionType.DEFAULT)
        .module("x-pack-stack")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .user(USER, PASS)
        .build();

    public static ElasticsearchCluster remoteClusterB = ElasticsearchCluster.local()
        .name("remote_cluster_b")
        .distribution(DistributionType.DEFAULT)
        .module("x-pack-stack")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .user(USER, PASS)
        .build();

    /**
     * Only project_a is statically configured. project_b is added dynamically
     * via the cluster settings API when multi-cluster tests need it.
     */
    public static ElasticsearchCluster localCluster = ElasticsearchCluster.local()
        .name("local_cluster")
        .distribution(DistributionType.DEFAULT)
        .module("x-pack-stack")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .user(USER, PASS)
        .setting("cluster.remote.project_a.seeds", () -> "\"" + remoteClusterA.getTransportEndpoint(0) + "\"")
        .setting("cluster.remote.connections_per_cluster", "1")
        .setting("cluster.remote.project_a.skip_unavailable", "false")
        .setting("xpack.ml.ccs_stabilization_cycles", "1")
        .setting("xpack.ml.ccs_stabilization_floor", "0s")
        .build();

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteClusterA).around(remoteClusterB).around(localCluster);

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private RestClient remoteClientA() throws IOException {
        var hosts = parseClusterHosts(remoteClusterA.getHttpAddresses());
        return buildClient(restClientSettings(), hosts.toArray(new HttpHost[0]));
    }

    private RestClient remoteClientB() throws IOException {
        var hosts = parseClusterHosts(remoteClusterB.getHttpAddresses());
        return buildClient(restClientSettings(), hosts.toArray(new HttpHost[0]));
    }

    @After
    public void cleanUpAfterTest() throws Exception {
        adminClient().performRequest(new Request("POST", "/_features/_reset"));
    }

    /**
     * Verifies the end-to-end pipeline: a CCS datafeed searching a remote cluster
     * produces remote_cluster_stats in the datafeed stats API response with the correct
     * project counts after baseline establishment, and the field disappears after stopping.
     */
    @SuppressWarnings("unchecked")
    public void testCcsDatafeedBaselineAndStats() throws Exception {
        String remoteIndex = "ccs_test_data";
        String jobId = "ccs-stats-job";
        String datafeedId = jobId + "-datafeed";

        try (RestClient remote = remoteClientA()) {
            createRemoteIndex(remote, remoteIndex);
            indexRemoteData(remote, remoteIndex);
        }

        createAnomalyDetectorWithDatafeed(jobId, datafeedId, "project_a:" + remoteIndex);
        openJob(jobId);
        startDatafeed(datafeedId);

        try {
            assertBusy(() -> {
                Map<String, Object> stats = getDatafeedStats(datafeedId);
                assertThat(stats.get("state"), equalTo("started"));
                assertThat(stats, hasKey("running_state"));

                Map<String, Object> runningState = (Map<String, Object>) stats.get("running_state");
                assertThat(runningState.get("real_time_configured"), equalTo(true));

                assertThat("remote_cluster_stats should be present for CCS datafeed after baseline", stats, hasKey("remote_cluster_stats"));

                Map<String, Object> ccsStats = (Map<String, Object>) stats.get("remote_cluster_stats");

                // The CCS search targets "project_a:<index>" so the response should include at least
                // the remote cluster ("project_a") and the local/coordinating cluster.
                int totalClusters = (int) ccsStats.get("total_clusters");
                assertThat(totalClusters, greaterThanOrEqualTo(1));

                int availableClusters = (int) ccsStats.get("available_clusters");
                assertThat(availableClusters, greaterThanOrEqualTo(1));
                assertThat("available should be <= total", availableClusters, lessThanOrEqualTo(totalClusters));

                int skippedClusters = (int) ccsStats.get("skipped_clusters");
                assertThat("no clusters should be skipped when both are up", skippedClusters, equalTo(0));

                double availabilityRatio = (double) ccsStats.get("availability_ratio");
                assertThat("all clusters available → ratio should be 1.0", availabilityRatio, closeTo(1.0, 0.001));

                List<String> stabilizedAliases = (List<String>) ccsStats.get("stabilized_cluster_aliases");
                assertThat(stabilizedAliases, notNullValue());
                assertThat("stabilized aliases should include at least one cluster", stabilizedAliases.size(), greaterThanOrEqualTo(1));

                Map<String, Object> consecutiveSkips = (Map<String, Object>) ccsStats.get("per_cluster_consecutive_skips");
                assertThat(consecutiveSkips, notNullValue());
                for (var entry : consecutiveSkips.entrySet()) {
                    assertThat("consecutive skips for " + entry.getKey() + " should be 0", (int) entry.getValue(), equalTo(0));
                }
            }, 60, TimeUnit.SECONDS);

            stopDatafeed(datafeedId);

            Map<String, Object> stoppedStats = getDatafeedStats(datafeedId);
            assertThat(stoppedStats.get("state"), equalTo("stopped"));
            assertThat("running_state should be absent after stop", stoppedStats, not(hasKey("running_state")));
            assertThat("remote_cluster_stats should be absent after stop", stoppedStats, not(hasKey("remote_cluster_stats")));

            closeJob(jobId);
        } finally {
            stopDatafeed(datafeedId);
            closeJob(jobId);
        }
    }

    /**
     * Verifies that a datafeed searching only local indices (no CCS) does NOT
     * produce remote_cluster_stats, even when the cluster has remote connections configured.
     */
    public void testLocalOnlyDatafeedHasNoCrossClusterStats() throws Exception {
        String localIndex = "local_test_data";
        String jobId = "local-only-stats-job";
        String datafeedId = jobId + "-datafeed";

        createLocalIndex(localIndex);
        indexLocalData(localIndex);

        createAnomalyDetectorWithDatafeed(jobId, datafeedId, localIndex);
        openJob(jobId);
        startDatafeed(datafeedId);

        try {
            assertBusy(() -> {
                Map<String, Object> stats = getDatafeedStats(datafeedId);
                assertThat(stats.get("state"), equalTo("started"));
                assertThat(stats, hasKey("running_state"));
                assertThat("remote_cluster_stats should be absent for local-only datafeed", stats, not(hasKey("remote_cluster_stats")));
            }, 60, TimeUnit.SECONDS);
        } finally {
            stopDatafeed(datafeedId);
            closeJob(jobId);
        }
    }

    /**
     * Verifies that when two remote clusters are pre-configured and searched via a
     * wildcard CCS pattern, the remote_cluster_stats correctly reflects both clusters
     * with detailed per-project fields including stabilized aliases and skip counts.
     */
    @SuppressWarnings("unchecked")
    public void testMultiClusterCcsStats() throws Exception {
        String sharedIndex = "multi_cluster_data";
        String jobId = "multi-cluster-stats-job";
        String datafeedId = jobId + "-datafeed";

        try (RestClient remoteA = remoteClientA()) {
            createRemoteIndex(remoteA, sharedIndex);
            indexRemoteData(remoteA, sharedIndex);
        }
        try (RestClient remoteB = remoteClientB()) {
            createRemoteIndex(remoteB, sharedIndex);
            indexRemoteData(remoteB, sharedIndex);
        }

        addRemoteCluster("project_b", remoteClusterB.getTransportEndpoint(0));
        assertBusy(() -> {
            Response infoResponse = client().performRequest(new Request("GET", "_remote/info"));
            String infoBody = entityAsString(infoResponse);
            assertThat(infoBody, containsString("project_b"));
        }, 30, TimeUnit.SECONDS);

        createAnomalyDetectorWithDatafeed(jobId, datafeedId, "*:" + sharedIndex);
        openJob(jobId);
        startDatafeed(datafeedId);

        try {
            assertBusy(() -> {
                Map<String, Object> stats = getDatafeedStats(datafeedId);
                assertThat(stats.get("state"), equalTo("started"));
                assertThat(stats, hasKey("remote_cluster_stats"));
                Map<String, Object> ccs = (Map<String, Object>) stats.get("remote_cluster_stats");

                int totalClusters = (int) ccs.get("total_clusters");
                assertThat("should include project_a and project_b", totalClusters, equalTo(2));

                int availableClusters = (int) ccs.get("available_clusters");
                assertThat(availableClusters, equalTo(2));

                assertThat((int) ccs.get("skipped_clusters"), equalTo(0));
                assertThat((double) ccs.get("availability_ratio"), closeTo(1.0, 0.001));

                List<String> stabilizedAliases = (List<String>) ccs.get("stabilized_cluster_aliases");
                assertThat("stabilized aliases should include both", stabilizedAliases.size(), greaterThanOrEqualTo(2));
                assertThat(stabilizedAliases, hasItem("project_a"));
                assertThat(stabilizedAliases, hasItem("project_b"));

                Map<String, Object> consecutiveSkips = (Map<String, Object>) ccs.get("per_cluster_consecutive_skips");
                assertThat(consecutiveSkips, notNullValue());
                assertThat((int) consecutiveSkips.getOrDefault("project_a", 0), equalTo(0));
                assertThat((int) consecutiveSkips.getOrDefault("project_b", 0), equalTo(0));
            }, 60, TimeUnit.SECONDS);

            stopDatafeed(datafeedId);
            closeJob(jobId);
            removeRemoteCluster("project_b");
        } finally {
            stopDatafeed(datafeedId);
            closeJob(jobId);
            removeRemoteCluster("project_b");
        }
    }

    /**
     * Verifies that stopping a CCS datafeed, updating its indices to target a different
     * set of remote clusters, and restarting it produces updated remote_cluster_stats
     * reflecting the new cluster configuration.
     */
    @SuppressWarnings("unchecked")
    public void testStatsUpdateAfterReconfiguration() throws Exception {
        String remoteIndex = "reconfig_data";
        String jobId = "reconfig-stats-job";
        String datafeedId = jobId + "-datafeed";

        try (RestClient remoteA = remoteClientA()) {
            createRemoteIndex(remoteA, remoteIndex);
            indexRemoteData(remoteA, remoteIndex);
        }
        try (RestClient remoteB = remoteClientB()) {
            createRemoteIndex(remoteB, remoteIndex);
            indexRemoteData(remoteB, remoteIndex);
        }

        addRemoteCluster("project_b", remoteClusterB.getTransportEndpoint(0));
        assertBusy(() -> {
            Response infoResponse = client().performRequest(new Request("GET", "_remote/info"));
            assertThat(entityAsString(infoResponse), containsString("project_b"));
        }, 30, TimeUnit.SECONDS);

        // Phase 1: Start with project_a only
        createAnomalyDetectorWithDatafeed(jobId, datafeedId, "project_a:" + remoteIndex);
        openJob(jobId);
        startDatafeed(datafeedId);

        try {
            assertBusy(() -> {
                Map<String, Object> stats = getDatafeedStats(datafeedId);
                assertThat(stats.get("state"), equalTo("started"));
                assertThat(stats, hasKey("remote_cluster_stats"));
                Map<String, Object> ccs = (Map<String, Object>) stats.get("remote_cluster_stats");
                List<String> aliases = (List<String>) ccs.get("stabilized_cluster_aliases");
                assertThat(aliases, hasItem("project_a"));
                assertThat(aliases, not(hasItem("project_b")));
            }, 60, TimeUnit.SECONDS);

            stopDatafeed(datafeedId);
        } finally {
            stopDatafeed(datafeedId);
        }

        // Phase 2: Update indices to include project_b, restart.
        // Index fresh data so the restarted datafeed's lookback finds new records and
        // triggers a CCS search, which is required to establish the CrossClusterSearchStats baseline.
        try (RestClient remoteA = remoteClientA()) {
            indexRemoteData(remoteA, remoteIndex);
        }
        try (RestClient remoteB = remoteClientB()) {
            indexRemoteData(remoteB, remoteIndex);
        }
        updateDatafeedIndices(datafeedId, "project_a:" + remoteIndex + ",project_b:" + remoteIndex);
        startDatafeed(datafeedId);

        try {
            assertBusy(() -> {
                Map<String, Object> stats = getDatafeedStats(datafeedId);
                assertThat(stats.get("state"), equalTo("started"));
                assertThat(stats, hasKey("remote_cluster_stats"));
                Map<String, Object> ccs = (Map<String, Object>) stats.get("remote_cluster_stats");
                int total = (int) ccs.get("total_clusters");
                assertThat("should now include both clusters", total, greaterThanOrEqualTo(2));
                List<String> aliases = (List<String>) ccs.get("stabilized_cluster_aliases");
                assertThat(aliases, hasItem("project_a"));
                assertThat(aliases, hasItem("project_b"));
            }, 60, TimeUnit.SECONDS);

            stopDatafeed(datafeedId);
            closeJob(jobId);
            removeRemoteCluster("project_b");
        } finally {
            stopDatafeed(datafeedId);
            closeJob(jobId);
            removeRemoteCluster("project_b");
        }
    }

    /**
     * Verifies that explicit CCS indices (project_a:index, project_b:index) produce
     * correct remote_cluster_stats with both clusters tracked, and that the stats
     * contain per-project consecutive skip counts of zero when both clusters are healthy.
     */
    @SuppressWarnings("unchecked")
    public void testExplicitCcsIndicesStats() throws Exception {
        String remoteIndex = "explicit_ccs_data";
        String jobId = "explicit-ccs-job";
        String datafeedId = jobId + "-datafeed";

        try (RestClient remoteA = remoteClientA()) {
            createRemoteIndex(remoteA, remoteIndex);
            indexRemoteData(remoteA, remoteIndex);
        }
        try (RestClient remoteB = remoteClientB()) {
            createRemoteIndex(remoteB, remoteIndex);
            indexRemoteData(remoteB, remoteIndex);
        }

        addRemoteCluster("project_b", remoteClusterB.getTransportEndpoint(0));
        assertBusy(() -> {
            Response infoResponse = client().performRequest(new Request("GET", "_remote/info"));
            assertThat(entityAsString(infoResponse), containsString("project_b"));
        }, 30, TimeUnit.SECONDS);

        createAnomalyDetectorWithDatafeed(jobId, datafeedId, "project_a:" + remoteIndex, "project_b:" + remoteIndex);
        openJob(jobId);
        startDatafeed(datafeedId);

        try {
            assertBusy(() -> {
                Map<String, Object> stats = getDatafeedStats(datafeedId);
                assertThat(stats.get("state"), equalTo("started"));
                assertThat(stats, hasKey("remote_cluster_stats"));
                Map<String, Object> ccs = (Map<String, Object>) stats.get("remote_cluster_stats");

                int totalClusters = (int) ccs.get("total_clusters");
                assertThat("should include both clusters", totalClusters, equalTo(2));

                assertThat((int) ccs.get("available_clusters"), equalTo(2));
                assertThat((int) ccs.get("skipped_clusters"), equalTo(0));
                assertThat((double) ccs.get("availability_ratio"), closeTo(1.0, 0.001));

                List<String> aliases = (List<String>) ccs.get("stabilized_cluster_aliases");
                assertThat(aliases, hasItem("project_a"));
                assertThat(aliases, hasItem("project_b"));

                Map<String, Object> consecutiveSkips = (Map<String, Object>) ccs.get("per_cluster_consecutive_skips");
                assertThat((int) consecutiveSkips.getOrDefault("project_a", 0), equalTo(0));
                assertThat((int) consecutiveSkips.getOrDefault("project_b", 0), equalTo(0));
            }, 60, TimeUnit.SECONDS);

            stopDatafeed(datafeedId);

            Map<String, Object> stoppedStats = getDatafeedStats(datafeedId);
            assertThat(stoppedStats.get("state"), equalTo("stopped"));
            assertThat("remote_cluster_stats should be absent after stop", stoppedStats, not(hasKey("remote_cluster_stats")));

            closeJob(jobId);
            removeRemoteCluster("project_b");
        } finally {
            stopDatafeed(datafeedId);
            closeJob(jobId);
            removeRemoteCluster("project_b");
        }
    }

    /**
     * Verifies that a running datafeed using a wildcard CCS pattern picks up a newly added
     * remote cluster live, without stopping the datafeed. The {@code *:} pattern is resolved at
     * search time by the transport layer using the live {@code RemoteClusterService} state, so
     * dynamic changes to {@code cluster.remote.*} settings are reflected on subsequent search
     * cycles.
     */
    @SuppressWarnings("unchecked")
    public void testTopologyExpansionWhileRunning() throws Exception {
        String sharedIndex = "topo_expand_data";
        String jobId = "topo-expand-job";
        String datafeedId = jobId + "-datafeed";

        try (RestClient remoteA = remoteClientA()) {
            createRemoteIndex(remoteA, sharedIndex);
            indexRemoteData(remoteA, sharedIndex);
        }
        try (RestClient remoteB = remoteClientB()) {
            createRemoteIndex(remoteB, sharedIndex);
            indexRemoteData(remoteB, sharedIndex);
        }

        // Phase 1: only project_a configured; datafeed uses *: wildcard
        createAnomalyDetectorWithDatafeed(jobId, datafeedId, "*:" + sharedIndex);
        openJob(jobId);
        startDatafeed(datafeedId);

        try {
            assertBusy(() -> {
                Map<String, Object> stats = getDatafeedStats(datafeedId);
                assertThat(stats.get("state"), equalTo("started"));
                assertThat(stats, hasKey("remote_cluster_stats"));
                Map<String, Object> ccs = (Map<String, Object>) stats.get("remote_cluster_stats");
                List<String> aliases = (List<String>) ccs.get("stabilized_cluster_aliases");
                assertThat(aliases, hasItem("project_a"));
                assertThat("project_b should not be present yet", aliases, not(hasItem("project_b")));
            }, 60, TimeUnit.SECONDS);

            // Phase 2: add project_b while the datafeed is still running
            addRemoteCluster("project_b", remoteClusterB.getTransportEndpoint(0));
            assertBusy(() -> {
                Response infoResponse = client().performRequest(new Request("GET", "_remote/info"));
                assertThat(entityAsString(infoResponse), containsString("project_b"));
            }, 30, TimeUnit.SECONDS);

            // Index fresh documents on both remotes so the datafeed's next searches fan out to
            // both sides (helps establish CrossClusterSearchStats after the remote is added).
            try (RestClient remoteA = remoteClientA()) {
                indexRemoteDataNearNow(remoteA, sharedIndex);
            }
            try (RestClient remoteB = remoteClientB()) {
                indexRemoteDataNearNow(remoteB, sharedIndex);
            }

            // Ensure wildcard CCS on the coordinating cluster lists both remotes in _clusters.details
            // before asserting datafeed stats (avoids racing the datafeed against remote wiring).
            waitForWildcardSearchRemoteDetails(sharedIndex, "project_a", "project_b");

            assertBusy(() -> {
                Map<String, Object> stats = getDatafeedStats(datafeedId);
                assertThat(stats.get("state"), equalTo("started"));
                assertThat(stats, hasKey("remote_cluster_stats"));
                Map<String, Object> ccs = (Map<String, Object>) stats.get("remote_cluster_stats");
                int total = (int) ccs.get("total_clusters");
                assertThat("*: should now resolve to both clusters", total, greaterThanOrEqualTo(2));
                List<String> aliases = (List<String>) ccs.get("stabilized_cluster_aliases");
                assertThat(aliases, hasItem("project_a"));
                assertThat(aliases, hasItem("project_b"));
            }, 120, TimeUnit.SECONDS);

            stopDatafeed(datafeedId);
            closeJob(jobId);
            removeRemoteCluster("project_b");
        } finally {
            stopDatafeed(datafeedId);
            closeJob(jobId);
            removeRemoteCluster("project_b");
        }
    }

    /**
     * Inverse of {@link #testTopologyExpansionWhileRunning()} at the cluster-admin level: with a
     * running {@code *:} datafeed, removes the dynamically added {@code project_b} remote definition.
     * The datafeed should keep running; {@code stabilized_cluster_aliases} may retain {@code project_b}
     * until CCS stops listing that alias in search responses, but
     * {@code project_a} must remain observable and the datafeed must stay {@code started}.
     */
    @SuppressWarnings("unchecked")
    public void testTopologyContractionWhileRunning() throws Exception {
        String sharedIndex = "topo_contract_live_data";
        String jobId = "topo-contract-live-job";
        String datafeedId = jobId + "-datafeed";

        try (RestClient remoteA = remoteClientA()) {
            createRemoteIndex(remoteA, sharedIndex);
            indexRemoteData(remoteA, sharedIndex);
        }
        try (RestClient remoteB = remoteClientB()) {
            createRemoteIndex(remoteB, sharedIndex);
            indexRemoteData(remoteB, sharedIndex);
        }

        addRemoteCluster("project_b", remoteClusterB.getTransportEndpoint(0));
        assertBusy(() -> {
            Response infoResponse = client().performRequest(new Request("GET", "_remote/info"));
            assertThat(entityAsString(infoResponse), containsString("project_b"));
        }, 30, TimeUnit.SECONDS);

        createAnomalyDetectorWithDatafeed(jobId, datafeedId, "*:" + sharedIndex);
        openJob(jobId);
        startDatafeed(datafeedId);

        try {
            assertBusy(() -> {
                Map<String, Object> stats = getDatafeedStats(datafeedId);
                assertThat(stats.get("state"), equalTo("started"));
                assertThat(stats, hasKey("remote_cluster_stats"));
                Map<String, Object> ccs = (Map<String, Object>) stats.get("remote_cluster_stats");
                List<String> aliases = (List<String>) ccs.get("stabilized_cluster_aliases");
                assertThat(aliases, hasItem("project_a"));
                assertThat(aliases, hasItem("project_b"));
            }, 60, TimeUnit.SECONDS);

            removeRemoteCluster("project_b");
            assertBusy(() -> {
                Response infoResponse = client().performRequest(new Request("GET", "_remote/info"));
                assertThat(entityAsString(infoResponse), not(containsString("project_b")));
            }, 30, TimeUnit.SECONDS);

            assertBusy(() -> {
                Map<String, Object> stats = getDatafeedStats(datafeedId);
                assertThat(stats.get("state"), equalTo("started"));
                assertThat(stats, hasKey("remote_cluster_stats"));
                Map<String, Object> ccs = (Map<String, Object>) stats.get("remote_cluster_stats");
                List<String> aliases = (List<String>) ccs.get("stabilized_cluster_aliases");
                assertThat(aliases, hasItem("project_a"));
            }, 60, TimeUnit.SECONDS);

            stopDatafeed(datafeedId);
            closeJob(jobId);
        } finally {
            stopDatafeed(datafeedId);
            closeJob(jobId);
            removeRemoteCluster("project_b");
        }
    }

    /**
     * Tests scope contraction via datafeed reconfiguration: the datafeed's index pattern is
     * narrowed from two explicit clusters to one. This requires a stop/update/restart cycle
     * because the datafeed config is immutable while running. After restart, the
     * remote_cluster_stats baseline is re-established with the reduced set.
     */
    @SuppressWarnings("unchecked")
    public void testTopologyContractionAcrossRestart() throws Exception {
        String sharedIndex = "topo_contract_data";
        String jobId = "topo-contract-job";
        String datafeedId = jobId + "-datafeed";

        try (RestClient remoteA = remoteClientA()) {
            createRemoteIndex(remoteA, sharedIndex);
            indexRemoteData(remoteA, sharedIndex);
        }
        try (RestClient remoteB = remoteClientB()) {
            createRemoteIndex(remoteB, sharedIndex);
            indexRemoteData(remoteB, sharedIndex);
        }

        addRemoteCluster("project_b", remoteClusterB.getTransportEndpoint(0));
        assertBusy(() -> {
            Response infoResponse = client().performRequest(new Request("GET", "_remote/info"));
            assertThat(entityAsString(infoResponse), containsString("project_b"));
        }, 30, TimeUnit.SECONDS);

        // Phase 1: datafeed targets both clusters
        createAnomalyDetectorWithDatafeed(jobId, datafeedId, "project_a:" + sharedIndex, "project_b:" + sharedIndex);
        openJob(jobId);
        startDatafeed(datafeedId);

        try {
            assertBusy(() -> {
                Map<String, Object> stats = getDatafeedStats(datafeedId);
                assertThat(stats.get("state"), equalTo("started"));
                assertThat(stats, hasKey("remote_cluster_stats"));
                Map<String, Object> ccs = (Map<String, Object>) stats.get("remote_cluster_stats");
                List<String> aliases = (List<String>) ccs.get("stabilized_cluster_aliases");
                assertThat(aliases, hasItem("project_a"));
                assertThat(aliases, hasItem("project_b"));
            }, 60, TimeUnit.SECONDS);

            stopDatafeed(datafeedId);
        } finally {
            stopDatafeed(datafeedId);
        }

        // Phase 2: update the datafeed to target project_a only (project_b cluster stays
        // configured but the datafeed no longer references it). This mirrors the real-world
        // operation of updating a datafeed's indices rather than delete+recreate.
        // Index fresh data so the restarted datafeed's lookback finds new records and
        // triggers a CCS search, which is required to establish the CrossClusterSearchStats baseline.
        try (RestClient remoteA = remoteClientA()) {
            indexRemoteData(remoteA, sharedIndex);
        }
        updateDatafeedIndices(datafeedId, "project_a:" + sharedIndex);
        startDatafeed(datafeedId);

        try {
            assertBusy(() -> {
                Map<String, Object> stats = getDatafeedStats(datafeedId);
                assertThat(stats.get("state"), equalTo("started"));
                assertThat(stats, hasKey("remote_cluster_stats"));
                Map<String, Object> ccs = (Map<String, Object>) stats.get("remote_cluster_stats");
                int total = (int) ccs.get("total_clusters");
                assertThat("should now track only one cluster", total, equalTo(1));
                List<String> aliases = (List<String>) ccs.get("stabilized_cluster_aliases");
                assertThat(aliases, hasItem("project_a"));
                assertThat("project_b should no longer appear", aliases, not(hasItem("project_b")));
            }, 60, TimeUnit.SECONDS);

            stopDatafeed(datafeedId);
            closeJob(jobId);
            removeRemoteCluster("project_b");
        } finally {
            stopDatafeed(datafeedId);
            closeJob(jobId);
            removeRemoteCluster("project_b");
        }
    }

    @SuppressWarnings("unchecked")
    private void waitForWildcardSearchRemoteDetails(String index, String... remotes) throws Exception {
        assertBusy(() -> {
            Request probe = new Request("GET", "/*:" + index + "/_search");
            probe.addParameter("size", "0");
            Map<String, Object> body = parseResponseBody(client().performRequest(probe));
            assertThat(body, hasKey("_clusters"));
            Map<String, Object> clusters = (Map<String, Object>) body.get("_clusters");
            assertThat(clusters, hasKey("details"));
            Map<String, Object> details = (Map<String, Object>) clusters.get("details");
            for (String remote : remotes) {
                assertThat("wildcard CCS search should list remote [" + remote + "]", details, hasKey(remote));
            }
        }, 120, TimeUnit.SECONDS);
    }

    private void addRemoteCluster(String alias, String transportEndpoint) throws IOException {
        Request request = new Request("PUT", "_cluster/settings");
        request.setJsonEntity(String.format(java.util.Locale.ROOT, """
            {
              "persistent": {
                "cluster.remote.%s.seeds": ["%s"],
                "cluster.remote.%s.skip_unavailable": false
              }
            }""", alias, transportEndpoint, alias));
        client().performRequest(request);
    }

    private void removeRemoteCluster(String alias) throws IOException {
        Request request = new Request("PUT", "_cluster/settings");
        request.setJsonEntity(String.format(java.util.Locale.ROOT, """
            {
              "persistent": {
                "cluster.remote.%s.seeds": null,
                "cluster.remote.%s.mode": null,
                "cluster.remote.%s.skip_unavailable": null
              }
            }""", alias, alias, alias));
        client().performRequest(request);
    }

    private void updateDatafeedIndices(String datafeedId, String commaDelimitedIndices) throws IOException {
        String[] indices = commaDelimitedIndices.split(",");
        StringBuilder indicesJson = new StringBuilder("[");
        for (int i = 0; i < indices.length; i++) {
            if (i > 0) indicesJson.append(",");
            indicesJson.append("\"").append(indices[i].trim()).append("\"");
        }
        indicesJson.append("]");

        Request request = new Request("POST", "_ml/datafeeds/" + datafeedId + "/_update");
        request.setJsonEntity(String.format(java.util.Locale.ROOT, """
            {
              "indices": %s
            }""", indicesJson));
        client().performRequest(request);
    }

    private void createRemoteIndex(RestClient remote, String index) throws IOException {
        Request request = new Request("PUT", index);
        request.setJsonEntity("""
            {
              "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
              "mappings": {
                "properties": {
                  "timestamp": { "type": "date" },
                  "value": { "type": "double" }
                }
              }
            }""");
        remote.performRequest(request);
    }

    private void indexRemoteData(RestClient remote, String index) throws IOException {
        long now = System.currentTimeMillis();
        long oneHourAgo = now - 3600_000;

        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 50; i++) {
            long ts = oneHourAgo + (i * 60_000);
            bulk.append("{\"index\":{\"_index\":\"").append(index).append("\"}}\n");
            bulk.append("{\"timestamp\":").append(ts).append(",\"value\":").append(randomDoubleBetween(1.0, 100.0, true)).append("}\n");
        }

        Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        Response response = remote.performRequest(bulkRequest);
        assertThat(entityAsString(response), not(containsString("\"errors\":true")));
    }

    /**
     * Indexes documents with timestamps in the last few minutes so a running realtime datafeed
     * (which advances {@code lastEndTimeMs}) still matches them on both remotes.
     */
    private void indexRemoteDataNearNow(RestClient remote, String index) throws IOException {
        long now = System.currentTimeMillis();
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 50; i++) {
            long ts = now - (50 - i) * 2000L;
            bulk.append("{\"index\":{\"_index\":\"").append(index).append("\"}}\n");
            bulk.append("{\"timestamp\":").append(ts).append(",\"value\":").append(randomDoubleBetween(1.0, 100.0, true)).append("}\n");
        }

        Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        Response response = remote.performRequest(bulkRequest);
        assertThat(entityAsString(response), not(containsString("\"errors\":true")));
    }

    private void createLocalIndex(String index) throws IOException {
        Request request = new Request("PUT", index);
        request.setJsonEntity("""
            {
              "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
              "mappings": {
                "properties": {
                  "timestamp": { "type": "date" },
                  "value": { "type": "double" }
                }
              }
            }""");
        client().performRequest(request);
    }

    private void indexLocalData(String index) throws IOException {
        long now = System.currentTimeMillis();
        long oneHourAgo = now - 3600_000;

        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 50; i++) {
            long ts = oneHourAgo + (i * 60_000);
            bulk.append("{\"index\":{\"_index\":\"").append(index).append("\"}}\n");
            bulk.append("{\"timestamp\":").append(ts).append(",\"value\":").append(randomDoubleBetween(1.0, 100.0, true)).append("}\n");
        }

        Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        Response response = client().performRequest(bulkRequest);
        assertThat(entityAsString(response), not(containsString("\"errors\":true")));
    }

    private void createAnomalyDetectorWithDatafeed(String jobId, String datafeedId, String... indices) throws IOException {
        StringBuilder indicesJson = new StringBuilder("[");
        for (int i = 0; i < indices.length; i++) {
            if (i > 0) indicesJson.append(",");
            indicesJson.append("\"").append(indices[i]).append("\"");
        }
        indicesJson.append("]");

        Request request = new Request("PUT", "_ml/anomaly_detectors/" + jobId);
        request.setJsonEntity(String.format(java.util.Locale.ROOT, """
            {
              "analysis_config": {
                "bucket_span": "5m",
                "detectors": [{ "function": "mean", "field_name": "value" }]
              },
              "data_description": { "time_field": "timestamp", "time_format": "epoch_ms" },
              "analysis_limits": { "model_memory_limit": "11mb" },
              "datafeed_config": {
                "datafeed_id": "%s",
                "indices": %s,
                "frequency": "10s"
              }
            }""", datafeedId, indicesJson));
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
    }

    private void openJob(String jobId) throws IOException {
        client().performRequest(new Request("POST", "_ml/anomaly_detectors/" + jobId + "/_open"));
    }

    private void startDatafeed(String datafeedId) throws IOException {
        Request request = new Request("POST", "_ml/datafeeds/" + datafeedId + "/_start");
        Response response = client().performRequest(request);
        assertThat(entityAsString(response), containsString("\"started\":true"));
    }

    private void stopDatafeed(String datafeedId) throws Exception {
        client().performRequest(new Request("POST", "_ml/datafeeds/" + datafeedId + "/_stop"));
        assertBusy(() -> {
            Map<String, Object> stats = getDatafeedStats(datafeedId);
            assertThat(stats.get("state"), equalTo("stopped"));
        }, 30, TimeUnit.SECONDS);
    }

    private void closeJob(String jobId) throws IOException {
        client().performRequest(new Request("POST", "_ml/anomaly_detectors/" + jobId + "/_close"));
    }

    private void deleteDatafeed(String datafeedId) throws IOException {
        client().performRequest(new Request("DELETE", "_ml/datafeeds/" + datafeedId));
    }

    private void deleteJob(String jobId) throws IOException {
        client().performRequest(new Request("DELETE", "_ml/anomaly_detectors/" + jobId));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getDatafeedStats(String datafeedId) throws IOException {
        Response response = client().performRequest(new Request("GET", "_ml/datafeeds/" + datafeedId + "/_stats"));
        Map<String, Object> body = parseResponseBody(response);
        var datafeeds = (java.util.List<Map<String, Object>>) body.get("datafeeds");
        assertThat(datafeeds, notNullValue());
        assertThat(datafeeds.size(), equalTo(1));
        return datafeeds.get(0);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseResponseBody(Response response) throws IOException {
        try (InputStream is = response.getEntity().getContent()) {
            XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, is);
            return parser.map();
        }
    }

    private String entityAsString(Response response) throws IOException {
        return org.apache.http.util.EntityUtils.toString(response.getEntity());
    }
}
