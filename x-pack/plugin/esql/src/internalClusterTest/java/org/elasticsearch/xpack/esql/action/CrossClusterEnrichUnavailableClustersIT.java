/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.TransportXPackInfoAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureResponse;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.EnrichPlugin;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class CrossClusterEnrichUnavailableClustersIT extends AbstractMultiClustersTestCase {

    public static String REMOTE_CLUSTER_1 = "c1";
    public static String REMOTE_CLUSTER_2 = "c2";

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    protected Collection<String> allClusters() {
        return CollectionUtils.appendToCopy(remoteClusterAlias(), LOCAL_CLUSTER);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPlugin.class);
        plugins.add(LocalStateEnrich.class);
        plugins.add(IngestCommonPlugin.class);
        plugins.add(ReindexPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
    }

    static final EnrichPolicy hostPolicy = new EnrichPolicy("match", null, List.of("hosts"), "ip", List.of("ip", "os"));
    static final EnrichPolicy vendorPolicy = new EnrichPolicy("match", null, List.of("vendors"), "os", List.of("os", "vendor"));

    @Before
    public void setupHostsEnrich() {
        // the hosts policy are identical on every node
        Map<String, String> allHosts = Map.of(
            "192.168.1.2",
            "Windows",
            "192.168.1.3",
            "MacOS",
            "192.168.1.4",
            "Linux",
            "192.168.1.5",
            "Android",
            "192.168.1.6",
            "iOS",
            "192.168.1.7",
            "Windows",
            "192.168.1.8",
            "MacOS",
            "192.168.1.9",
            "Linux",
            "192.168.1.10",
            "Linux",
            "192.168.1.11",
            "Windows"
        );
        for (String cluster : allClusters()) {
            Client client = client(cluster);
            client.admin().indices().prepareCreate("hosts").setMapping("ip", "type=ip", "os", "type=keyword").get();
            for (Map.Entry<String, String> h : allHosts.entrySet()) {
                client.prepareIndex("hosts").setSource("ip", h.getKey(), "os", h.getValue()).get();
            }
            client.admin().indices().prepareRefresh("hosts").get();
            client.execute(PutEnrichPolicyAction.INSTANCE, new PutEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, "hosts", hostPolicy))
                .actionGet();
            client.execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, "hosts"))
                .actionGet();
            assertAcked(client.admin().indices().prepareDelete("hosts"));
        }
    }

    @Before
    public void setupVendorPolicy() {
        var localVendors = Map.of("Windows", "Microsoft", "MacOS", "Apple", "iOS", "Apple", "Android", "Samsung", "Linux", "Redhat");
        var c1Vendors = Map.of("Windows", "Microsoft", "MacOS", "Apple", "iOS", "Apple", "Android", "Google", "Linux", "Suse");
        var c2Vendors = Map.of("Windows", "Microsoft", "MacOS", "Apple", "iOS", "Apple", "Android", "Sony", "Linux", "Ubuntu");
        var vendors = Map.of(LOCAL_CLUSTER, localVendors, "c1", c1Vendors, "c2", c2Vendors);
        for (Map.Entry<String, Map<String, String>> e : vendors.entrySet()) {
            Client client = client(e.getKey());
            client.admin().indices().prepareCreate("vendors").setMapping("os", "type=keyword", "vendor", "type=keyword").get();
            for (Map.Entry<String, String> v : e.getValue().entrySet()) {
                client.prepareIndex("vendors").setSource("os", v.getKey(), "vendor", v.getValue()).get();
            }
            client.admin().indices().prepareRefresh("vendors").get();
            client.execute(PutEnrichPolicyAction.INSTANCE, new PutEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, "vendors", vendorPolicy))
                .actionGet();
            client.execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, "vendors"))
                .actionGet();
            assertAcked(client.admin().indices().prepareDelete("vendors"));
        }
    }

    @Before
    public void setupEventsIndices() {
        record Event(long timestamp, String user, String host) {

        }
        List<Event> e0 = List.of(
            new Event(1, "matthew", "192.168.1.3"),
            new Event(2, "simon", "192.168.1.5"),
            new Event(3, "park", "192.168.1.2"),
            new Event(4, "andrew", "192.168.1.7"),
            new Event(5, "simon", "192.168.1.20"),
            new Event(6, "kevin", "192.168.1.2"),
            new Event(7, "akio", "192.168.1.5"),
            new Event(8, "luke", "192.168.1.2"),
            new Event(9, "jack", "192.168.1.4")
        );
        List<Event> e1 = List.of(
            new Event(1, "andres", "192.168.1.2"),
            new Event(2, "sergio", "192.168.1.6"),
            new Event(3, "kylian", "192.168.1.8"),
            new Event(4, "andrew", "192.168.1.9"),
            new Event(5, "jack", "192.168.1.3"),
            new Event(6, "kevin", "192.168.1.4"),
            new Event(7, "akio", "192.168.1.7"),
            new Event(8, "kevin", "192.168.1.21"),
            new Event(9, "andres", "192.168.1.8")
        );
        List<Event> e2 = List.of(
            new Event(1, "park", "192.168.1.25"),
            new Event(2, "akio", "192.168.1.5"),
            new Event(3, "park", "192.168.1.2"),
            new Event(4, "kevin", "192.168.1.3")
        );
        for (var c : Map.of(LOCAL_CLUSTER, e0, "c1", e1, "c2", e2).entrySet()) {
            Client client = client(c.getKey());
            client.admin()
                .indices()
                .prepareCreate("events")
                .setMapping("timestamp", "type=long", "user", "type=keyword", "host", "type=ip")
                .get();
            for (var e : c.getValue()) {
                client.prepareIndex("events").setSource("timestamp", e.timestamp, "user", e.user, "host", e.host).get();
            }
            client.admin().indices().prepareRefresh("events").get();
        }
    }

    // @After
    // public void wipeEnrichPolicies() {
    // for (String cluster : allClusters()) {
    // cluster(cluster).wipe(Set.of());
    // for (String policy : List.of("hosts", "vendors")) {
    // client(cluster).execute(
    // DeleteEnrichPolicyAction.INSTANCE,
    // new DeleteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policy)
    // );
    // }
    // }
    // }

    static String enrichHosts(Enrich.Mode mode) {
        return EsqlTestUtils.randomEnrichCommand("hosts", mode, hostPolicy.getMatchField(), hostPolicy.getEnrichFields());
    }

    static String enrichVendors(Enrich.Mode mode) {
        return EsqlTestUtils.randomEnrichCommand("vendors", mode, vendorPolicy.getMatchField(), vendorPolicy.getEnrichFields());
    }

    private void setSkipUnavailable(String clusterAlias, boolean skip) {
        client(LOCAL_CLUSTER).admin()
            .cluster()
            .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .setPersistentSettings(Settings.builder().put("cluster.remote." + clusterAlias + ".skip_unavailable", skip).build())
            .get();
    }

    private void clearSkipUnavailable() {
        Settings.Builder settingsBuilder = Settings.builder()
            .putNull("cluster.remote." + REMOTE_CLUSTER_1 + ".skip_unavailable")
            .putNull("cluster.remote." + REMOTE_CLUSTER_2 + ".skip_unavailable");
        client(LOCAL_CLUSTER).admin()
            .cluster()
            .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .setPersistentSettings(settingsBuilder.build())
            .get();
    }

    public void testEnrichWithHostsPolicyAndDisconnectedRemotesWithSkipUnavailableTrue() throws IOException {
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        setSkipUnavailable(REMOTE_CLUSTER_2, true);

        try {
            // close remote-cluster-1 so that it is unavailable
            cluster(REMOTE_CLUSTER_1).close();

            Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
            Boolean requestIncludeMeta = includeCCSMetadata.v1();
            boolean responseExpectMeta = includeCCSMetadata.v2();

            {
                Enrich.Mode mode = randomFrom(Enrich.Mode.values());
                String query = "FROM *:events | eval ip= TO_STR(host) | " + enrichHosts(mode) + " | stats c = COUNT(*) by os | SORT os";
                try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                    List<List<Object>> rows = getValuesList(resp);
                    assertThat(rows.size(), greaterThanOrEqualTo(1));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertCCSExecutionInfoDetails(executionInfo);

                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2)));

                    EsqlExecutionInfo.Cluster cluster1 = executionInfo.getCluster(REMOTE_CLUSTER_1);
                    assertThat(cluster1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                    assertThat(cluster1.getTotalShards(), equalTo(0));
                    assertThat(cluster1.getSuccessfulShards(), equalTo(0));
                    assertThat(cluster1.getSkippedShards(), equalTo(0));
                    assertThat(cluster1.getFailedShards(), equalTo(0));

                    EsqlExecutionInfo.Cluster cluster2 = executionInfo.getCluster(REMOTE_CLUSTER_2);
                    assertThat(cluster2.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                    assertThat(cluster2.getTotalShards(), greaterThanOrEqualTo(0));
                    assertThat(cluster2.getSuccessfulShards(), equalTo(cluster2.getSuccessfulShards()));
                    assertThat(cluster2.getSkippedShards(), equalTo(0));
                    assertThat(cluster2.getFailedShards(), equalTo(0));
                }
            }

            // close remote-cluster-2 so that it is also unavailable
            cluster(REMOTE_CLUSTER_2).close();

            {
                Enrich.Mode mode = randomFrom(Enrich.Mode.values());
                String query = "FROM *:events | eval ip= TO_STR(host) | " + enrichHosts(mode) + " | stats c = COUNT(*) by os | SORT os";
                try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                    List<List<Object>> rows = getValuesList(resp);
                    assertThat(rows.size(), equalTo(0));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertCCSExecutionInfoDetails(executionInfo);

                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2)));

                    EsqlExecutionInfo.Cluster cluster1 = executionInfo.getCluster(REMOTE_CLUSTER_1);
                    assertThat(cluster1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                    assertThat(cluster1.getTotalShards(), equalTo(0));
                    assertThat(cluster1.getSuccessfulShards(), equalTo(0));
                    assertThat(cluster1.getSkippedShards(), equalTo(0));
                    assertThat(cluster1.getFailedShards(), equalTo(0));

                    EsqlExecutionInfo.Cluster cluster2 = executionInfo.getCluster(REMOTE_CLUSTER_2);
                    assertThat(cluster2.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                    assertThat(cluster2.getTotalShards(), equalTo(0));
                    assertThat(cluster2.getSuccessfulShards(), equalTo(0));
                    assertThat(cluster2.getSkippedShards(), equalTo(0));
                    assertThat(cluster2.getFailedShards(), equalTo(0));
                }
            }
        } finally {
            clearSkipUnavailable();
        }
    }

    public void testEnrichWithHostsPolicyAndDisconnectedRemotesWithSkipUnavailableFalse() throws IOException {
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);

        try {
            // close remote-cluster-1 so that it is unavailable
            cluster(REMOTE_CLUSTER_1).close();

            Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
            Boolean requestIncludeMeta = includeCCSMetadata.v1();
            boolean responseExpectMeta = includeCCSMetadata.v2();

            {
                Enrich.Mode mode = randomFrom(Enrich.Mode.values());
                String query = "FROM *:events | EVAL ip= TO_STR(host) | " + enrichHosts(mode) + " | STATS c = COUNT(*) by os | SORT os";
                try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                    List<List<Object>> rows = getValuesList(resp);
                    assertThat(rows.size(), greaterThanOrEqualTo(1));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertCCSExecutionInfoDetails(executionInfo);

                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2)));

                    EsqlExecutionInfo.Cluster cluster1 = executionInfo.getCluster(REMOTE_CLUSTER_1);
                    assertThat(cluster1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                    assertThat(cluster1.getTotalShards(), equalTo(0));
                    assertThat(cluster1.getSuccessfulShards(), equalTo(0));
                    assertThat(cluster1.getSkippedShards(), equalTo(0));
                    assertThat(cluster1.getFailedShards(), equalTo(0));

                    EsqlExecutionInfo.Cluster cluster2 = executionInfo.getCluster(REMOTE_CLUSTER_2);
                    assertThat(cluster2.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                    assertThat(cluster2.getTotalShards(), greaterThanOrEqualTo(0));
                    assertThat(cluster2.getSuccessfulShards(), equalTo(cluster2.getSuccessfulShards()));
                    assertThat(cluster2.getSkippedShards(), equalTo(0));
                    assertThat(cluster2.getFailedShards(), equalTo(0));
                }
            }

            // close remote-cluster-2 so that it is also unavailable
            cluster(REMOTE_CLUSTER_2).close();
            {
                Enrich.Mode mode = randomFrom(Enrich.Mode.values());
                String query = "FROM *:events | eval ip= TO_STR(host) | " + enrichHosts(mode) + " | stats c = COUNT(*) by os | SORT os";
                Exception exception = expectThrows(Exception.class, () -> runQuery(query, requestIncludeMeta));
                assertTrue(ExceptionsHelper.isRemoteUnavailableException(exception));
            }
        } finally {
            clearSkipUnavailable();
        }
    }

    public void testEnrichHostsAggThenEnrichVendorCoordinatorWithUnavailableRemotes() throws IOException {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        boolean skipUnavailableRemote1 = randomBoolean();
        setSkipUnavailable(REMOTE_CLUSTER_1, skipUnavailableRemote1);
        setSkipUnavailable(REMOTE_CLUSTER_2, true);

        try {
            // close remote-cluster-2 so that it is unavailable
            cluster(REMOTE_CLUSTER_2).close();

            for (var hostMode : Enrich.Mode.values()) {
                // note: this is a local + remote test, unlike most other tests in this class
                String query = String.format(Locale.ROOT, """
                    FROM *:events,events
                    | eval ip= TO_STR(host)
                    | %s
                    | stats c = COUNT(*) by os
                    | %s
                    | stats c = SUM(c) by vendor
                    | sort vendor
                    """, enrichHosts(hostMode), enrichVendors(Enrich.Mode.COORDINATOR));
                try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                    assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(1));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertThat(
                        executionInfo.clusterAliases(),
                        equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2))
                    );
                    assertCCSExecutionInfoDetails(executionInfo);

                    EsqlExecutionInfo.Cluster cluster1 = executionInfo.getCluster(REMOTE_CLUSTER_1);
                    assertThat(cluster1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                    assertThat(cluster1.getTotalShards(), greaterThanOrEqualTo(0));
                    assertThat(cluster1.getSuccessfulShards(), equalTo(cluster1.getSuccessfulShards()));
                    assertThat(cluster1.getSkippedShards(), equalTo(0));
                    assertThat(cluster1.getFailedShards(), equalTo(0));
                    assertThat(cluster1.getTook().millis(), greaterThanOrEqualTo(0L));

                    EsqlExecutionInfo.Cluster cluster2 = executionInfo.getCluster(REMOTE_CLUSTER_2);
                    assertThat(cluster2.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                    assertThat(cluster2.getTotalShards(), equalTo(0));
                    assertThat(cluster2.getSuccessfulShards(), equalTo(0));
                    assertThat(cluster2.getSkippedShards(), equalTo(0));
                    assertThat(cluster2.getFailedShards(), equalTo(0));
                }
            }

            // close remote-cluster-1 so that it is also unavailable
            cluster(REMOTE_CLUSTER_1).close();

            for (var hostMode : Enrich.Mode.values()) {
                // note: this is a local + remote test, unlike most other tests in this class
                String query = String.format(Locale.ROOT, """
                    FROM *:events,events
                    | eval ip= TO_STR(host)
                    | %s
                    | stats c = COUNT(*) by os
                    | %s
                    | stats c = SUM(c) by vendor
                    | sort vendor
                    """, enrichHosts(hostMode), enrichVendors(Enrich.Mode.COORDINATOR));

                if (skipUnavailableRemote1 == false) {
                    Exception exception = expectThrows(Exception.class, () -> runQuery(query, requestIncludeMeta));
                    assertTrue(ExceptionsHelper.isRemoteUnavailableException(exception));
                } else {
                    try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                        assertThat(getValuesList(resp).size(), greaterThan(0));
                        EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                        assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                        assertThat(
                            executionInfo.clusterAliases(),
                            equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2))
                        );
                        assertCCSExecutionInfoDetails(executionInfo);

                        EsqlExecutionInfo.Cluster cluster1 = executionInfo.getCluster(REMOTE_CLUSTER_1);
                        assertThat(cluster1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                        assertThat(cluster1.getTotalShards(), equalTo(0));
                        assertThat(cluster1.getSuccessfulShards(), equalTo(0));
                        assertThat(cluster1.getSkippedShards(), equalTo(0));
                        assertThat(cluster1.getFailedShards(), equalTo(0));

                        EsqlExecutionInfo.Cluster cluster2 = executionInfo.getCluster(REMOTE_CLUSTER_2);
                        assertThat(cluster2.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                        assertThat(cluster2.getTotalShards(), equalTo(0));
                        assertThat(cluster2.getSuccessfulShards(), equalTo(0));
                        assertThat(cluster2.getSkippedShards(), equalTo(0));
                        assertThat(cluster2.getFailedShards(), equalTo(0));
                    }
                }
            }

        } finally {
            clearSkipUnavailable();
        }
    }

    public void testEnrichTwiceThenAggsWithUnavailableRemotes() throws IOException {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        boolean skipUnavailableRemote1 = randomBoolean();
        setSkipUnavailable(REMOTE_CLUSTER_1, skipUnavailableRemote1);
        setSkipUnavailable(REMOTE_CLUSTER_2, true);

        try {
            // close remote-cluster-2 so that it is unavailable
            cluster(REMOTE_CLUSTER_2).close();

            for (var hostMode : Enrich.Mode.values()) {
                String query = String.format(Locale.ROOT, """
                    FROM *:events,events
                    | eval ip= TO_STR(host)
                    | %s
                    | %s
                    | stats c = COUNT(*) by vendor
                    | sort vendor
                    """, enrichHosts(hostMode), enrichVendors(Enrich.Mode.COORDINATOR));
                try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                    assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(1));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertThat(executionInfo.clusterAliases(), equalTo(Set.of("", REMOTE_CLUSTER_1, REMOTE_CLUSTER_2)));
                    assertCCSExecutionInfoDetails(executionInfo);

                    EsqlExecutionInfo.Cluster cluster1 = executionInfo.getCluster(REMOTE_CLUSTER_1);
                    assertThat(cluster1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                    assertThat(cluster1.getTotalShards(), greaterThanOrEqualTo(0));
                    assertThat(cluster1.getSuccessfulShards(), equalTo(cluster1.getSuccessfulShards()));
                    assertThat(cluster1.getSkippedShards(), equalTo(0));
                    assertThat(cluster1.getFailedShards(), equalTo(0));

                    EsqlExecutionInfo.Cluster cluster2 = executionInfo.getCluster(REMOTE_CLUSTER_2);
                    assertThat(cluster2.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                    assertThat(cluster2.getTotalShards(), equalTo(0));
                    assertThat(cluster2.getSuccessfulShards(), equalTo(0));
                    assertThat(cluster2.getSkippedShards(), equalTo(0));
                    assertThat(cluster2.getFailedShards(), equalTo(0));

                    EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                    assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                    assertThat(localCluster.getTotalShards(), greaterThan(0));
                    assertThat(localCluster.getSuccessfulShards(), equalTo(localCluster.getTotalShards()));
                    assertThat(localCluster.getSkippedShards(), equalTo(0));
                    assertThat(localCluster.getFailedShards(), equalTo(0));
                }
            }

            // close remote-cluster-1 so that it is also unavailable
            cluster(REMOTE_CLUSTER_1).close();

            for (var hostMode : Enrich.Mode.values()) {
                String query = String.format(Locale.ROOT, """
                    FROM *:events,events
                    | eval ip= TO_STR(host)
                    | %s
                    | %s
                    | stats c = COUNT(*) by vendor
                    | sort vendor
                    """, enrichHosts(hostMode), enrichVendors(Enrich.Mode.COORDINATOR));
                if (skipUnavailableRemote1 == false) {
                    Exception exception = expectThrows(Exception.class, () -> runQuery(query, requestIncludeMeta));
                    assertTrue(ExceptionsHelper.isRemoteUnavailableException(exception));
                } else {
                    try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                        assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(1));
                        System.err.println(getValuesList(resp));
                        EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                        assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                        assertThat(executionInfo.clusterAliases(), equalTo(Set.of("", REMOTE_CLUSTER_1, REMOTE_CLUSTER_2)));
                        assertCCSExecutionInfoDetails(executionInfo);

                        EsqlExecutionInfo.Cluster cluster1 = executionInfo.getCluster(REMOTE_CLUSTER_1);
                        assertThat(cluster1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                        assertThat(cluster1.getTotalShards(), equalTo(0));
                        assertThat(cluster1.getSuccessfulShards(), equalTo(0));
                        assertThat(cluster1.getSkippedShards(), equalTo(0));
                        assertThat(cluster1.getFailedShards(), equalTo(0));
                        assertThat(cluster1.getTook().millis(), greaterThanOrEqualTo(0L));

                        EsqlExecutionInfo.Cluster cluster2 = executionInfo.getCluster(REMOTE_CLUSTER_2);
                        assertThat(cluster2.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                        assertThat(cluster2.getTotalShards(), equalTo(0));
                        assertThat(cluster2.getSuccessfulShards(), equalTo(0));
                        assertThat(cluster2.getSkippedShards(), equalTo(0));
                        assertThat(cluster2.getFailedShards(), equalTo(0));

                        EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                        assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                        assertThat(localCluster.getTotalShards(), greaterThan(0));
                        assertThat(localCluster.getSuccessfulShards(), equalTo(localCluster.getTotalShards()));
                        assertThat(localCluster.getSkippedShards(), equalTo(0));
                        assertThat(localCluster.getFailedShards(), equalTo(0));
                    }
                }
            }
        } finally {
            clearSkipUnavailable();
        }
    }

    // MP TODO: add testEnrichCoordinatorThenAnyWithSingleUnavailableRemoteAndNotLocal
    public void testEnrichCoordinatorThenAnyWithSingleUnavailableRemoteAndLocal() throws IOException {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        boolean skipUnavailableRemote1 = randomBoolean();
        setSkipUnavailable(REMOTE_CLUSTER_1, skipUnavailableRemote1);

        try {
            // close remote-cluster-1 so that it is unavailable
            cluster(REMOTE_CLUSTER_1).close();
            String query = String.format(Locale.ROOT, """
                FROM %s:events,events
                | eval ip= TO_STR(host)
                | %s
                | %s
                | stats c = COUNT(*) by vendor
                | sort vendor
                """, REMOTE_CLUSTER_1, enrichHosts(Enrich.Mode.COORDINATOR), enrichVendors(Enrich.Mode.ANY));
            if (skipUnavailableRemote1 == false) {
                Exception exception = expectThrows(Exception.class, () -> runQuery(query, requestIncludeMeta));
                assertTrue(ExceptionsHelper.isRemoteUnavailableException(exception));
            } else {
                try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                    assertThat(getValuesList(resp).size(), greaterThan(0));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertThat(
                        executionInfo.clusterAliases(),
                        equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1))
                    );
                    assertCCSExecutionInfoDetails(executionInfo);

                    EsqlExecutionInfo.Cluster cluster1 = executionInfo.getCluster(REMOTE_CLUSTER_1);
                    assertThat(cluster1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                    assertThat(cluster1.getTotalShards(), equalTo(0));
                    assertThat(cluster1.getSuccessfulShards(), equalTo(0));
                    assertThat(cluster1.getSkippedShards(), equalTo(0));
                    assertThat(cluster1.getFailedShards(), equalTo(0));

                    EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                    assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                    assertThat(localCluster.getTotalShards(), greaterThan(0));
                    assertThat(localCluster.getSuccessfulShards(), equalTo(localCluster.getTotalShards()));
                    assertThat(localCluster.getSkippedShards(), equalTo(0));
                    assertThat(localCluster.getFailedShards(), equalTo(0));
                }
            }
        } finally {
            clearSkipUnavailable();
        }
    }

    public void testEnrichCoordinatorThenAnyWithSingleUnavailableRemoteAndNotLocal() throws IOException {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        boolean skipUnavailableRemote1 = randomBoolean();
        setSkipUnavailable(REMOTE_CLUSTER_1, skipUnavailableRemote1);

        try {
            // close remote-cluster-1 so that it is unavailable
            cluster(REMOTE_CLUSTER_1).close();
            String query = String.format(Locale.ROOT, """
                FROM %s:events
                | eval ip= TO_STR(host)
                | %s
                | %s
                | stats c = COUNT(*) by vendor
                | sort vendor
                """, REMOTE_CLUSTER_1, enrichHosts(Enrich.Mode.COORDINATOR), enrichVendors(Enrich.Mode.ANY));
            if (skipUnavailableRemote1 == false) {
                Exception exception = expectThrows(Exception.class, () -> runQuery(query, requestIncludeMeta));
                assertTrue(ExceptionsHelper.isRemoteUnavailableException(exception));
            } else {
                try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                    assertThat(getValuesList(resp).size(), equalTo(0));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1)));
                    assertCCSExecutionInfoDetails(executionInfo);

                    EsqlExecutionInfo.Cluster cluster1 = executionInfo.getCluster(REMOTE_CLUSTER_1);
                    assertThat(cluster1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                    assertThat(cluster1.getTotalShards(), equalTo(0));
                    assertThat(cluster1.getSuccessfulShards(), equalTo(0));
                    assertThat(cluster1.getSkippedShards(), equalTo(0));
                    assertThat(cluster1.getFailedShards(), equalTo(0));
                }
            }
        } finally {
            clearSkipUnavailable();
        }
    }

    public void testEnrichRemoteWithVendor() throws IOException {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        boolean skipUnavailableRemote2 = randomBoolean();
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        setSkipUnavailable(REMOTE_CLUSTER_2, skipUnavailableRemote2);

        try {
            // close remote-cluster-1 so that it is unavailable
            cluster(REMOTE_CLUSTER_1).close();

            for (Enrich.Mode hostMode : List.of(Enrich.Mode.ANY, Enrich.Mode.REMOTE)) {
                var query = String.format(Locale.ROOT, """
                    FROM *:events,events
                    | eval ip= TO_STR(host)
                    | %s
                    | %s
                    | stats c = COUNT(*) by vendor
                    | sort vendor
                    """, enrichHosts(hostMode), enrichVendors(Enrich.Mode.REMOTE));
                try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                    assertThat(getValuesList(resp).size(), greaterThan(0));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertThat(
                        executionInfo.clusterAliases(),
                        equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2))
                    );
                    assertCCSExecutionInfoDetails(executionInfo);

                    EsqlExecutionInfo.Cluster cluster1 = executionInfo.getCluster(REMOTE_CLUSTER_1);
                    assertThat(cluster1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                    assertThat(cluster1.getTotalShards(), equalTo(0));
                    assertThat(cluster1.getSuccessfulShards(), equalTo(0));
                    assertThat(cluster1.getSkippedShards(), equalTo(0));
                    assertThat(cluster1.getFailedShards(), equalTo(0));
                    assertThat(cluster1.getTook().millis(), greaterThanOrEqualTo(0L));

                    EsqlExecutionInfo.Cluster cluster2 = executionInfo.getCluster(REMOTE_CLUSTER_2);
                    assertThat(cluster2.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                    assertThat(cluster2.getTotalShards(), greaterThan(0));
                    assertThat(cluster2.getSuccessfulShards(), equalTo(cluster2.getSuccessfulShards()));
                    assertThat(cluster2.getSkippedShards(), equalTo(0));
                    assertThat(cluster2.getFailedShards(), equalTo(0));

                    EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                    assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                    assertThat(localCluster.getTotalShards(), greaterThan(0));
                    assertThat(localCluster.getSuccessfulShards(), equalTo(localCluster.getTotalShards()));
                    assertThat(localCluster.getSkippedShards(), equalTo(0));
                    assertThat(localCluster.getFailedShards(), equalTo(0));
                }
            }

            // close remote-cluster-2 so that it is also unavailable
            cluster(REMOTE_CLUSTER_2).close();

            for (Enrich.Mode hostMode : List.of(Enrich.Mode.ANY, Enrich.Mode.REMOTE)) {
                var query = String.format(Locale.ROOT, """
                    FROM *:events,events
                    | eval ip= TO_STR(host)
                    | %s
                    | %s
                    | stats c = COUNT(*) by vendor
                    | sort vendor
                    """, enrichHosts(hostMode), enrichVendors(Enrich.Mode.REMOTE));
                if (skipUnavailableRemote2 == false) {
                    Exception exception = expectThrows(Exception.class, () -> runQuery(query, requestIncludeMeta));
                    assertTrue(ExceptionsHelper.isRemoteUnavailableException(exception));
                } else {

                    try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                        assertThat(getValuesList(resp).size(), greaterThan(0));
                        EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                        assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                        assertThat(
                            executionInfo.clusterAliases(),
                            equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2))
                        );
                        assertCCSExecutionInfoDetails(executionInfo);

                        EsqlExecutionInfo.Cluster cluster1 = executionInfo.getCluster(REMOTE_CLUSTER_1);
                        assertThat(cluster1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                        assertThat(cluster1.getTotalShards(), equalTo(0));
                        assertThat(cluster1.getSuccessfulShards(), equalTo(0));
                        assertThat(cluster1.getSkippedShards(), equalTo(0));
                        assertThat(cluster1.getFailedShards(), equalTo(0));
                        assertThat(cluster1.getTook().millis(), greaterThanOrEqualTo(0L));

                        EsqlExecutionInfo.Cluster cluster2 = executionInfo.getCluster(REMOTE_CLUSTER_2);
                        assertThat(cluster2.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                        assertThat(cluster2.getTotalShards(), equalTo(0));
                        assertThat(cluster2.getSuccessfulShards(), equalTo(0));
                        assertThat(cluster2.getSkippedShards(), equalTo(0));
                        assertThat(cluster2.getFailedShards(), equalTo(0));

                        EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                        assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                        assertThat(localCluster.getTotalShards(), greaterThan(0));
                        assertThat(localCluster.getSuccessfulShards(), equalTo(localCluster.getTotalShards()));
                        assertThat(localCluster.getSkippedShards(), equalTo(0));
                        assertThat(localCluster.getFailedShards(), equalTo(0));
                    }
                }
            }
        } finally {
            clearSkipUnavailable();
        }
    }

    protected EsqlQueryResponse runQuery(String query, Boolean ccsMetadataInResponse) {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query(query);
        request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
        if (randomBoolean()) {
            request.profile(true);
        }
        if (ccsMetadataInResponse != null) {
            request.includeCCSMetadata(ccsMetadataInResponse);
        }
        return client(LOCAL_CLUSTER).execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS);
    }

    private static void assertCCSExecutionInfoDetails(EsqlExecutionInfo executionInfo) {
        assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));
        assertTrue(executionInfo.isCrossClusterSearch());

        for (String clusterAlias : executionInfo.clusterAliases()) {
            EsqlExecutionInfo.Cluster cluster = executionInfo.getCluster(clusterAlias);
            assertThat(cluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(cluster.getTook().millis(), lessThanOrEqualTo(executionInfo.overallTook().millis()));
        }
    }

    public static Tuple<Boolean, Boolean> randomIncludeCCSMetadata() {
        return switch (randomIntBetween(1, 3)) {
            case 1 -> new Tuple<>(Boolean.TRUE, Boolean.TRUE);
            case 2 -> new Tuple<>(Boolean.FALSE, Boolean.FALSE);
            case 3 -> new Tuple<>(null, Boolean.FALSE);
            default -> throw new AssertionError("should not get here");
        };
    }

    public static class LocalStateEnrich extends LocalStateCompositeXPackPlugin {

        public LocalStateEnrich(final Settings settings, final Path configPath) {
            super(settings, configPath);

            plugins.add(new EnrichPlugin(settings) {
                @Override
                protected XPackLicenseState getLicenseState() {
                    return this.getLicenseState();
                }
            });
        }

        public static class EnrichTransportXPackInfoAction extends TransportXPackInfoAction {
            @Inject
            public EnrichTransportXPackInfoAction(
                TransportService transportService,
                ActionFilters actionFilters,
                LicenseService licenseService,
                NodeClient client
            ) {
                super(transportService, actionFilters, licenseService, client);
            }

            @Override
            protected List<ActionType<XPackInfoFeatureResponse>> infoActions() {
                return Collections.singletonList(XPackInfoFeatureAction.ENRICH);
            }
        }

        @Override
        protected Class<? extends TransportAction<XPackInfoRequest, XPackInfoResponse>> getInfoAction() {
            return EnrichTransportXPackInfoAction.class;
        }
    }
}
