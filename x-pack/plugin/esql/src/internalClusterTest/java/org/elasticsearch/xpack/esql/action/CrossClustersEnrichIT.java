/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

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
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.TransportXPackInfoAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureResponse;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.EnrichPlugin;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class CrossClustersEnrichIT extends AbstractMultiClustersTestCase {

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of("c1", "c2");
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

    @After
    public void wipeEnrichPolicies() {
        for (String cluster : allClusters()) {
            cluster(cluster).wipe(Set.of());
            for (String policy : List.of("hosts", "vendors")) {
                client(cluster).execute(
                    DeleteEnrichPolicyAction.INSTANCE,
                    new DeleteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policy)
                );
            }
        }
    }

    static String enrichHosts(Enrich.Mode mode) {
        return EsqlTestUtils.randomEnrichCommand("hosts", mode, hostPolicy.getMatchField(), hostPolicy.getEnrichFields());
    }

    static String enrichVendors(Enrich.Mode mode) {
        return EsqlTestUtils.randomEnrichCommand("vendors", mode, vendorPolicy.getMatchField(), vendorPolicy.getEnrichFields());
    }

    public void testWithHostsPolicy() {
        for (var mode : Enrich.Mode.values()) {
            String query = "FROM events | eval ip= TO_STR(host) | " + enrichHosts(mode) + " | stats c = COUNT(*) by os | SORT os";
            try (EsqlQueryResponse resp = runQuery(query, null)) {
                List<List<Object>> rows = getValuesList(resp);
                assertThat(
                    rows,
                    equalTo(
                        List.of(
                            List.of(2L, "Android"),
                            List.of(1L, "Linux"),
                            List.of(1L, "MacOS"),
                            List.of(4L, "Windows"),
                            Arrays.asList(1L, (String) null)
                        )
                    )
                );
                assertFalse(resp.getExecutionInfo().isCrossClusterSearch());
            }
        }

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        for (var mode : Enrich.Mode.values()) {
            String query = "FROM *:events | eval ip= TO_STR(host) | " + enrichHosts(mode) + " | stats c = COUNT(*) by os | SORT os";
            try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                List<List<Object>> rows = getValuesList(resp);
                assertThat(
                    rows,
                    equalTo(
                        List.of(
                            List.of(1L, "Android"),
                            List.of(2L, "Linux"),
                            List.of(4L, "MacOS"),
                            List.of(3L, "Windows"),
                            List.of(1L, "iOS"),
                            Arrays.asList(2L, (String) null)
                        )
                    )
                );
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.clusterAliases(), equalTo(Set.of("c1", "c2")));
                assertCCSExecutionInfoDetails(executionInfo);
            }
        }

        for (var mode : Enrich.Mode.values()) {
            String query = "FROM *:events,events | eval ip= TO_STR(host) | " + enrichHosts(mode) + " | stats c = COUNT(*) by os | SORT os";
            try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                List<List<Object>> rows = getValuesList(resp);
                assertThat(
                    rows,
                    equalTo(
                        List.of(
                            List.of(3L, "Android"),
                            List.of(3L, "Linux"),
                            List.of(5L, "MacOS"),
                            List.of(7L, "Windows"),
                            List.of(1L, "iOS"),
                            Arrays.asList(3L, (String) null)
                        )
                    )
                );
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.clusterAliases(), equalTo(Set.of("", "c1", "c2")));
                assertCCSExecutionInfoDetails(executionInfo);
            }
        }
    }

    public void testEnrichHostsAggThenEnrichVendorCoordinator() {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        for (var hostMode : Enrich.Mode.values()) {
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
                assertThat(
                    getValuesList(resp),
                    equalTo(
                        List.of(
                            List.of(6L, "Apple"),
                            List.of(7L, "Microsoft"),
                            List.of(3L, "Redhat"),
                            List.of(3L, "Samsung"),
                            Arrays.asList(3L, (String) null)
                        )
                    )
                );
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.clusterAliases(), equalTo(Set.of("", "c1", "c2")));
                assertCCSExecutionInfoDetails(executionInfo);
            }
        }
    }

    public void testEnrichTwiceThenAggs() {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

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
                assertThat(
                    getValuesList(resp),
                    equalTo(
                        List.of(
                            List.of(6L, "Apple"),
                            List.of(7L, "Microsoft"),
                            List.of(3L, "Redhat"),
                            List.of(3L, "Samsung"),
                            Arrays.asList(3L, (String) null)
                        )
                    )
                );
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.clusterAliases(), equalTo(Set.of("", "c1", "c2")));
                assertCCSExecutionInfoDetails(executionInfo);
            }
        }
    }

    public void testEnrichCoordinatorThenAny() {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        String query = String.format(Locale.ROOT, """
            FROM *:events,events
            | eval ip= TO_STR(host)
            | %s
            | %s
            | stats c = COUNT(*) by vendor
            | sort vendor
            """, enrichHosts(Enrich.Mode.COORDINATOR), enrichVendors(Enrich.Mode.ANY));
        try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
            assertThat(
                getValuesList(resp),
                equalTo(
                    List.of(
                        List.of(6L, "Apple"),
                        List.of(7L, "Microsoft"),
                        List.of(3L, "Redhat"),
                        List.of(3L, "Samsung"),
                        Arrays.asList(3L, (String) null)
                    )
                )
            );
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
            assertThat(executionInfo.clusterAliases(), equalTo(Set.of("", "c1", "c2")));
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testEnrichCoordinatorWithVendor() {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        for (Enrich.Mode hostMode : Enrich.Mode.values()) {
            String query = String.format(Locale.ROOT, """
                FROM *:events,events
                | eval ip= TO_STR(host)
                | %s
                | %s
                | stats c = COUNT(*) by vendor
                | sort vendor
                """, enrichHosts(hostMode), enrichVendors(Enrich.Mode.COORDINATOR));
            try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                assertThat(
                    getValuesList(resp),
                    equalTo(
                        List.of(
                            List.of(6L, "Apple"),
                            List.of(7L, "Microsoft"),
                            List.of(3L, "Redhat"),
                            List.of(3L, "Samsung"),
                            Arrays.asList(3L, (String) null)
                        )
                    )
                );
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.clusterAliases(), equalTo(Set.of("", "c1", "c2")));
                assertCCSExecutionInfoDetails(executionInfo);
            }
        }

    }

    public void testEnrichRemoteWithVendor() {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

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
                assertThat(
                    getValuesList(resp),
                    equalTo(
                        List.of(
                            List.of(6L, "Apple"),
                            List.of(7L, "Microsoft"),
                            List.of(1L, "Redhat"),
                            List.of(2L, "Samsung"),
                            List.of(1L, "Sony"),
                            List.of(2L, "Suse"),
                            Arrays.asList(3L, (String) null)
                        )
                    )
                );
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.clusterAliases(), equalTo(Set.of("", "c1", "c2")));
                assertCCSExecutionInfoDetails(executionInfo);
            }
        }
    }

    public void testTopNThenEnrichRemote() {
        String query = String.format(Locale.ROOT, """
            FROM *:events,events
            | eval ip= TO_STR(host)
            | SORT ip
            | LIMIT 5
            | %s
            """, enrichHosts(Enrich.Mode.REMOTE));
        var error = expectThrows(VerificationException.class, () -> runQuery(query, randomBoolean()).close());
        assertThat(error.getMessage(), containsString("ENRICH with remote policy can't be executed after LIMIT"));
    }

    public void testLimitThenEnrichRemote() {
        String query = String.format(Locale.ROOT, """
            FROM *:events,events
            | LIMIT 10
            | eval ip= TO_STR(host)
            | %s
            """, enrichHosts(Enrich.Mode.REMOTE));
        var error = expectThrows(VerificationException.class, () -> runQuery(query, randomBoolean()).close());
        assertThat(error.getMessage(), containsString("ENRICH with remote policy can't be executed after LIMIT"));
    }

    public void testAggThenEnrichRemote() {
        String query = String.format(Locale.ROOT, """
            FROM *:events,events
            | eval ip= TO_STR(host)
            | %s
            | stats c = COUNT(*) by os
            | %s
            | sort vendor
            """, enrichHosts(Enrich.Mode.ANY), enrichVendors(Enrich.Mode.REMOTE));
        var error = expectThrows(VerificationException.class, () -> runQuery(query, randomBoolean()).close());
        assertThat(error.getMessage(), containsString("ENRICH with remote policy can't be executed after STATS"));
    }

    public void testEnrichCoordinatorThenEnrichRemote() {
        String query = String.format(Locale.ROOT, """
            FROM *:events,events
            | eval ip= TO_STR(host)
            | %s
            | %s
            | sort vendor
            """, enrichHosts(Enrich.Mode.COORDINATOR), enrichVendors(Enrich.Mode.REMOTE));
        var error = expectThrows(VerificationException.class, () -> runQuery(query, randomBoolean()).close());
        assertThat(
            error.getMessage(),
            containsString("ENRICH with remote policy can't be executed after another ENRICH with coordinator policy")
        );
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
        List<EsqlExecutionInfo.Cluster> clusters = executionInfo.clusterAliases()
            .stream()
            .map(alias -> executionInfo.getCluster(alias))
            .collect(Collectors.toList());

        for (EsqlExecutionInfo.Cluster cluster : clusters) {
            assertThat(cluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(cluster.getIndexExpression(), equalTo("events"));
            assertThat(cluster.getTotalShards(), equalTo(1));
            assertThat(cluster.getSuccessfulShards(), equalTo(1));
            assertThat(cluster.getSkippedShards(), equalTo(0));
            assertThat(cluster.getFailedShards(), equalTo(0));
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

        public LocalStateEnrich(final Settings settings, final Path configPath) throws Exception {
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
