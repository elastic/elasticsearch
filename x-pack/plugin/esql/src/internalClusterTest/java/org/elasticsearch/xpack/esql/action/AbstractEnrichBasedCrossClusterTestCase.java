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
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

public abstract class AbstractEnrichBasedCrossClusterTestCase extends AbstractMultiClustersTestCase {

    public static String REMOTE_CLUSTER_1 = "c1";
    public static String REMOTE_CLUSTER_2 = "c2";

    /**
     * subclasses should override if they don't want enrich policies wiped after each test method run
     */
    protected boolean tolerateErrorsWhenWipingEnrichPolicies() {
        return false;
    }

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
    }

    protected Collection<String> allClusters() {
        return CollectionUtils.appendToCopy(remoteClusterAlias(), LOCAL_CLUSTER);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(CrossClusterEnrichIT.LocalStateEnrich.class);
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
        var vendors = Map.of(LOCAL_CLUSTER, localVendors, REMOTE_CLUSTER_1, c1Vendors, REMOTE_CLUSTER_2, c2Vendors);
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
        for (var c : Map.of(LOCAL_CLUSTER, e0, REMOTE_CLUSTER_1, e1, REMOTE_CLUSTER_2, e2).entrySet()) {
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
                if (tolerateErrorsWhenWipingEnrichPolicies()) {
                    try {
                        client(cluster).execute(
                            DeleteEnrichPolicyAction.INSTANCE,
                            new DeleteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policy)
                        );
                    } catch (Exception e) {
                        assertThat(e.getMessage(), containsString("Cluster is already closed"));
                    }

                } else {
                    client(cluster).execute(
                        DeleteEnrichPolicyAction.INSTANCE,
                        new DeleteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policy)
                    );
                }
            }
        }
    }

    static String enrichHosts(Enrich.Mode mode) {
        return EsqlTestUtils.randomEnrichCommand("hosts", mode, hostPolicy.getMatchField(), hostPolicy.getEnrichFields());
    }

    static String enrichVendors(Enrich.Mode mode) {
        return EsqlTestUtils.randomEnrichCommand("vendors", mode, vendorPolicy.getMatchField(), vendorPolicy.getEnrichFields());
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
            return CrossClusterQueriesWithInvalidLicenseIT.LocalStateEnrich.EnrichTransportXPackInfoAction.class;
        }
    }
}
