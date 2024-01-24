/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
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
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.EnrichPlugin;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.After;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

public class CrossClustersEnrichIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER = "cluster_a";

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPlugin.class);
        plugins.add(InternalExchangePlugin.class);
        plugins.add(LocalStateEnrich.class);
        plugins.add(IngestCommonPlugin.class);
        plugins.add(ReindexPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
    }

    public static class InternalExchangePlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(
                Setting.timeSetting(
                    ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING,
                    TimeValue.timeValueSeconds(30),
                    Setting.Property.NodeScope
                )
            );
        }
    }

    public void testUnsupportedEnrich() {
        Client localClient = client(LOCAL_CLUSTER);
        localClient.admin().indices().prepareCreate("hosts").setMapping("ip", "type=ip", "os", "type=keyword").get();
        record Host(String ip, String os) {

        }
        var hosts = List.of(new Host("192.168.1.3", "Windows"));
        for (var h : hosts) {
            localClient.prepareIndex("hosts").setSource("ip", h.ip, "os", h.os).get();
        }
        localClient.admin().indices().prepareRefresh("hosts").get();
        EnrichPolicy policy = new EnrichPolicy("match", null, List.of("hosts"), "ip", List.of("ip", "os"));
        localClient.execute(PutEnrichPolicyAction.INSTANCE, new PutEnrichPolicyAction.Request("hosts", policy)).actionGet();
        localClient.execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request("hosts")).actionGet();
        assertAcked(client(LOCAL_CLUSTER).admin().indices().prepareDelete("hosts"));

        record Event(String ip, String message) {

        }
        for (String cluster : List.of(LOCAL_CLUSTER, REMOTE_CLUSTER)) {
            var events = List.of(new Event("192.168.1.4", "access denied"), new Event("192.168.1.3", "restart"));
            assertAcked(client(cluster).admin().indices().prepareCreate("events").setMapping("ip", "type=ip", "message", "type=text"));
            for (Event e : events) {
                client(cluster).prepareIndex("events").setSource("ip", e.ip, "message", e.message).get();
            }
            client(cluster).admin().indices().prepareRefresh("events").get();
        }
        List<String> queries = List.of(
            "FROM *:events | EVAL ip_str = TO_STR(ip) | ENRICH hosts on ip_str | LIMIT 1",
            "FROM events*,*:events | EVAL ip_str = TO_STR(ip) | ENRICH hosts on ip_str | LIMIT 1",
            "FROM *:events | EVAL ip_str = TO_STR(ip) | ENRICH hosts on ip_str | STATS COUNT(*) BY ip | LIMIT 1",
            "FROM events*,*:events | EVAL ip_str = TO_STR(ip) | ENRICH hosts on ip_str | STATS COUNT(*) BY ip | LIMIT 1"
        );
        for (String q : queries) {
            Exception error = expectThrows(IllegalArgumentException.class, () -> runQuery(q).close());
            assertThat(error.getMessage(), containsString("cross clusters query doesn't support enrich yet"));
        }
    }

    @After
    public void cleanClusters() {
        cluster(LOCAL_CLUSTER).wipe(Set.of());
        client(LOCAL_CLUSTER).execute(DeleteEnrichPolicyAction.INSTANCE, new DeleteEnrichPolicyAction.Request("hosts"));
        cluster(REMOTE_CLUSTER).wipe(Set.of());
    }

    protected EsqlQueryResponse runQuery(String query) {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(query);
        request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
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
            protected List<XPackInfoFeatureAction> infoActions() {
                return Collections.singletonList(XPackInfoFeatureAction.ENRICH);
            }
        }

        @Override
        protected Class<? extends TransportAction<XPackInfoRequest, XPackInfoResponse>> getInfoAction() {
            return EnrichTransportXPackInfoAction.class;
        }
    }
}
