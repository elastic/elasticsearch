/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

public class CrossClustersQueryIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER = "cluster-a";

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPlugin.class);
        plugins.add(InternalExchangePlugin.class);
        return plugins;
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

    @Before
    public void populateLocalIndices() {
        Client localClient = client(LOCAL_CLUSTER);
        assertAcked(
            localClient.admin()
                .indices()
                .prepareCreate("logs-1")
                .setSettings(Settings.builder().put("index.number_of_shards", randomIntBetween(1, 5)))
                .setMapping("id", "type=keyword", "tag", "type=keyword", "v", "type=long")
        );
        for (int i = 0; i < 10; i++) {
            localClient.prepareIndex("logs-1").setSource("id", "local-" + i, "tag", "local", "v", i).get();
        }
        localClient.admin().indices().prepareRefresh("logs-1").get();
    }

    @Before
    public void populateRemoteIndices() {
        Client remoteClient = client(REMOTE_CLUSTER);
        assertAcked(
            remoteClient.admin()
                .indices()
                .prepareCreate("logs-2")
                .setSettings(Settings.builder().put("index.number_of_shards", randomIntBetween(1, 5)))
                .setMapping("id", "type=keyword", "tag", "type=keyword", "v", "type=long")
        );
        for (int i = 0; i < 10; i++) {
            remoteClient.prepareIndex("logs-2").setSource("id", "remote-" + i, "tag", "remote", "v", i * i).get();
        }
        remoteClient.admin().indices().prepareRefresh("logs-2").get();
    }

    public void testSimple() {
        try (EsqlQueryResponse resp = runQuery("from logs-*,*:logs-* | stats sum (v)")) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), equalTo(List.of(330L)));
        }
        try (EsqlQueryResponse resp = runQuery("from logs-*,*:logs-* | stats count(*) by tag | sort tag | keep tag")) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(2));
            assertThat(values.get(0), equalTo(List.of("local")));
            assertThat(values.get(1), equalTo(List.of("remote")));
        }
    }

    public void testMetadataIndex() {
        try (EsqlQueryResponse resp = runQuery("FROM logs*,*:logs* METADATA _index | stats sum(v) by _index | sort _index")) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.get(0), equalTo(List.of(285L, "cluster-a:logs-2")));
            assertThat(values.get(1), equalTo(List.of(45L, "logs-1")));
        }
    }

    public void testProfile() {
        assumeTrue("pragmas only enabled on snapshot builds", Build.current().isSnapshot());
        final int localOnlyProfiles;
        // uses shard partitioning as segments can be merged during these queries
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.DATA_PARTITIONING.getKey(), DataPartitioning.SHARD).build());
        {
            EsqlQueryRequest request = new EsqlQueryRequest();
            request.query("FROM logs* | stats sum(v)");
            request.pragmas(pragmas);
            request.profile(true);
            try (EsqlQueryResponse resp = runQuery(request)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values.get(0), equalTo(List.of(45L)));
                assertNotNull(resp.profile());
                List<DriverProfile> drivers = resp.profile().drivers();
                assertThat(drivers.size(), greaterThanOrEqualTo(2)); // one coordinator and at least one data
                localOnlyProfiles = drivers.size();
            }
        }
        final int remoteOnlyProfiles;
        {
            EsqlQueryRequest request = new EsqlQueryRequest();
            request.query("FROM *:logs* | stats sum(v)");
            request.pragmas(pragmas);
            request.profile(true);
            try (EsqlQueryResponse resp = runQuery(request)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values.get(0), equalTo(List.of(285L)));
                assertNotNull(resp.profile());
                List<DriverProfile> drivers = resp.profile().drivers();
                assertThat(drivers.size(), greaterThanOrEqualTo(3)); // two coordinators and at least one data
                remoteOnlyProfiles = drivers.size();
            }
        }
        final int allProfiles;
        {
            EsqlQueryRequest request = new EsqlQueryRequest();
            request.query("FROM logs*,*:logs* | stats total = sum(v)");
            request.pragmas(pragmas);
            request.profile(true);
            try (EsqlQueryResponse resp = runQuery(request)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values.get(0), equalTo(List.of(330L)));
                assertNotNull(resp.profile());
                List<DriverProfile> drivers = resp.profile().drivers();
                assertThat(drivers.size(), greaterThanOrEqualTo(4)); // two coordinators and at least two data
                allProfiles = drivers.size();
            }
        }
        assertThat(allProfiles, equalTo(localOnlyProfiles + remoteOnlyProfiles - 1));
    }

    public void testWarnings() throws Exception {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query("FROM logs*,*:logs* | EVAL ip = to_ip(id) | STATS total = sum(v) by ip | LIMIT 10");
        PlainActionFuture<EsqlQueryResponse> future = new PlainActionFuture<>();
        InternalTestCluster cluster = cluster(LOCAL_CLUSTER);
        String node = randomFrom(cluster.getNodeNames());
        CountDownLatch latch = new CountDownLatch(1);
        cluster.client(node).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(resp -> {
            TransportService ts = cluster.getInstance(TransportService.class, node);
            Map<String, List<String>> responseHeaders = ts.getThreadPool().getThreadContext().getResponseHeaders();
            List<String> warnings = responseHeaders.getOrDefault("Warning", List.of())
                .stream()
                .filter(w -> w.contains("is not an IP string literal"))
                .toList();
            assertThat(warnings.size(), greaterThanOrEqualTo(20));
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.get(0).get(0), equalTo(330L));
            assertNull(values.get(0).get(1));
            latch.countDown();
        }, e -> {
            latch.countDown();
            throw new AssertionError(e);
        }));
        assertTrue(latch.await(30, TimeUnit.SECONDS));
    }

    protected EsqlQueryResponse runQuery(String query) {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(query);
        request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
        return runQuery(request);
    }

    protected EsqlQueryResponse runQuery(EsqlQueryRequest request) {
        return client(LOCAL_CLUSTER).execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS);
    }
}
