/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.reindex.ReindexPlugin;
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
import org.junit.Before;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class EnrichIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EsqlPlugin.class);
        plugins.add(InternalExchangePlugin.class);
        plugins.add(LocalStateEnrich.class);
        plugins.add(IngestCommonPlugin.class);
        plugins.add(ReindexPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .build();
    }

    @Before
    public void setupEnrichPolicies() {
        client().admin()
            .indices()
            .prepareCreate("songs")
            .setMapping("song_id", "type=keyword", "title", "type=keyword", "artist", "type=keyword", "length", "type=double")
            .get();
        record Song(String id, String title, String artist, double length) {

        }
        var songs = List.of(
            new Song("s1", "Hotel California", "Eagles", 7.12),
            new Song("s2", "In The End", "Linkin Park", 3.36),
            new Song("s3", "Numb", "Linkin Park", 3.05),
            new Song("s4", "The Sound Of Silence", "Disturbed", 4.08)
        );
        for (var s : songs) {
            client().prepareIndex("songs").setSource("song_id", s.id, "title", s.title, "artist", s.artist, "length", s.length).get();
        }
        client().admin().indices().prepareRefresh("songs").get();
        EnrichPolicy policy = new EnrichPolicy("match", null, List.of("songs"), "song_id", List.of("title", "artist", "length"));
        client().execute(PutEnrichPolicyAction.INSTANCE, new PutEnrichPolicyAction.Request("songs", policy)).actionGet();
        client().execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request("songs")).actionGet();
        assertAcked(client().admin().indices().prepareDelete("songs"));
    }

    @After
    public void cleanEnrichPolicies() {
        cluster().wipe(Set.of());
        client().execute(DeleteEnrichPolicyAction.INSTANCE, new DeleteEnrichPolicyAction.Request("songs"));
    }

    @Before
    public void setupMainIndex() {
        var localListens = List.of(
            new Listen(1, "s3", 1.5),
            new Listen(2, "s2", 2.0),
            new Listen(3, "s1", 0.5),
            new Listen(4, "s3", 1.0),
            new Listen(5, "s1", 2.5),
            new Listen(6, "s1", 0.25),
            new Listen(7, "s2", 3.0)
        );
        client().admin()
            .indices()
            .prepareCreate("listens")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1))
            .setMapping("timestamp", "type=long", "song_id", "type=keyword", "duration", "type=double")
            .get();
        for (Listen listen : localListens) {
            client().prepareIndex("listens")
                .setSource("timestamp", listen.timestamp, "song_id", listen.songId, "duration", listen.duration)
                .get();
        }
        client().admin().indices().prepareRefresh("listens").get();
    }

    record Listen(long timestamp, String songId, double duration) {

    }

    public void testSumDurationByArtist() {
        Function<EsqlQueryResponse, Map<String, Double>> extractStats = resp -> {
            List<ColumnInfo> columns = resp.columns();
            assertThat(columns, hasSize(2));
            assertThat(columns.get(0).name(), equalTo("sum(duration)"));
            assertThat(columns.get(0).type(), equalTo("double"));
            assertThat(columns.get(1).name(), equalTo("artist"));
            assertThat(columns.get(1).type(), equalTo("keyword"));
            Iterator<Iterator<Object>> rows = resp.values();
            Map<String, Double> actualValues = new HashMap<>();
            while (rows.hasNext()) {
                Iterator<Object> row = rows.next();
                Object v = row.next();
                Object k = row.next();
                actualValues.put((String) k, (Double) v);
            }
            return actualValues;
        };

        var statsCommands = List.of(
            "ENRICH songs | STATS sum(duration) by artist",
            "STATS duration = sum(duration) by song_id | ENRICH songs | STATS sum(duration) by artist"
        );
        for (String statsCommand : statsCommands) {
            try (var resp = run("from listens* | " + statsCommand)) {
                assertThat(extractStats.apply(resp), equalTo(Map.of("Eagles", 3.25, "Linkin Park", 7.5)));
            }
        }
    }

    public void testAvgDurationByArtist() {
        Function<EsqlQueryResponse, Map<String, Double>> extractStats = resp -> {
            List<ColumnInfo> columns = resp.columns();
            assertThat(columns, hasSize(2));
            assertThat(columns.get(0).name(), equalTo("avg(duration)"));
            assertThat(columns.get(0).type(), equalTo("double"));
            assertThat(columns.get(1).name(), equalTo("artist"));
            assertThat(columns.get(1).type(), equalTo("keyword"));
            Iterator<Iterator<Object>> rows = resp.values();
            Map<String, Double> actualValues = new HashMap<>();
            while (rows.hasNext()) {
                Iterator<Object> row = rows.next();
                Object v = row.next();
                Object k = row.next();
                actualValues.put((String) k, (Double) v);
            }
            return actualValues;
        };
        try (var resp = run("from listens* | ENRICH songs | STATS avg(duration) by artist")) {
            Map<String, Double> stats = extractStats.apply(resp);
            assertThat(stats.keySet(), containsInAnyOrder("Eagles", "Linkin Park"));
            assertThat(stats.get("Eagles"), closeTo(1.08333, 0.1));
            assertThat(stats.get("Linkin Park"), closeTo(1.875, 0.1));
        }
    }

    public void testListeningRatio() {
        Function<EsqlQueryResponse, Map<String, Double>> extractStats = resp -> {
            List<ColumnInfo> columns = resp.columns();
            assertThat(columns, hasSize(2));
            assertThat(columns.get(0).name(), equalTo("ratio"));
            assertThat(columns.get(0).type(), equalTo("double"));
            assertThat(columns.get(1).name(), equalTo("artist"));
            assertThat(columns.get(1).type(), equalTo("keyword"));
            Iterator<Iterator<Object>> rows = resp.values();
            Map<String, Double> actualValues = new HashMap<>();
            while (rows.hasNext()) {
                Iterator<Object> row = rows.next();
                Object v = row.next();
                Object k = row.next();
                actualValues.put((String) k, (Double) v);
            }
            return actualValues;
        };

        var statsCommand = " | ENRICH songs | STATS d = sum(duration), l = sum(length) by artist | EVAL ratio=d /l | KEEP ratio, artist";
        try (var resp = run("from listens* " + statsCommand)) {
            Map<String, Double> stats = extractStats.apply(resp);
            assertThat(stats.keySet(), containsInAnyOrder("Eagles", "Linkin Park"));
            assertThat(stats.get("Eagles"), closeTo(0.1521, 0.05));
            assertThat(stats.get("Linkin Park"), closeTo(0.585, 0.05));
        }
    }

    public void testFilterAfterEnrich() {
        try (var resp = run("from listens* | ENRICH songs | WHERE length < 3.2 | limit 10 | KEEP artist,title")) {
            Iterator<Object> row = resp.values().next();
            assertThat(row.next(), equalTo("Linkin Park"));
            assertThat(row.next(), equalTo("Numb"));
        }
    }

    public void testTopN() {
        try (var resp = run("from listens* | sort timestamp DESC | limit 1 | ENRICH songs | KEEP timestamp, artist")) {
            Iterator<Object> row = resp.values().next();
            assertThat(row.next(), equalTo(7L));
            assertThat(row.next(), equalTo("Linkin Park"));
        }
        try (var resp = run("from listens* | ENRICH songs | sort timestamp DESC | limit 1 | KEEP timestamp, artist")) {
            Iterator<Object> row = resp.values().next();
            assertThat(row.next(), equalTo(7L));
            assertThat(row.next(), equalTo("Linkin Park"));
        }
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
