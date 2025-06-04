/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.RemoteTransportException;
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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupService;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.transport.AbstractSimpleTransportTestCase.IGNORE_DESERIALIZATION_ERRORS_SETTING;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class EnrichIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EsqlPlugin.class);
        plugins.add(InternalExchangePlugin.class);
        plugins.add(LocalStateEnrich.class);
        plugins.add(IngestCommonPlugin.class);
        plugins.add(ReindexPlugin.class);
        plugins.add(InternalTransportSettingPlugin.class);
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    public static class InternalTransportSettingPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(IGNORE_DESERIALIZATION_ERRORS_SETTING);
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "128mb")
            /*
             * Force standard settings for the request breaker or we may not break at all.
             * Without this we can randomly decide to use the `noop` breaker for request
             * and it won't break.....
             */
            .put(
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(),
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getDefault(Settings.EMPTY)
            )
            .put(
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(),
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getDefault(Settings.EMPTY)
            )
            .put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, TimeValue.timeValueMillis(between(3000, 4000)))
            .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_SIZE_SETTING, ByteSizeValue.ofBytes(between(0, 256)))
            .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_MAX_SIZE_SETTING, ByteSizeValue.ofBytes(between(0, 1024)))
            // allow reading pages from network can trip the circuit breaker
            .put(IGNORE_DESERIALIZATION_ERRORS_SETTING.getKey(), true)
            .build();
    }

    @Override
    protected EsqlQueryResponse run(EsqlQueryRequest request) {
        final Client client;
        if (randomBoolean()) {
            client = client(randomFrom(clusterService().state().nodes().getCoordinatingOnlyNodes().values()).getName());
        } else {
            client = client();
        }
        if (request.profile() == false && randomBoolean()) {
            request.profile(true);
        }
        if (randomBoolean()) {
            setRequestCircuitBreakerLimit(ByteSizeValue.ofBytes(between(256, 4096)));
            try {
                return client.execute(EsqlQueryAction.INSTANCE, request).actionGet(2, TimeUnit.MINUTES);
            } catch (Exception e) {
                logger.info("request failed", e);
                EsqlTestUtils.assertEsqlFailure(e);
                ensureBlocksReleased();
            } finally {
                setRequestCircuitBreakerLimit(null);
            }
        }
        return client.execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS);
    }

    static EnrichPolicy policy = new EnrichPolicy("match", null, List.of("songs"), "song_id", List.of("title", "artist", "length"));

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
        client().execute(PutEnrichPolicyAction.INSTANCE, new PutEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, "songs", policy))
            .actionGet();
        client().execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, "songs"))
            .actionGet();
        assertAcked(client().admin().indices().prepareDelete("songs"));
    }

    @After
    public void cleanEnrichPolicies() {
        cluster().wipe(Set.of());
        client().execute(DeleteEnrichPolicyAction.INSTANCE, new DeleteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, "songs"));
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

    @Before
    public void ensureAtLeastOneCoordinatingNodeOnly() {
        if (clusterService().state().nodes().getCoordinatingOnlyNodes().isEmpty()) {
            internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        }
    }

    record Listen(long timestamp, String songId, double duration) {

    }

    private static String enrichSongCommand() {
        return EsqlTestUtils.randomEnrichCommand(
            "songs",
            randomFrom(Enrich.Mode.COORDINATOR, Enrich.Mode.ANY),
            policy.getMatchField(),
            policy.getEnrichFields()
        );
    }

    public void testSumDurationByArtist() {
        Function<EsqlQueryResponse, Map<String, Double>> extractStats = resp -> {
            List<ColumnInfoImpl> columns = resp.columns();
            assertThat(columns, hasSize(2));
            assertThat(columns.get(0).name(), equalTo("sum(duration)"));
            assertThat(columns.get(0).type(), equalTo(DataType.DOUBLE));
            assertThat(columns.get(1).name(), equalTo("artist"));
            assertThat(columns.get(1).type(), equalTo(DataType.KEYWORD));
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
            enrichSongCommand() + " | STATS sum(duration) by artist",
            "STATS duration = sum(duration) by song_id | " + enrichSongCommand() + " | STATS sum(duration) by artist"
        );
        for (String statsCommand : statsCommands) {
            try (var resp = run("from listens* | " + statsCommand)) {
                assertThat(extractStats.apply(resp), equalTo(Map.of("Eagles", 3.25, "Linkin Park", 7.5)));
            }
        }
    }

    public void testAvgDurationByArtist() {
        Function<EsqlQueryResponse, Map<String, Double>> extractStats = resp -> {
            List<ColumnInfoImpl> columns = resp.columns();
            assertThat(columns, hasSize(2));
            assertThat(columns.get(0).name(), equalTo("avg(duration)"));
            assertThat(columns.get(0).type(), equalTo(DataType.DOUBLE));
            assertThat(columns.get(1).name(), equalTo("artist"));
            assertThat(columns.get(1).type(), equalTo(DataType.KEYWORD));
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
        try (var resp = run("from listens* | " + enrichSongCommand() + " | STATS avg(duration) by artist")) {
            Map<String, Double> stats = extractStats.apply(resp);
            assertThat(stats.keySet(), containsInAnyOrder("Eagles", "Linkin Park"));
            assertThat(stats.get("Eagles"), closeTo(1.08333, 0.1));
            assertThat(stats.get("Linkin Park"), closeTo(1.875, 0.1));
        }
    }

    public void testListeningRatio() {
        Function<EsqlQueryResponse, Map<String, Double>> extractStats = resp -> {
            List<ColumnInfoImpl> columns = resp.columns();
            assertThat(columns, hasSize(2));
            assertThat(columns.get(0).name(), equalTo("ratio"));
            assertThat(columns.get(0).type(), equalTo(DataType.DOUBLE));
            assertThat(columns.get(1).name(), equalTo("artist"));
            assertThat(columns.get(1).type(), equalTo(DataType.KEYWORD));
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

        var statsCommand = "STATS d = sum(duration), l = sum(length) by artist | EVAL ratio=d /l | KEEP ratio, artist";
        try (var resp = run("from listens* | " + enrichSongCommand() + "|" + statsCommand)) {
            Map<String, Double> stats = extractStats.apply(resp);
            assertThat(stats.keySet(), containsInAnyOrder("Eagles", "Linkin Park"));
            assertThat(stats.get("Eagles"), closeTo(0.1521, 0.05));
            assertThat(stats.get("Linkin Park"), closeTo(0.585, 0.05));
        }
    }

    public void testFilterAfterEnrich() {
        try (var resp = run("from listens* | " + enrichSongCommand() + " | WHERE length < 3.2 | limit 10 | KEEP artist,title")) {
            Iterator<Object> row = resp.values().next();
            assertThat(row.next(), equalTo("Linkin Park"));
            assertThat(row.next(), equalTo("Numb"));
        }
    }

    public void testTopN() {
        try (var resp = run("from listens* | sort timestamp DESC | limit 1 |" + enrichSongCommand() + " | KEEP timestamp, artist")) {
            Iterator<Object> row = resp.values().next();
            assertThat(row.next(), equalTo(7L));
            assertThat(row.next(), equalTo("Linkin Park"));
        }
        try (var resp = run("from listens* | " + enrichSongCommand() + " | sort timestamp DESC | limit 1 | KEEP timestamp, artist")) {
            Iterator<Object> row = resp.values().next();
            assertThat(row.next(), equalTo(7L));
            assertThat(row.next(), equalTo("Linkin Park"));
        }
    }

    public void testProfile() {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.pragmas(randomPragmas());
        request.query("from listens* | sort timestamp DESC | limit 1 | " + enrichSongCommand() + " | KEEP timestamp, artist");
        request.profile(true);
        try (var resp = run(request)) {
            Iterator<Object> row = resp.values().next();
            assertThat(row.next(), equalTo(7L));
            assertThat(row.next(), equalTo("Linkin Park"));
            EsqlQueryResponse.Profile profile = resp.profile();
            assertNotNull(profile);
            List<DriverProfile> drivers = profile.drivers();
            assertThat(drivers.size(), greaterThanOrEqualTo(2));
            List<OperatorStatus> enrichOperators = drivers.stream()
                .flatMap(d -> d.operators().stream())
                .filter(status -> status.operator().startsWith("EnrichOperator"))
                .toList();
            assertThat(enrichOperators, not(emptyList()));
        }
    }

    /**
     * Some enrich queries that could fail without the PushDownEnrich rule.
     */
    public void testForPushDownEnrichRule() {
        {
            String query = String.format(Locale.ROOT, """
                FROM listens*
                | eval x = TO_STR(song_id)
                | SORT x
                | %s
                | SORT song_id
                | LIMIT 5
                | STATS listens = count(*) BY title
                | SORT listens DESC
                | KEEP title, listens
                """, enrichSongCommand());
            try (EsqlQueryResponse resp = run(query)) {
                assertThat(EsqlTestUtils.getValuesList(resp), equalTo(List.of(List.of("Hotel California", 3L), List.of("In The End", 2L))));
            }
        }
        {
            String query = String.format(Locale.ROOT, """
                FROM listens*
                | eval x = TO_STR(song_id)
                | SORT x
                | KEEP x, song_id
                | %s
                | SORT song_id
                | KEEP title, song_id
                | LIMIT 1
                """, enrichSongCommand());
            try (EsqlQueryResponse resp = run(query)) {
                assertThat(EsqlTestUtils.getValuesList(resp), equalTo(List.of(List.of("Hotel California", "s1"))));
            }
        }
    }

    /**
     * To enable enrich lookup using ordinals
     */
    public void testManyDocuments() {
        int numDocs = between(200, 2000);
        var artists = Map.of("s1", "Eagles", "s2", "Linkin Park", "s3", "Linkin Park", "s4", "Disturbed");
        client().admin()
            .indices()
            .prepareCreate("many_docs")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1))
            .setMapping("song_id", "type=keyword")
            .get();
        Map<String, Long> songs = new HashMap<>();
        for (int i = 0; i < numDocs; i++) {
            String song = randomFrom(artists.keySet());
            client().prepareIndex("many_docs").setSource("song_id", song).get();
            songs.merge(song, 1L, Long::sum);
        }
        client().admin().indices().prepareRefresh("many_docs").get();
        try (EsqlQueryResponse resp = run("FROM many_docs | ENRICH songs | STATS count(*) BY artist")) {
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp);
            Map<String, Long> actual = new HashMap<>();
            for (List<Object> value : values) {
                actual.merge((String) value.get(1), (Long) value.get(0), Long::sum);
            }
            Map<String, Long> expected = new HashMap<>();
            for (Map.Entry<String, Long> e : songs.entrySet()) {
                expected.merge(artists.get(e.getKey()), e.getValue(), Long::sum);
            }
            assertThat(actual, equalTo(expected));
        }
    }

    public void testRejection() {
        for (var ts : internalCluster().getInstances(TransportService.class)) {
            ((MockTransportService) ts).addRequestHandlingBehavior(EnrichLookupService.LOOKUP_ACTION_NAME, (h, r, channel, t) -> {
                EsRejectedExecutionException ex = new EsRejectedExecutionException("test", false);
                channel.sendResponse(new RemoteTransportException("test", ex));
            });
        }
        try {
            String query = "FROM listen* | " + enrichSongCommand();
            Exception error = expectThrows(Exception.class, () -> run(query).close());
            assertThat(ExceptionsHelper.status(error), equalTo(RestStatus.TOO_MANY_REQUESTS));
        } finally {
            for (var ts : internalCluster().getInstances(TransportService.class)) {
                ((MockTransportService) ts).clearAllRules();
            }
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
