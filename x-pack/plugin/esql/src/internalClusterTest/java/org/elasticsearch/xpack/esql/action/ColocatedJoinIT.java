/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.indices.ResizeIndexTestUtils;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.shrink.TransportResizeAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.not;

/**
 * {@code FROM events | JOIN sessions ON session_id} between two indices partitioned the same way on the key.
 * <p>
 * Layout: three data nodes, three primaries per index, one shard per node, no replicas. Both indices are cloned from an
 * empty seed index, which forces shard {@code i} of each clone onto the node holding seed shard {@code i}, so the two
 * indices have the same shard-to-node assignment. The seed sets {@code index.routing_field: session_id}, which the clones
 * inherit, so the server routes every document by its key: a key lives in the same shard number on both sides and the join
 * can run per shard pair on the data nodes. Documents are indexed without explicit routing.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 3, numClientNodes = 0, supportsDedicatedMasters = false)
public class ColocatedJoinIT extends AbstractEsqlIntegTestCase {

    private static final int SHARDS = 3;
    /**
     * Every document needs a {@code _routing}: the join requires it on both sides.
     */
    private static final String ROUTED_MAPPING = """
        {
          "_routing": { "required": true },
          "properties": { "session_id": { "type": "keyword" } }
        }
        """;

    @Before
    public void createColocatedIndices() {
        assumeTrue("co-located join is snapshot only", EsqlCapabilities.Cap.COLOCATED_JOIN_V1.isEnabled());
        createColocatedPair("session_id", ROUTED_MAPPING, "events", "sessions");
        assertAcked(
            indicesAdmin().preparePutMapping("events")
                .setSource(
                    "@timestamp",
                    "type=date",
                    "seq",
                    "type=long",
                    "event_type",
                    "type=keyword",
                    "page",
                    "type=keyword",
                    "duration_ms",
                    "type=long"
                )
        );
        assertAcked(
            indicesAdmin().preparePutMapping("sessions")
                .setSource(
                    "started_at",
                    "type=date",
                    "user_id",
                    "type=keyword",
                    "device",
                    "type=keyword",
                    "country",
                    "type=keyword",
                    "referrer",
                    "type=keyword",
                    "plan",
                    "type=keyword"
                )
        );

        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        event(bulk, "s-100", 1, "view", "/home", 120);
        event(bulk, "s-100", 2, "click", "/pricing", 30);
        event(bulk, "s-100", 3, "purchase", "/checkout", 900);
        event(bulk, "s-200", 1, "view", "/home", 80);
        event(bulk, "s-200", 2, "click", "/docs", 45);
        event(bulk, "s-300", 1, "view", "/home", 60);
        event(bulk, "s-300", 2, "purchase", "/checkout", 700);
        event(bulk, "s-400", 1, "view", "/home", 50);
        event(bulk, "s-400", 2, "search", "/search", 210);
        event(bulk, "s-400", 3, "click", "/docs", 40);
        event(bulk, "s-500", 1, "view", "/home", 95); // no session row
        event(bulk, "s-600", 1, "view", "/pricing", 70);
        event(bulk, "s-600", 2, "click", "/checkout", 35);
        event(bulk, "s-600", 3, "purchase", "/checkout", 1200);
        session(bulk, "s-100", "u-1", "desktop", "DE", "google", "pro");
        session(bulk, "s-200", "u-2", "mobile", "US", "direct", "free");
        session(bulk, "s-300", "u-3", "mobile", "DE", "twitter", "free");
        session(bulk, "s-400", "u-1", "desktop", "DE", "google", "pro");
        session(bulk, "s-600", "u-2", "tablet", "FR", "direct", "pro");
        session(bulk, "s-700", "u-5", "desktop", "US", "google", "free"); // no events
        var bulkResponse = bulk.get();
        assertFalse(bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
    }

    /**
     * The events again, in an index that knows nothing about sessions: two shards, routed by {@code _id}, placed wherever
     * the allocator likes. Only the right index has to be routed by the join key, so it joins {@code sessions} all the same,
     * looking up each event on the {@code sessions} shard that holds its session, on this node or another.
     */
    private void createUnroutedEvents() {
        assertAcked(
            prepareCreate("events_unrouted").setSettings(
                Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0)
            )
                .setMapping(
                    "session_id",
                    "type=keyword",
                    "@timestamp",
                    "type=date",
                    "seq",
                    "type=long",
                    "event_type",
                    "type=keyword",
                    "page",
                    "type=keyword",
                    "duration_ms",
                    "type=long"
                )
        );
        ensureGreen("events_unrouted");
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        assertResponse(client().prepareSearch("events").setSize(100), events -> {
            for (var hit : events.getHits()) {
                bulk.add(prepareIndex("events_unrouted").setSource(hit.getSourceAsMap()));
            }
        });
        var bulkResponse = bulk.get();
        assertFalse(bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
    }

    /**
     * Creates an empty write-blocked seed index routed by {@code routingField}, one shard per node, and clones it into
     * {@code left} and {@code right}, which then have the same shard-to-node assignment and route by the same field.
     */
    private void createColocatedPair(String routingField, String mapping, String left, String right) {
        String seed = left + "_seed";
        assertAcked(
            prepareCreate(seed).setSettings(
                Settings.builder()
                    .put("index.number_of_shards", SHARDS)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.total_shards_per_node", 1)
                    .put("index.routing.rebalance.enable", "none")
                    .put("index.routing_field", routingField)
                    .put("index.blocks.write", true)
            ).setMapping(mapping)
        );
        ensureGreen(seed);
        clone(seed, left);
        clone(seed, right);
        ensureGreen(left, right);
        updateIndexSettings(Settings.builder().putNull("index.blocks.write"), left, right);
    }

    private static void clone(String source, String target) {
        var request = ResizeIndexTestUtils.resizeRequest(
            ResizeType.CLONE,
            source,
            target,
            Settings.builder().put("index.number_of_replicas", 0)
        );
        assertAcked(client().execute(TransportResizeAction.TYPE, request).actionGet());
    }

    private void event(BulkRequestBuilder bulk, String session, int seq, String type, String page, int duration) {
        bulk.add(
            prepareIndex("events").setSource(
                Map.of(
                    "session_id",
                    session,
                    "@timestamp",
                    "2026-10-30T10:00:0" + seq + "Z",
                    "seq",
                    seq,
                    "event_type",
                    type,
                    "page",
                    page,
                    "duration_ms",
                    duration
                )
            )
        );
    }

    private void session(BulkRequestBuilder bulk, String session, String user, String device, String country, String ref, String plan) {
        bulk.add(
            prepareIndex("sessions").setId(session)
                .setSource(
                    Map.of(
                        "session_id",
                        session,
                        "started_at",
                        "2026-10-30T10:00:00Z",
                        "user_id",
                        user,
                        "device",
                        device,
                        "country",
                        country,
                        "referrer",
                        ref,
                        "plan",
                        plan
                    )
                )
        );
    }

    /**
     * Runs every query with both the streaming and the per-page lookup.
     */
    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(
            Settings.builder().put(randomPragmas().settings()).put(QueryPragmas.COLOCATED_JOIN_STREAMING.getKey(), randomBoolean()).build()
        );
    }

    private static EsqlQueryRequest profiled(String query, boolean streaming) {
        EsqlQueryRequest request = syncEsqlQueryRequest(query);
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.COLOCATED_JOIN_STREAMING.getKey(), streaming).build()));
        request.profile(true);
        return request;
    }

    private static List<DriverProfile> dataDrivers(EsqlQueryResponse resp) {
        return resp.profile().drivers().stream().filter(d -> d.description().equals("data")).toList();
    }

    private static List<String> operators(DriverProfile driver) {
        return driver.operators().stream().map(OperatorStatus::operator).toList();
    }

    private static String lookupOperator(List<String> operators, boolean streaming) {
        String name = streaming ? "StreamingLookupOperator[" : "LookupOperator[";
        String lookup = operators.stream().filter(o -> o.startsWith(name)).findFirst().orElse(null);
        assertNotNull("no " + name + "] in " + operators, lookup);
        return lookup;
    }

    /**
     * The rows of {@link #JOIN_QUERY} over the test data, for any left index holding the events.
     */
    private static final List<Iterable<Object>> JOINED = List.of(
        List.of("s-100", 1L, "view", "DE", "pro"),
        List.of("s-100", 2L, "click", "DE", "pro"),
        List.of("s-100", 3L, "purchase", "DE", "pro"),
        List.of("s-200", 1L, "view", "US", "free"),
        List.of("s-200", 2L, "click", "US", "free"),
        List.of("s-300", 1L, "view", "DE", "free"),
        List.of("s-300", 2L, "purchase", "DE", "free"),
        List.of("s-400", 1L, "view", "DE", "pro"),
        List.of("s-400", 2L, "search", "DE", "pro"),
        List.of("s-400", 3L, "click", "DE", "pro"),
        List.of("s-600", 1L, "view", "FR", "pro"),
        List.of("s-600", 2L, "click", "FR", "pro"),
        List.of("s-600", 3L, "purchase", "FR", "pro")
    );

    private static final String JOIN_QUERY = """
        FROM %s
        | JOIN %s ON session_id
        | KEEP session_id, seq, event_type, country, plan
        | SORT session_id, seq
        """;

    private void assertJoined(String left, String right) {
        try (var resp = run(String.format(Locale.ROOT, JOIN_QUERY, left, right))) {
            assertColumnNames(resp.columns(), List.of("session_id", "seq", "event_type", "country", "plan"));
            assertValues(resp.values(), JOINED);
        }
    }

    public void testJoin() {
        assertJoined("events", "sessions");
    }

    public void testJoinOutputColumns() {
        try (var resp = run("FROM events | JOIN sessions ON session_id | LIMIT 1")) {
            // left columns, then right columns minus the join key
            assertColumnNames(
                resp.columns(),
                List.of(
                    "@timestamp",
                    "duration_ms",
                    "event_type",
                    "page",
                    "seq",
                    "session_id",
                    "country",
                    "device",
                    "plan",
                    "referrer",
                    "started_at",
                    "user_id"
                )
            );
        }
    }

    public void testWhereAfterJoin() {
        try (var resp = run("""
            FROM events
            | JOIN sessions ON session_id
            | WHERE country == "DE" AND seq > 1
            | KEEP session_id, seq, country
            | SORT session_id, seq
            """)) {
            assertValues(
                resp.values(),
                List.of(
                    List.of("s-100", 2L, "DE"),
                    List.of("s-100", 3L, "DE"),
                    List.of("s-300", 2L, "DE"),
                    List.of("s-400", 2L, "DE"),
                    List.of("s-400", 3L, "DE")
                )
            );
        }
    }

    public void testWhereBeforeJoin() {
        try (var resp = run("""
            FROM events
            | WHERE event_type == "purchase"
            | JOIN sessions ON session_id
            | KEEP session_id, country, plan
            | SORT session_id
            """)) {
            assertValues(
                resp.values(),
                List.of(List.of("s-100", "DE", "pro"), List.of("s-300", "DE", "free"), List.of("s-600", "FR", "pro"))
            );
        }
    }

    public void testStatsAfterJoin() {
        try (var resp = run("""
            FROM events
            | JOIN sessions ON session_id
            | STATS n = COUNT(*), spent = SUM(duration_ms) BY country, plan
            | SORT country, plan
            """)) {
            assertColumnNames(resp.columns(), List.of("n", "spent", "country", "plan"));
            assertValues(
                resp.values(),
                List.of(
                    List.of(2L, 760L, "DE", "free"),
                    List.of(6L, 1350L, "DE", "pro"),
                    List.of(3L, 1305L, "FR", "pro"),
                    List.of(2L, 125L, "US", "free")
                )
            );
        }
    }

    /**
     * Left rows without a session row ({@code s-500}) are dropped, also when a whole input page has no match.
     */
    public void testNoMatches() {
        try (var resp = run("FROM events | WHERE session_id == \"s-500\" | JOIN sessions ON session_id | KEEP session_id, seq")) {
            assertColumnNames(resp.columns(), List.of("session_id", "seq"));
            assertValues(resp.values(), List.of());
        }
    }

    /**
     * The join runs on every data node as a local lookup against the partner shard: each data driver carries the
     * co-located lookup operator, and only aggregates leave the node. The operator emits the inner join itself, so no
     * filter follows it.
     */
    public void testRunsOnEachDataNode() {
        for (boolean streaming : new boolean[] { false, true }) {
            EsqlQueryRequest request = profiled("FROM events | JOIN sessions ON session_id | STATS n = COUNT(*) BY country", streaming);
            try (var resp = run(request)) {
                List<DriverProfile> dataDrivers = dataDrivers(resp);
                Set<String> nodes = dataDrivers.stream().map(DriverProfile::nodeName).collect(Collectors.toSet());
                assertThat("one data driver per node, one shard pair each", nodes, hasSize(SHARDS));
                for (DriverProfile driver : dataDrivers) {
                    List<String> operators = operators(driver);
                    String lookup = lookupOperator(operators, streaming);
                    assertThat(lookup, containsString("index=sessions"));
                    assertThat(lookup, containsString("colocated=true"));
                    assertThat(lookup, containsString("join=inner"));
                    // only the right column the query uses is loaded
                    assertThat(lookup, matchesRegex("(?s).* load_fields=\\[country\\{f}#\\d+] .*"));
                    if (streaming) {
                        // an aligned left shard only needs its partner shard
                        assertThat(lookup, matchesRegex("(?s).* shards=\\[\\d] *]"));
                    }
                    assertThat(operators.stream().filter(o -> o.startsWith("FilterOperator")).toList(), empty());
                    assertThat(operators.get(operators.size() - 1), containsString("ExchangeSinkOperator"));
                }
            }
        }
    }

    /**
     * With a {@code SORT} the left fields are read after the node-level top-n, for the surviving rows only, as for a single index.
     * The right index must not get the left's {@code _doc} column, or every left field is read before the sort.
     */
    public void testSortReadsLeftFieldsAfterTopN() {
        for (boolean streaming : new boolean[] { false, true }) {
            EsqlQueryRequest request = profiled("FROM events | JOIN sessions ON session_id | SORT plan | LIMIT 3", streaming);
            try (var resp = run(request)) {
                assertColumnNames(
                    resp.columns(),
                    List.of(
                        "@timestamp",
                        "duration_ms",
                        "event_type",
                        "page",
                        "seq",
                        "session_id",
                        "country",
                        "device",
                        "plan",
                        "referrer",
                        "started_at",
                        "user_id"
                    )
                );
                for (DriverProfile driver : dataDrivers(resp)) {
                    List<String> readers = operators(driver).stream().filter(o -> o.startsWith("ValuesSourceReaderOperator")).toList();
                    // the join key only: the other left fields are read on the node_reduce driver, after its top-n
                    assertThat(readers.toString(), readers, everyItem(equalTo("ValuesSourceReaderOperator[fields = [session_id]]")));
                }
            }
        }
    }

    /**
     * The left index is not routed by the join key, has another shard count and is placed anywhere: each event is looked up
     * on the {@code sessions} shard that holds its session.
     */
    public void testUnroutedLeft() {
        createUnroutedEvents();
        assertJoined("events_unrouted", "sessions");
    }

    public void testUnroutedLeftWhereAndStats() {
        createUnroutedEvents();
        try (var resp = run("""
            FROM events_unrouted
            | WHERE event_type != "view"
            | JOIN sessions ON session_id
            | WHERE plan == "pro"
            | STATS n = COUNT(*), total = SUM(duration_ms) BY country
            | SORT country
            """)) {
            assertValues(resp.values(), List.of(List.of(4L, 1180L, "DE"), List.of(2L, 1235L, "FR")));
        }
    }

    /**
     * Events without a session match nothing: they are left out of the lookups, alone or mixed with keyed rows in a page.
     */
    public void testUnroutedLeftNullKeys() {
        createUnroutedEvents();
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.add(prepareIndex("events_unrouted").setSource("seq", 1, "event_type", "view"));
        bulk.add(prepareIndex("events_unrouted").setSource("seq", 2, "event_type", "click"));
        assertFalse(bulk.get().hasFailures());
        try (var resp = run("FROM events_unrouted | JOIN sessions ON session_id | STATS n = COUNT(*)")) {
            assertValues(resp.values(), List.of(List.of(13L)));
        }
        try (var resp = run("FROM events_unrouted | WHERE session_id IS NULL | JOIN sessions ON session_id | STATS n = COUNT(*)")) {
            assertValues(resp.values(), List.of(List.of(0L)));
        }
    }

    /**
     * A left index pattern mixing an aligned index and an unrouted one: pages of {@code events} go to their partner shard as
     * a whole, pages of {@code events_unrouted} are split by key. Every event is joined once per index.
     */
    public void testAlignedAndUnroutedLeftIndices() {
        createUnroutedEvents();
        try (var resp = run("FROM events, events_unrouted | JOIN sessions ON session_id | STATS n = COUNT(*) BY plan | SORT plan")) {
            assertValues(resp.values(), List.of(List.of(8L, "free"), List.of(18L, "pro")));
        }
    }

    /**
     * The join on the unrouted index runs on the nodes of its shards, with the co-located lookup operator.
     */
    public void testUnroutedLeftProfile() {
        createUnroutedEvents();
        for (boolean streaming : new boolean[] { false, true }) {
            EsqlQueryRequest request = profiled("FROM events_unrouted | JOIN sessions ON session_id | STATS n = COUNT(*)", streaming);
            try (var resp = run(request)) {
                assertValues(resp.values(), List.of(List.of(13L)));
                List<DriverProfile> dataDrivers = dataDrivers(resp);
                assertThat(dataDrivers, not(empty()));
                for (DriverProfile driver : dataDrivers) {
                    String lookup = lookupOperator(operators(driver), streaming);
                    assertThat(lookup, containsString("colocated=true"));
                    assertThat(lookup, containsString("join=inner"));
                    // COUNT(*) uses no right column
                    assertThat(lookup, containsString("load_fields=[]"));
                }
            }
        }
    }

    /**
     * A one-shard {@code sessions}: every event of the three-shard {@code events} is looked up on that one shard.
     */
    public void testRightWithOtherShardCount() {
        assertAcked(
            prepareCreate("sessions_one").setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing_field", "session_id")
            ).setMapping(ROUTED_MAPPING)
        );
        assertAcked(indicesAdmin().preparePutMapping("sessions_one").setSource("country", "type=keyword", "plan", "type=keyword"));
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        assertResponse(client().prepareSearch("sessions").setSize(100), sessions -> {
            for (var hit : sessions.getHits()) {
                Map<String, Object> source = hit.getSourceAsMap();
                bulk.add(
                    prepareIndex("sessions_one").setSource(
                        Map.of("session_id", source.get("session_id"), "country", source.get("country"), "plan", source.get("plan"))
                    )
                );
            }
        });
        assertFalse(bulk.get().hasFailures());
        assertJoined("events", "sessions_one");
    }

    /**
     * The right index must be routed by the join key: {@code events_unrouted} is not, so it can't be the right side.
     */
    public void testRejectsUnroutedRight() {
        createUnroutedEvents();
        var e = expectThrows(VerificationException.class, () -> run("FROM sessions | JOIN events_unrouted ON session_id").close());
        assertThat(
            e.getMessage(),
            containsString("JOIN requires [events_unrouted] to set [index.routing_field] to the join key [session_id]")
        );
        assertThat(e.getMessage(), containsString("JOIN requires [events_unrouted] to have [_routing] required in its mapping"));
    }

    /**
     * The join runs inside the data-node fragment, so a {@code LIMIT} or {@code STATS} below it would run once per node.
     */
    public void testRejectsPipelineBreakerBeforeJoin() {
        var limit = expectThrows(VerificationException.class, () -> run("FROM events | LIMIT 5 | JOIN sessions ON session_id").close());
        assertThat(limit.getMessage(), containsString("JOIN can't be executed after [LIMIT 5] on the left side"));
        var stats = expectThrows(
            VerificationException.class,
            () -> run("FROM events | STATS n = COUNT(*) BY session_id | JOIN sessions ON session_id").close()
        );
        assertThat(stats.getMessage(), containsString("JOIN can't be executed after [STATS n = COUNT(*) BY session_id] on the left side"));
    }

    /**
     * Moves {@code sessions[1]} next to {@code events[0]}: shard 1 of the two indices no longer shares a node, and the
     * events of shard 1 are looked up on the other node.
     */
    public void testMisplacedShards() {
        updateIndexSettings(Settings.builder().put("index.routing.allocation.total_shards_per_node", -1), "sessions");
        var state = clusterService().state();
        String from = state.nodes().get(state.routingTable().index("sessions").shard(1).primaryShard().currentNodeId()).getName();
        String to = state.nodes().get(state.routingTable().index("events").shard(0).primaryShard().currentNodeId()).getName();
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand("sessions", 1, from, to));
        ensureGreen("sessions");
        assertJoined("events", "sessions");
    }

    /**
     * A {@code long} key: documents spell it as a number or a string, and both indices route its canonical form, so equal
     * keys meet on the same shard.
     */
    public void testLongKey() {
        createColocatedPair("customer_id", """
            {"_routing": {"required": true}, "properties": {"customer_id": {"type": "long"}}}""", "orders", "customers");
        assertAcked(indicesAdmin().preparePutMapping("orders").setSource("amount", "type=long"));
        assertAcked(indicesAdmin().preparePutMapping("customers").setSource("tier", "type=keyword"));
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.add(prepareIndex("orders").setSource("{\"customer_id\": 1, \"amount\": 10}", XContentType.JSON));
        bulk.add(prepareIndex("orders").setSource("{\"customer_id\": \"1\", \"amount\": 20}", XContentType.JSON));
        bulk.add(prepareIndex("orders").setSource("{\"customer_id\": 2, \"amount\": 5}", XContentType.JSON));
        bulk.add(prepareIndex("orders").setSource("{\"customer_id\": 3, \"amount\": 7}", XContentType.JSON));
        bulk.add(prepareIndex("customers").setSource("{\"customer_id\": \"1\", \"tier\": \"gold\"}", XContentType.JSON));
        bulk.add(prepareIndex("customers").setSource("{\"customer_id\": 2, \"tier\": \"silver\"}", XContentType.JSON));
        bulk.add(prepareIndex("customers").setSource("{\"customer_id\": 4, \"tier\": \"bronze\"}", XContentType.JSON));
        var bulkResponse = bulk.get();
        assertFalse(bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());

        try (var resp = run("FROM orders | JOIN customers ON customer_id | STATS total = SUM(amount) BY tier | SORT tier")) {
            assertValues(resp.values(), List.of(List.of(30L, "gold"), List.of(5L, "silver")));
        }
    }

    /**
     * A right index that does not declare its routing field: the server does not guarantee where a key lives, so the join
     * is rejected even when shard counts match.
     */
    public void testRejectsIndexWithoutRoutingField() {
        assertAcked(
            prepareCreate("sessions_plain").setSettings(
                Settings.builder().put("index.number_of_shards", SHARDS).put("index.number_of_replicas", 0)
            ).setMapping(ROUTED_MAPPING)
        );
        ensureGreen("sessions_plain");
        var e = expectThrows(VerificationException.class, () -> run("FROM events | JOIN sessions_plain ON session_id").close());
        assertThat(
            e.getMessage(),
            containsString("JOIN requires [sessions_plain] to set [index.routing_field] to the join key [session_id]")
        );
    }

    public void testRejectsUnknownKey() {
        var left = expectThrows(VerificationException.class, () -> run("FROM events | JOIN sessions ON device").close());
        assertThat(left.getMessage(), containsString("Unknown column [device] in left side of join"));
        var right = expectThrows(VerificationException.class, () -> run("FROM events | JOIN sessions ON seq").close());
        assertThat(right.getMessage(), containsString("Unknown column [seq] in right side of join"));
    }

    public void testRejectsPatterns() {
        var wildcard = expectThrows(ParsingException.class, () -> run("FROM events | JOIN sess* ON session_id").close());
        assertThat(wildcard.getMessage(), containsString("* is not allowed in JOIN"));
        var twoKeys = expectThrows(ParsingException.class, () -> run("FROM events | JOIN sessions ON session_id, seq").close());
        assertThat(twoKeys.getMessage(), containsString("supports exactly one field"));
    }
}
