/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactoryBuilder;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ShardDocSortField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlPluginWithEnterpriseOrTrialLicense;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.ql.plugin.QlPlugin;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;

/**
 * Positive-sequence parity and explicit migration differences through both actual query actions, planners, searches and
 * payload retrieval on identical immutable events. Capacity observations have no relative timing assertions: cache state and test-host scheduling make
 * them unsuitable for production performance claims. Identity, completeness and resource cleanup remain strict checks.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceEqlDifferentialIT extends AbstractSequenceIntegTestCase {
    private static final String INDEX = "sequence-differential";
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389Z");
    private static final TimeValue TIMEOUT = TimeValue.timeValueMinutes(2);
    private static final List<String> TWO_STAGES = List.of("phase == \"A\"", "phase == \"B\"");
    private final AtomicReference<Measurement> measurement = new AtomicReference<>();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        assertTrue(plugins.removeIf(EsqlPlugin.class::isAssignableFrom));
        plugins.add(SequenceBlockFactoryPlugin.class);
        plugins.add(EqlPlugin.class);
        plugins.add(QlPlugin.class);
        plugins.add(SecuritySettingPlugin.class);
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "128mb")
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "memory")
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), 1.0)
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .put(
                BreakerSettings.CIRCUIT_BREAKER_LIMIT_SETTING.getConcreteSettingForNamespace(EqlPlugin.CIRCUIT_BREAKER_NAME).getKey(),
                "128mb"
            )
            .build();
    }

    /** Supplies the setting normally registered by security while both query actions run without that plugin. */
    public static class SecuritySettingPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(XPackSettings.SECURITY_ENABLED);
        }
    }

    /** Uses the same real compute blocks and request breaker as native execution while tracking block ownership. */
    public static class SequenceBlockFactoryPlugin extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        protected BlockFactoryProvider blockFactoryProvider(BlockFactoryBuilder builder) {
            return new BlockFactoryProvider(new MockBlockFactory(builder));
        }
    }

    public void testLatestPredecessorAndConsumption() throws Exception {
        indexEvents(
            List.of(
                event("A1", "A", "shared", 0),
                event("A2", "A", "shared", 1),
                event("B3", "B", "shared", 2),
                event("B4", "B", "shared", 3),
                event("A5", "A", "shared", 4),
                event("B6", "B", "shared", 5)
            )
        );
        assertParity("key", false, TWO_STAGES, "30s", 100, 2, List.of(match("shared", "A2", "B3"), match("shared", "A5", "B6")));
    }

    /** Positive keyword membership needs MV_CONTAINS in ES|QL, including inside a sequence stage. */
    public void testPositiveMultivaluePredicateMigration() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(indexSettings(1, 0))
                .setMapping(
                    "@timestamp",
                    "type=date_nanos",
                    "key",
                    "type=keyword",
                    "phase",
                    "type=keyword",
                    "process.args",
                    "type=keyword",
                    "event.category",
                    "type=keyword",
                    "event.sequence",
                    "type=long"
                )
        );
        ensureGreen(INDEX);
        List<String> cases = List.of("scalar", "array_match", "array_miss", "missing", "explicit_null");
        var bulk = client().prepareBulk();
        for (int i = 0; i < cases.size(); i++) {
            String key = cases.get(i);
            for (int stage = 0; stage < 2; stage++) {
                String phase = stage == 0 ? "A" : "B";
                Map<String, Object> source = new HashMap<>();
                source.put("@timestamp", BASE.plusSeconds(i * 10L + stage).toString());
                source.put("key", key);
                source.put("phase", phase);
                source.put("event", Map.of("category", "process", "sequence", i * 2 + stage));
                if (stage == 0 && key.equals("missing") == false) {
                    Map<String, Object> process = new HashMap<>();
                    process.put("args", switch (key) {
                        case "scalar" -> "bar";
                        case "array_match" -> List.of("foo", "bar");
                        case "array_miss" -> List.of("foo", "baz");
                        case "explicit_null" -> null;
                        default -> throw new AssertionError(key);
                    });
                    source.put("process", process);
                }
                bulk.add(prepareIndex(INDEX).setId(phase + "-" + key).setSource(source));
            }
        }
        assertFalse(bulk.get().hasFailures());
        refresh(INDEX);

        String equality = "phase == \"A\" and process.args == \"bar\"";
        try (
            var response = client().execute(
                EsqlQueryAction.INSTANCE,
                syncEsqlQueryRequest("FROM " + INDEX + " METADATA _id | WHERE " + equality + " | KEEP _id | LIMIT 100")
            ).actionGet(TIMEOUT)
        ) {
            assertFalse(response.isPartial());
            assertEquals(List.of(List.of("A-scalar")), getValuesList(response));
        }

        List<String> equalityStages = List.of(equality, "phase == \"B\"");
        List<Match> scalarOnly = List.of(match("scalar", "A-scalar", "B-scalar"));
        List<Match> membership = List.of(match("scalar", "A-scalar", "B-scalar"), match("array_match", "A-array_match", "B-array_match"));
        assertEquals(scalarOnly, nativeRun("key", false, equalityStages, "30s", 100).matches);
        assertEquals(membership, eql("key", equalityStages, "30s", 100, 2).matches);
        assertEquals(
            membership,
            nativeRun("key", false, List.of("phase == \"A\" AND MV_CONTAINS(process.args, \"bar\")", "phase == \"B\""), "30s", 100).matches
        );
        // Predicate pushdown can change whether a multivalue warning is emitted. Match identities are the contract here.
    }

    public void testNanosecondOrderingAndInclusiveThreeStageTotalSpan() throws Exception {
        indexEvents(
            List.of(
                event("A-nano", "A", "nano", 1),
                event("B-nano", "B", "nano", 2),
                event("C-nano", "C", "nano", 3),
                event("A-boundary", "A", "boundary", 100),
                event("B-boundary", "B", "boundary", TimeUnit.SECONDS.toNanos(20) + 100),
                event("C-boundary", "C", "boundary", TimeUnit.SECONDS.toNanos(30) + 100),
                event("A-outside", "A", "outside", 200),
                event("B-outside", "B", "outside", TimeUnit.SECONDS.toNanos(20) + 200),
                event("C-outside", "C", "outside", TimeUnit.SECONDS.toNanos(30) + 201)
            )
        );
        assertParity(
            "key",
            false,
            List.of("phase == \"A\"", "phase == \"B\"", "phase == \"C\""),
            "30s",
            100,
            2,
            List.of(match("nano", "A-nano", "B-nano", "C-nano"), match("boundary", "A-boundary", "B-boundary", "C-boundary"))
        );
    }

    /** The selected latest starts and consumed completions agree when the two engines share a verified tie order. */
    public void testControlledEqualTimestamps() throws Exception {
        List<Event> events = List.of(
            event("A1", "A", "shared", 0),
            event("A2", "A", "shared", 0),
            event("B3", "B", "shared", 0),
            event("B4", "B", "shared", 0),
            event("A5", "A", "shared", 0),
            event("B6", "B", "shared", 0)
        );
        indexEvents(events, "date_nanos", true);
        assertControlledTieOrder(events);
        assertParity("key", false, TWO_STAGES, "30s", 100, 2, List.of(match("shared", "A2", "B3"), match("shared", "A5", "B6")));
    }

    /** Equal millisecond timestamps still order distinct events by the immutable PIT ordinal. */
    public void testControlledSameTimestampContractsForDate() throws Exception {
        assertControlledSameTimestampContracts("date");
    }

    /** Nanosecond mappings retain the same strict ordinal and key-partition contract when timestamps tie. */
    public void testControlledSameTimestampContractsForDateNanos() throws Exception {
        assertControlledSameTimestampContracts("date_nanos");
    }

    private void assertControlledSameTimestampContracts(String timestampType) throws Exception {
        List<Event> events = List.of(
            event("A-positive", "A", "positive", 0),
            event("B-positive", "B", "positive", 0),
            event("B-reverse", "B", "reverse", 0),
            event("A-reverse", "A", "reverse", 0),
            event("A-first", "A", "first", 0),
            event("B-second", "B", "second", 0),
            event("A-second", "A", "second", 0),
            event("B-first", "B", "first", 0),
            event("both-self", "both", "self", 0),
            event("A-adjacent", "A", "adjacent", 0),
            event("both-adjacent", "both", "adjacent", 0),
            event("B-adjacent", "B", "adjacent", 0)
        );
        indexEvents(events, timestampType, true);
        assertControlledTieOrder(events);
        // At one full ordinal, stage 1 runs before stage 0. A is consumed once by both, then the distinct
        // both event starts a new prefix consumed once by B. No prefix survives its completed match.
        assertParity(
            "key",
            false,
            List.of("phase in (\"A\", \"both\")", "phase in (\"B\", \"both\")"),
            "30s",
            100,
            2,
            List.of(
                match("positive", "A-positive", "B-positive"),
                match("first", "A-first", "B-first"),
                match("adjacent", "A-adjacent", "both-adjacent"),
                match("adjacent", "both-adjacent", "B-adjacent")
            )
        );
    }

    public void testOneEventEligibleForSeveralStages() throws Exception {
        indexEvents(List.of(event("A", "A", "shared", 0), event("both", "both", "shared", 1), event("B", "B", "shared", 2)));
        assertParity(
            "key",
            false,
            List.of("phase in (\"A\", \"both\")", "phase in (\"B\", \"both\")"),
            "30s",
            100,
            2,
            List.of(match("shared", "A", "both"), match("shared", "both", "B"))
        );
    }

    public void testLatestPredecessorAcrossSeveralSourcePages() throws Exception {
        var events = new ArrayList<Event>();
        for (int event = 0; event < 1105; event++) {
            events.add(event("A" + event, "A", "shared", event));
        }
        events.add(event("B", "B", "shared", 1106));
        indexEvents(events);
        assertParity("key", false, TWO_STAGES, "30s", 100, 37, List.of(match("shared", "A1104", "B")));
    }

    public void testExpandedKeysAndUnsortedLimit() throws Exception {
        indexEvents(List.of(new Event("A", "A", "unused", 0, List.of("b", "aa")), new Event("B", "B", "unused", 1, List.of("b", "aa"))));
        List<Match> expected = List.of(match("aa", "A", "B"), match("b", "A", "B"));
        assertParity("labels", true, TWO_STAGES, "30s", 2, 2, expected);
        // EQL stops accepting expansions at its head limit, then sorts the accepted completions. The fields API's
        // multivalue enumeration therefore decides which tied key survives; the unlimited sorted result is not a prefix
        // guarantee. Compare membership and identities at this boundary, and use explicit ES|QL SORT to choose a key.
        Run eqlLimited = eql("labels", TWO_STAGES, "30s", 1, 2);
        Run nativeLimited = nativeRun("labels", true, TWO_STAGES, "30s", 1);
        for (Run limited : List.of(eqlLimited, nativeLimited)) {
            assertEquals(1, limited.matches.size());
            assertTrue(expected.contains(limited.matches.getFirst()));
        }
        assertEquals(List.of(expected.getFirst()), nativeRun("labels", true, TWO_STAGES, "30s", 1, true).matches);
        assertReleased();
    }

    public void testRepresentativeCapacityWorkloadsAgainstEql() throws Exception {
        for (String workload : List.of("sparse", "many_live", "many_complete")) {
            var events = new ArrayList<Event>();
            var expected = new ArrayList<Match>();
            switch (workload) {
                case "sparse" -> {
                    for (int event = 0; event < 10_000; event++) {
                        events.add(event("noise" + event, "noise", "noise", event));
                    }
                    for (int event = 0; event < 300; event++) {
                        events.add(event("A" + event, "A", "shared", event));
                    }
                    events.add(event("B", "B", "shared", 301));
                    expected.add(match("shared", "A299", "B"));
                }
                case "many_live" -> {
                    for (int event = 0; event < 1500; event++) {
                        events.add(event("A" + event, "A", "key" + event, event));
                    }
                    // A real nonmatching terminal causes both engines to search the terminal stage.
                    events.add(event("B-unmatched", "B", "absent", 1501));
                }
                case "many_complete" -> {
                    for (int event = 0; event < 1025; event++) {
                        String key = "key" + event;
                        events.add(event("A" + event, "A", key, event));
                        events.add(event("B" + event, "B", key, 2000L + event));
                        expected.add(match(key, "A" + event, "B" + event));
                    }
                }
                default -> throw new IllegalArgumentException("unexpected workload [" + workload + "]");
            }
            indexEvents(events);
            installMeasurementHooks();
            try {
                Run eql;
                Run nativeRun;
                if (randomBoolean()) {
                    eql = eql("key", TWO_STAGES, "30s", 1500, 256);
                    nativeRun = nativeRun("key", false, TWO_STAGES, "30s", 1500);
                } else {
                    nativeRun = nativeRun("key", false, TWO_STAGES, "30s", 1500);
                    eql = eql("key", TWO_STAGES, "30s", 1500, 256);
                }
                assertEquals(expected, eql.matches);
                assertEquals(eql.matches, nativeRun.matches);
                logger.debug(
                    "sequence capacity [{}]: documents [{}], matches [{}]; EQL wall [{}]ms, shard queries [{}], sampled sequence breaker "
                        + "[{}] bytes; native wall [{}]ms, shard queries [{}], sampled request breaker [{}] bytes",
                    workload,
                    events.size(),
                    expected.size(),
                    TimeUnit.NANOSECONDS.toMillis(eql.elapsedNanos),
                    eql.shardQueries,
                    eql.sampledEqlBytes,
                    TimeUnit.NANOSECONDS.toMillis(nativeRun.elapsedNanos),
                    nativeRun.shardQueries,
                    nativeRun.sampledRequestBytes
                );
                assertReleased();
            } finally {
                measurement.set(null);
                clearMeasurementHooks();
                assertAcked(indicesAdmin().prepareDelete(INDEX));
            }
        }
    }

    private void assertParity(
        String key,
        boolean expand,
        List<String> predicates,
        String maxspan,
        int limit,
        int eqlFetchSize,
        List<Match> expected
    ) throws Exception {
        Run eql = eql(key, predicates, maxspan, limit, eqlFetchSize);
        Run nativeRun = nativeRun(key, expand, predicates, maxspan, limit);
        assertEquals(expected, eql.matches);
        assertEquals(eql.matches, nativeRun.matches);
        assertReleased();
    }

    private Run eql(String key, List<String> predicates, String maxspan, int limit, int fetchSize) {
        var observed = new Measurement();
        measurement.set(observed);
        long start = System.nanoTime();
        var request = new EqlSearchRequest().indices(INDEX)
            .query(
                "sequence by "
                    + key
                    + " with maxspan="
                    + maxspan
                    + " "
                    + predicates.stream().map(predicate -> "[any where " + predicate + "]").collect(Collectors.joining(" "))
            )
            .tiebreakerField("event.sequence")
            .resultPosition("head")
            .size(limit)
            .fetchSize(fetchSize)
            .allowPartialSearchResults(false)
            .allowPartialSequenceResults(false);
        var response = client().execute(EqlSearchAction.INSTANCE, request).actionGet(TIMEOUT);
        try {
            assertFalse(response.isTimeout());
            assertFalse(response.isPartial());
            assertEquals(0, response.shardFailures().length);
            List<Match> matches = response.hits()
                .sequences()
                .stream()
                .map(sequence -> new Match(sequence.joinKeys(), sequence.events().stream().map(event -> event.id()).toList()))
                .toList();
            return new Run(
                matches,
                System.nanoTime() - start,
                observed.eqlQueries.get(),
                observed.requestBytes.get(),
                observed.eqlBytes.get()
            );
        } finally {
            response.decRef();
            measurement.set(null);
        }
    }

    private Run nativeRun(String key, boolean expand, List<String> predicates, String maxspan, int limit) {
        return nativeRun(key, expand, predicates, maxspan, limit, false);
    }

    private Run nativeRun(String key, boolean expand, List<String> predicates, String maxspan, int limit, boolean sortByKey) {
        var observed = new Measurement();
        measurement.set(observed);
        long start = System.nanoTime();
        String stages = predicates.stream()
            .map(
                predicate -> "(from "
                    + INDEX
                    + " metadata _id | where "
                    + predicate
                    + (expand ? " | mv_expand " + key : "")
                    + " | keep "
                    + key
                    + ", _id)"
            )
            .collect(Collectors.joining(", "));
        var output = new ArrayList<String>();
        output.add(key);
        output.add("_id");
        var memberBindings = new StringBuilder();
        for (int stage = 2; stage <= predicates.size(); stage++) {
            String alias = "member" + stage + "_id";
            output.add(alias);
            memberBindings.append(" | EVAL ").append(alias).append(" = MEMBER(").append(stage).append(", _id)");
        }
        String query = "sequence by "
            + key
            + " with {\"maxspan\":\""
            + maxspan
            + "\"} PRIMARY 1 "
            + stages
            + memberBindings
            + " | keep "
            + String.join(", ", output)
            + (sortByKey ? " | sort " + key : "")
            + " | limit "
            + limit;
        try (var response = client().execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query)).actionGet(TIMEOUT)) {
            assertFalse(response.isPartial());
            List<Match> matches = getValuesList(response).stream()
                .map(row -> new Match(List.of(row.getFirst()), row.subList(1, row.size()).stream().map(Object::toString).toList()))
                .toList();
            return new Run(
                matches,
                System.nanoTime() - start,
                observed.nativeQueries.get(),
                observed.requestBytes.get(),
                observed.eqlBytes.get()
            );
        } finally {
            measurement.set(null);
        }
    }

    private void indexEvents(List<Event> events) {
        indexEvents(events, "date_nanos", false);
    }

    private void indexEvents(List<Event> events, String timestampType, boolean controlledTieOrder) {
        Settings.Builder settings = indexSettings(1, 0);
        if (controlledTieOrder) {
            settings.put("index.sort.field", "event.sequence").put("index.sort.order", "asc");
        }
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(settings)
                .setMapping(
                    "@timestamp",
                    "type=" + timestampType,
                    "key",
                    "type=keyword",
                    "phase",
                    "type=keyword",
                    "labels",
                    "type=keyword",
                    "event.category",
                    "type=keyword",
                    "event.sequence",
                    "type=long"
                )
        );
        ensureGreen(INDEX);
        var bulk = client().prepareBulk();
        for (int sequence = 0; sequence < events.size(); sequence++) {
            Event event = events.get(sequence);
            bulk.add(
                prepareIndex(INDEX).setId(event.id)
                    .setSource(
                        Map.of(
                            "@timestamp",
                            BASE.plusNanos(event.nanos).toString(),
                            "key",
                            event.key,
                            "phase",
                            event.phase,
                            "labels",
                            event.labels,
                            "event",
                            Map.of("category", "process", "sequence", sequence)
                        )
                    )
            );
        }
        assertFalse(bulk.get().hasFailures());
        refresh(INDEX);
        if (controlledTieOrder) {
            // The index is private to this fixture. One sorted immutable segment fixes native document order
            // across the separate raw, EQL and native PITs without giving native matching an explicit tie-breaker field.
            assertAcked(indicesAdmin().prepareAddBlock(IndexMetadata.APIBlock.WRITE, INDEX));
            assertEquals(0, indicesAdmin().prepareForceMerge(INDEX).setMaxNumSegments(1).setFlush(true).get().getFailedShards());
            refresh(INDEX);
        }
    }

    /** Establish the declared full-ordinal order from raw PIT hits, independently of either matching engine. */
    private void assertControlledTieOrder(List<Event> events) {
        assertTrue(events.stream().allMatch(event -> event.nanos == 0));
        BytesReference pit = new BytesArray(
            BytesRef.deepCopyOf(
                client().execute(TransportOpenPointInTimeAction.TYPE, new OpenPointInTimeRequest(INDEX).keepAlive(TIMEOUT))
                    .actionGet(TIMEOUT)
                    .getPointInTimeId()
                    .toBytesRef()
            )
        );
        Throwable primary = null;
        try {
            var source = new SearchSourceBuilder().sort(
                new FieldSortBuilder("@timestamp").order(SortOrder.ASC).setFormat("strict_date_optional_time_nanos")
            )
                .sort(new FieldSortBuilder(ShardDocSortField.NAME).order(SortOrder.ASC))
                .docValueField("event.sequence")
                .size(events.size())
                .pointInTimeBuilder(new PointInTimeBuilder(pit).setKeepAlive(TIMEOUT));
            var response = client().prepareSearch()
                .setSource(source)
                .setPreference(null)
                .setPreFilterShardSize(Integer.MAX_VALUE)
                .setAllowPartialSearchResults(false)
                .get(TIMEOUT);
            try {
                if (response.pointInTimeId() != null) {
                    pit = new BytesArray(BytesRef.deepCopyOf(response.pointInTimeId().toBytesRef()));
                }
                assertFalse(response.isTimedOut());
                assertEquals(0, response.getFailedShards());
                var hits = response.getHits().getHits();
                assertEquals(events.size(), hits.length);
                long previous = -1;
                for (int ordinal = 0; ordinal < hits.length; ordinal++) {
                    var hit = hits[ordinal];
                    assertEquals(events.get(ordinal).id, hit.getId());
                    assertEquals(2, hit.getSortValues().length);
                    assertEquals(BASE, Instant.parse(hit.getSortValues()[0].toString()));
                    assertTrue(hit.getSortValues()[1] instanceof Long);
                    long shardDoc = (Long) hit.getSortValues()[1];
                    assertTrue("PIT document coordinates must advance even at equal timestamps", shardDoc > previous);
                    previous = shardDoc;
                    assertNotNull(hit.field("event.sequence"));
                    assertEquals(ordinal, ((Number) hit.field("event.sequence").getValue()).longValue());
                }
            } finally {
                response.decRef();
            }
        } catch (Exception | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            try {
                var closed = client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pit)).actionGet(TIMEOUT);
                assertTrue(closed.isSucceeded());
                assertEquals(1, closed.getNumFreed());
            } catch (Exception | Error cleanup) {
                if (primary != null) {
                    if (primary != cleanup) {
                        primary.addSuppressed(cleanup);
                    }
                } else {
                    throw cleanup;
                }
            }
        }
    }

    private void installMeasurementHooks() {
        for (String node : internalCluster().getNodeNames()) {
            MockTransportService.getInstance(node)
                .<ShardSearchRequest>addRequestHandlingBehavior(
                    SearchTransportService.QUERY_ACTION_NAME,
                    (handler, request, channel, task) -> {
                        Measurement current = measurement.get();
                        if (current != null) {
                            if (request.source() != null
                                && request.source().ext().stream().anyMatch(SequenceDocValuesSearchExt.class::isInstance)) {
                                current.nativeQueries.incrementAndGet();
                            } else {
                                current.eqlQueries.incrementAndGet();
                            }
                            long requestBytes = 0;
                            long eqlBytes = 0;
                            for (CircuitBreakerService service : internalCluster().getInstances(CircuitBreakerService.class)) {
                                requestBytes += service.getBreaker(CircuitBreaker.REQUEST).getUsed();
                                eqlBytes += service.getBreaker(EqlPlugin.CIRCUIT_BREAKER_NAME).getUsed();
                            }
                            current.requestBytes.accumulateAndGet(requestBytes, Math::max);
                            current.eqlBytes.accumulateAndGet(eqlBytes, Math::max);
                        }
                        handler.messageReceived(request, channel, task);
                    }
                );
        }
    }

    private void clearMeasurementHooks() {
        for (String node : internalCluster().getNodeNames()) {
            MockTransportService.getInstance(node).clearAllRules();
        }
    }

    /** Checks both engines between workloads so one query cannot conceal reservations or tasks left by another. */
    @After
    public void assertReleased() throws Exception {
        measurement.set(null);
        clearMeasurementHooks();
        assertBusy(() -> {
            for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                assertEquals("PIT contexts retained after a differential query", 0, service.getActiveContexts());
            }
            for (CircuitBreakerService service : internalCluster().getInstances(CircuitBreakerService.class)) {
                assertEquals("EQL matcher reservations retained", 0, service.getBreaker(EqlPlugin.CIRCUIT_BREAKER_NAME).getUsed());
            }
            assertThat(
                client().admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(EqlSearchAction.NAME, EsqlQueryAction.NAME, DriverTaskRunner.ACTION_NAME)
                    .get()
                    .getTasks(),
                empty()
            );
        });
        ensureBlocksReleased();
    }

    private static Event event(String id, String phase, String key, long nanos) {
        return new Event(id, phase, key, nanos, List.of());
    }

    private static Match match(String key, String... identities) {
        return new Match(List.of(key), List.of(identities));
    }

    /** Stores only the payload identity needed to compare matching, keeping payload layouts independent. */
    private record Match(List<Object> keys, List<String> identities) {}

    /** Generation order supplies event.sequence; controlled tie fixtures independently verify immutable PIT order. */
    private record Event(String id, String phase, String key, long nanos, List<String> labels) {}

    /** Observations from one complete query, with sampled memory rather than an asserted peak-memory bound. */
    private record Run(List<Match> matches, long elapsedNanos, long shardQueries, long sampledRequestBytes, long sampledEqlBytes) {}

    /** Samples actual shard query dispatch without changing the request, handler, result or exception path. */
    private static final class Measurement {
        private final AtomicLong nativeQueries = new AtomicLong();
        private final AtomicLong eqlQueries = new AtomicLong();
        private final AtomicLong requestBytes = new AtomicLong();
        private final AtomicLong eqlBytes = new AtomicLong();
    }
}
