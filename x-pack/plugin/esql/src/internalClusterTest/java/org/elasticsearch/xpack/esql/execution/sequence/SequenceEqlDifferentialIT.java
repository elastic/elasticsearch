/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.common.breaker.CircuitBreaker;
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
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlPluginWithEnterpriseOrTrialLicense;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.ql.plugin.QlPlugin;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
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
 * Positive-sequence parity through both actual query actions, planners, searches and payload retrieval on identical
 * immutable events. Capacity observations have no relative timing assertions: cache state and test-host scheduling make
 * them unsuitable for production performance claims. Identity, completeness and resource cleanup remain strict checks.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceEqlDifferentialIT extends AbstractEsqlIntegTestCase {
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

    public void testControlledEqualTimestamps() throws Exception {
        // One shard, one append-only bulk, and ascending event.sequence make EQL's explicit tie breaker agree with
        // native PIT document order. This does not claim parity for arbitrary user-selected EQL tie-breaker fields.
        indexEvents(
            List.of(
                event("A1", "A", "shared", 0),
                event("A2", "A", "shared", 0),
                event("B3", "B", "shared", 0),
                event("B4", "B", "shared", 0),
                event("A5", "A", "shared", 0),
                event("B6", "B", "shared", 0)
            )
        );
        assertParity("key", false, TWO_STAGES, "30s", 100, 2, List.of(match("shared", "A2", "B3"), match("shared", "A5", "B6")));
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
        for (int stage = 1; stage <= predicates.size(); stage++) {
            output.add("stage" + stage + "._id");
        }
        String query = "sequence by "
            + key
            + " with {\"maxspan\":\""
            + maxspan
            + "\"} "
            + stages
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

    /** Append order supplies event.sequence and native document order for controlled equal-timestamp fixtures. */
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
