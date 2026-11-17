/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryTask;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * Generated histories compare the complete native reader result in both flag modes.
 * Seeds and the complete indexed population are logged before either query so a difference can be reproduced directly.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceKeyFilterRandomizedIT extends AbstractSequenceIntegTestCase {
    private static final Instant BASE = Instant.parse("2026-11-16T00:00:00Z");
    private static final int SHARDS = 3;

    // Only these observed internal-cluster node settings vary by coordinator; every raw warning remains logged.
    private static final Set<String> NODE_DEPRECATIONS = Set.of(
        "[indices.merge.scheduler.use_thread_pool] setting was deprecated in Elasticsearch and will be removed in a future release. See the breaking changes documentation for the next major version.",
        "setting [path.shared_data] is deprecated and will be removed in a future release",
        "Configuring multiple [path.data] paths is deprecated. Use RAID or other system level features for utilizing multiple disks. This feature will be removed in a future release.",
        "Configuring [path.data] with a list is deprecated. Instead specify as a string value.",
        "[index.bloom_filter_for_id_field.enabled] setting was deprecated in Elasticsearch and will be removed in a future release. See the deprecation documentation for the next major version."
    );

    /** Small and over-cutoff populations retain late starts, native ties and every projected event field. */
    public void testSeededGeneratedHistoriesPreserveFullRowsOffAndOn() throws Exception {
        int caseNumber = 0;
        for (int keys : List.of(5, 129)) {
            for (int iteration = 0; iteration < 2; iteration++) {
                String index = "sequence-key-filter-generated-" + caseNumber++;
                long seed = randomLong();
                Throwable primaryFailure = null;
                try {
                    create(index);
                    List<String> routes = routes(index);
                    List<Event> events = generate(seed, keys, routes);
                    logPopulation(index, seed, keys, events);
                    var bulk = client().prepareBulk();
                    for (Event event : events) {
                        bulk.add(prepareIndex(index).setId(event.id()).setRouting(event.routing()).setSource(event.source()));
                    }
                    assertFalse(bulk.get().hasFailures());
                    refresh(index);
                    assertEquals(0, indicesAdmin().prepareForceMerge(index).setMaxNumSegments(1).get().getFailedShards());
                    refresh(index);
                    Observed off = observe(query(index), false);
                    assertReleased();
                    Observed on = observe(query(index), true);
                    assertReleased();
                    logComparison(index, seed, off, on);
                    String reason = "seed=" + seed + ", index=" + index + ", first-key-count=" + (keys + 1);
                    assertFalse(reason, off.rows().isEmpty());
                    assertTrue(reason, off.rows().stream().anyMatch(row -> row.getFirst().equals("late-A")));
                    assertEquals(reason + "/full-rows", off.rows(), on.rows());
                    assertEquals(reason + "/query-warnings", off.queryWarnings(), on.queryWarnings());
                    assertEquals(reason + "/expression-warnings", off.expressionWarnings(), on.expressionWarnings());
                    assertEquals(List.of(), off.expressionWarnings());
                    assertEquals(off.status().completedMatches(), on.status().completedMatches());
                    assertEquals(off.status().rowsEmitted(), on.status().rowsEmitted());
                    assertNoLimitLoss(off);
                    assertNoLimitLoss(on);
                } catch (Exception | Error failure) {
                    primaryFailure = failure;
                    throw failure;
                } finally {
                    Throwable cleanupFailure = null;
                    try {
                        assertReleased();
                    } catch (Exception | Error failure) {
                        cleanupFailure = failure;
                    }
                    try {
                        if (indexExists(index)) {
                            assertAcked(indicesAdmin().prepareDelete(index));
                        }
                    } catch (Exception | Error failure) {
                        if (cleanupFailure == null) {
                            cleanupFailure = failure;
                        } else {
                            cleanupFailure.addSuppressed(failure);
                        }
                    }
                    if (cleanupFailure != null) {
                        if (primaryFailure != null) {
                            primaryFailure.addSuppressed(cleanupFailure);
                        } else if (cleanupFailure instanceof Exception exception) {
                            throw exception;
                        } else {
                            throw (Error) cleanupFailure;
                        }
                    }
                }
            }
        }
        assertEquals(4, caseNumber);
    }

    private void create(String index) {
        assertAcked(indicesAdmin().prepareCreate(index).setSettings(indexSettings(SHARDS, 0).put("index.mode", "standard")).setMapping("""
            {"dynamic":false,"properties":{"@timestamp":{"type":"date_nanos"},"key":{"type":"keyword"},
              "phase":{"type":"keyword"},"value":{"type":"long"},"ordinal":{"type":"long"}}}
            """));
        ensureGreen(index);
    }

    private List<String> routes(String index) {
        IndexRouting routing = IndexRouting.fromIndexMetadata(clusterService().state().metadata().getProject().index(index));
        Map<Integer, String> routes = new LinkedHashMap<>();
        for (int candidate = 0; candidate < 1000 && routes.size() < SHARDS; candidate++) {
            String route = "route-" + candidate;
            routes.putIfAbsent(routing.getShard("probe", route), route);
        }
        assertEquals("generated histories must reach every physical primary shard", SHARDS, routes.size());
        return List.copyOf(routes.values());
    }

    private static List<Event> generate(long seed, int keys, List<String> routes) {
        Random random = new Random(seed);
        List<Event> events = new ArrayList<>();
        for (int key = 0; key < keys; key++) {
            int starts = 1 + random.nextInt(4);
            for (int start = 0; start < starts; start++) {
                // Repeated timestamps and independently routed starts make native tie order part of the comparison.
                events.add(
                    new Event(
                        "A-" + key + "-" + start,
                        random.nextInt(6),
                        "key-" + key,
                        "A",
                        random.nextLong(),
                        events.size(),
                        routes.get(key < SHARDS && start == 0 ? key : random.nextInt(SHARDS))
                    )
                );
            }
            int ends = 1 + random.nextInt(3);
            for (int end = 0; end < ends; end++) {
                events.add(
                    new Event(
                        "B-" + key + "-" + end,
                        5000 + random.nextInt(12),
                        "key-" + key,
                        "B",
                        random.nextLong(),
                        events.size(),
                        routes.get(random.nextInt(SHARDS))
                    )
                );
            }
        }
        int noise = 512 + random.nextInt(65);
        for (int position = 0; position < noise; position++) {
            events.add(
                new Event(
                    "noise-" + position,
                    100 + random.nextInt(3000),
                    "outside-" + position,
                    "B",
                    random.nextLong(),
                    events.size(),
                    routes.get(random.nextInt(SHARDS))
                )
            );
        }
        Collections.shuffle(events, random);
        String lateRoute = routes.get(random.nextInt(SHARDS));
        // Appended in one bulk on one shard: this strict tied pair has stable native _shard_doc order in both legs.
        events.add(new Event("late-A", 4000, "late-key", "A", random.nextLong(), events.size(), lateRoute));
        events.add(new Event("late-B", 4000, "late-key", "B", random.nextLong(), events.size(), lateRoute));
        return List.copyOf(events);
    }

    private static String query(String index) {
        // This limit exceeds the complete generated population; every field from both member documents is retained.
        return """
            SEQUENCE BY key WITHIN 1h PRIMARY 1
            (FROM %s METADATA _id | WHERE phase=="A"),
            (FROM %s METADATA _id | WHERE phase=="B")
            | EVAL second_id=MEMBER(2,_id), second_time=MEMBER(2,@timestamp), second_key=MEMBER(2,key),
                   second_phase=MEMBER(2,phase), second_value=MEMBER(2,value), second_ordinal=MEMBER(2,ordinal)
            | KEEP _id,@timestamp,key,phase,value,ordinal,second_id,second_time,second_key,second_phase,second_value,second_ordinal
            | SORT ordinal,second_ordinal | LIMIT 10000
            """.formatted(index, index);
    }

    private Observed observe(String query, boolean enabled) {
        String coordinator = randomFrom(internalCluster().getNodeNames());
        Settings settings = Settings.builder()
            .put("field_extract_preference", "NONE")
            .put("sequence_max_candidates", 128)
            .put("sequence_key_filter", enabled)
            .build();
        var request = syncEsqlQueryRequest(query).allowPartialResults(false).profile(true).pragmas(new QueryPragmas(settings));
        var future = new PlainActionFuture<Observed>();
        client(coordinator).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
            // The transport releases this borrowed response after the callback; only copied values escape.
            try {
                if (response.isPartial() || response.profile() == null) {
                    throw new IllegalStateException("Generated history requires a complete profiled native response");
                }
                List<SequenceSourceStatus> statuses = response.profile()
                    .drivers()
                    .stream()
                    .flatMap(driver -> driver.operators().stream())
                    .map(OperatorStatus::status)
                    .filter(SequenceSourceStatus.class::isInstance)
                    .map(SequenceSourceStatus.class::cast)
                    .toList();
                if (statuses.size() != 1 || statuses.getFirst().candidateSearchRequests() <= 0) {
                    throw new IllegalStateException("Generated history must execute exactly one real SEQUENCE reader");
                }
                var context = internalCluster().getInstance(TransportService.class, coordinator).getThreadPool().getThreadContext();
                List<String> warnings = context.getResponseHeaders()
                    .getOrDefault("Warning", List.of())
                    .stream()
                    .map(warning -> HeaderWarning.extractWarningValueFromWarningHeader(warning, false))
                    .toList();
                List<String> expressionWarnings = warnings.stream().filter(warning -> warning.startsWith("Line ")).toList();
                future.onResponse(
                    new Observed(getValuesList(response), warnings, expressionWarnings, queryWarnings(warnings), statuses.getFirst())
                );
            } catch (Exception failure) {
                future.onFailure(failure);
            }
        }, future::onFailure));
        return future.actionGet(TimeValue.timeValueSeconds(60));
    }

    private static List<String> queryWarnings(List<String> warnings) {
        return warnings.stream().filter(warning -> NODE_DEPRECATIONS.contains(warning) == false).toList();
    }

    private static void assertNoLimitLoss(Observed observed) {
        assertEquals(0L, observed.status().latestValidCounters().candidatesDroppedAtBound());
        assertEquals(0L, observed.status().latestValidCounters().searchesAbandonedAtWorkLimit());
        assertEquals(0L, observed.status().latestValidCounters().keysThatHitBound());
    }

    private void logPopulation(String index, long seed, int keys, List<Event> events) throws Exception {
        try (var builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field("generator", "seeded-key-filter-history-v1");
            builder.field("index", index);
            builder.field("seed", seed);
            builder.field("first_key_count", keys + 1);
            builder.field("physical_shards", SHARDS);
            builder.field("events", events.stream().map(Event::evidence).toList());
            builder.endObject();
            logger.info("KEY_FILTER_GENERATED_POPULATION {}", BytesReference.bytes(builder).utf8ToString());
        }
    }

    private void logComparison(String index, long seed, Observed off, Observed on) throws Exception {
        try (var builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field("index", index);
            builder.field("seed", seed);
            builder.field("off_rows", off.rows());
            builder.field("on_rows", on.rows());
            builder.field("off_warnings", off.warnings());
            builder.field("on_warnings", on.warnings());
            builder.field("off_expression_warnings", off.expressionWarnings());
            builder.field("on_expression_warnings", on.expressionWarnings());
            builder.field("off_query_warnings", off.queryWarnings());
            builder.field("on_query_warnings", on.queryWarnings());
            builder.field("off_node_deprecations", off.warnings().stream().filter(NODE_DEPRECATIONS::contains).toList());
            builder.field("on_node_deprecations", on.warnings().stream().filter(NODE_DEPRECATIONS::contains).toList());
            builder.field(
                "node_deprecation_policy",
                "Only four exact observed internal-cluster node-setting messages are excluded from parity"
            );
            builder.field("off_status");
            off.status().toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.field("on_status");
            on.status().toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            logger.info("KEY_FILTER_GENERATED_COMPARISON {}", BytesReference.bytes(builder).utf8ToString());
        }
    }

    private record Event(String id, long millis, String key, String phase, long value, long ordinal, String routing) {
        private Map<String, Object> source() {
            return Map.of("@timestamp", BASE.plusMillis(millis).toString(), "key", key, "phase", phase, "value", value, "ordinal", ordinal);
        }

        private Map<String, Object> evidence() {
            return Map.of("id", id, "source", source(), "routing", routing);
        }
    }

    private record Observed(
        List<List<Object>> rows,
        List<String> warnings,
        List<String> expressionWarnings,
        List<String> queryWarnings,
        SequenceSourceStatus status
    ) {}

    /** Every native leg and failed history must release PIT contexts, query tasks and compute ownership. */
    @After
    public void assertReleased() throws Exception {
        assertBusy(() -> {
            for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                assertEquals(0, service.getActiveContexts());
            }
            for (TransportService service : internalCluster().getInstances(TransportService.class)) {
                assertFalse(service.getTaskManager().getTasks().values().stream().anyMatch(EsqlQueryTask.class::isInstance));
            }
        });
        ensureBlocksReleased();
    }
}
