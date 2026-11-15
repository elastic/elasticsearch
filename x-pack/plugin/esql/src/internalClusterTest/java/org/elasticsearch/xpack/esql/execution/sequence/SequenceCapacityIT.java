/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ShardDocSortField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * Measures native sequence queries over bounded, representative workloads using real two-node searches and stage
 * compute. Result assertions verify selection, complete paging and early termination; elapsed times are informational.
 * Search counters describe shard query/fetch phases, rather than source documents or coordinator requests. Request
 * breaker sampling reports an observed peak of accounted memory, rather than an exact peak or total JVM memory.
 * Parent driver CPU and the source status's child stage CPU measure separate work. These small single-query workloads establish
 * regression behavior and local costs, not production throughput, concurrency limits or parity with EQL capacity.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceCapacityIT extends AbstractSequenceIntegTestCase {
    private static final String FIRST = "sequence-capacity-a";
    private static final String SECOND = "sequence-capacity-b";
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389Z");
    private static final String PAYLOAD_SUFFIX = "/" + "x".repeat(512);
    private static final String SEQUENCE = """
        sequence by key with {"maxspan":"30s"} PRIMARY 1
        (from sequence-capacity-a metadata _id | where disposition=="candidate" | keep key, _id, payload),
        (from sequence-capacity-b metadata _id | where disposition=="candidate" | keep key, _id, payload)
        """;
    private static final String PROJECT = "| eval member2_id = MEMBER(2, _id), member2_payload = MEMBER(2, payload) "
        + "| keep key, _id, member2_id, payload, member2_payload ";

    public void testSparseMatchesWithSelectivePredicatesAndDeferredPayloads() {
        createIndices();
        var bulk = client().prepareBulk();
        for (int event = 0; event < 3072; event++) {
            bulk.add(
                prepareIndex(FIRST).setId("A" + event)
                    .setSource(
                        "@timestamp",
                        BASE.plusMillis(event).toString(),
                        "key",
                        key(event),
                        "disposition",
                        event % 4 == 0 ? "candidate" : "noise",
                        "payload",
                        payload("A", event)
                    )
            );
        }
        List<List<Object>> expected = new ArrayList<>();
        for (int event = 0; event < 1536; event++) {
            boolean candidate = event % 256 == 0;
            int anchor = event * 2;
            bulk.add(
                prepareIndex(SECOND).setId("B" + event)
                    .setSource(
                        "@timestamp",
                        BASE.plusMillis(4000 + event).toString(),
                        "key",
                        key(anchor),
                        "disposition",
                        candidate ? "candidate" : "noise",
                        "payload",
                        payload("B", event)
                    )
            );
            if (candidate) {
                expected.add(List.of(key(anchor), "A" + anchor, "B" + event, payload("A", anchor), payload("B", event)));
            }
        }
        assertFalse(bulk.get().hasFailures());
        refresh(FIRST, SECOND);
        var shape = new Shape(4608, 768, 6);
        Measured projected = measure("sparse_projected", SEQUENCE + PROJECT + "| sort key | limit 100", shape);
        assertEquals(expected, projected.values());
        assertEquals(774L, projected.source().candidateRowsFetched());
        assertEquals(12L, projected.source().payloadRowsFetched());
        assertEquals(4L, projected.source().payloadSearchRequests());
        assertEquals(6L, projected.source().completedMatches());
        assertEquals(6L, projected.source().rowsEmitted());
        Measured counted = measure("sparse_count_only", SEQUENCE + "| stats count(*) | limit 1", shape);
        assertEquals(List.of(List.of(6L)), counted.values());
        assertEquals(774L, counted.source().candidateRowsFetched());
        assertEquals(0L, counted.source().payloadRowsFetched());
        assertEquals(0L, counted.source().payloadSearchRequests());
        assertEquals(6L, counted.source().completedMatches());
        assertEquals(6L, counted.source().rowsEmitted());
        assertTrue(
            "COUNT-only selection should avoid the selected-event payload searches",
            counted.search().queryPhases() < projected.search().queryPhases()
        );
    }

    public void testManyLiveKeysAcrossCandidatePages() {
        createIndices();
        var bulk = client().prepareBulk();
        for (int event = 0; event < 2048; event++) {
            boolean candidate = event < 1536;
            bulk.add(
                prepareIndex(FIRST).setId("A" + event)
                    .setSource(
                        "@timestamp",
                        BASE.plusMillis(event).toString(),
                        "key",
                        key(event) + "-distinct-host-and-process-entity",
                        "disposition",
                        candidate ? "candidate" : "noise",
                        "payload",
                        payload("A", event)
                    )
            );
        }
        String matchedKey = key(1535) + "-distinct-host-and-process-entity";
        bulk.add(
            prepareIndex(SECOND).setId("B")
                .setSource(
                    "@timestamp",
                    BASE.plusMillis(4000).toString(),
                    "key",
                    matchedKey,
                    "disposition",
                    "candidate",
                    "payload",
                    payload("B", 1535)
                )
        );
        assertFalse(bulk.get().hasFailures());
        refresh(FIRST, SECOND);
        Measured measured = measure("many_live_keys", SEQUENCE + PROJECT + "| limit 1", new Shape(2049, 1536, 1));
        assertEquals(List.of(List.of(matchedKey, "A1535", "B", payload("A", 1535), payload("B", 1535))), measured.values());
        assertEquals(1537L, measured.source().candidateRowsFetched());
        assertEquals(2L, measured.source().payloadRowsFetched());
        assertEquals(1536L, measured.source().maxPendingCandidates());
        assertEquals(1L, measured.source().completedMatches());
        assertEquals(1L, measured.source().rowsEmitted());
    }

    public void testManyCompletedMatchesAndEarlyLimit() {
        createIndices();
        var bulk = client().prepareBulk();
        List<List<Object>> expected = new ArrayList<>();
        for (int event = 0; event < 300; event++) {
            String sharedKey = key(event);
            for (int version = 0; version < 2; version++) {
                String id = "A" + event + "-" + version;
                bulk.add(
                    prepareIndex(FIRST).setId(id)
                        .setSource(
                            "@timestamp",
                            BASE.plusMillis(event * 2L + version).toString(),
                            "key",
                            sharedKey,
                            "disposition",
                            "candidate",
                            "payload",
                            payload(id, event)
                        )
                );
            }
            bulk.add(
                prepareIndex(SECOND).setId("B" + event)
                    .setSource(
                        "@timestamp",
                        BASE.plusMillis(2000 + event).toString(),
                        "key",
                        sharedKey,
                        "disposition",
                        "candidate",
                        "payload",
                        payload("B", event)
                    )
            );
            for (int noise = 0; noise < 3; noise++) {
                for (String index : List.of(FIRST, SECOND)) {
                    bulk.add(
                        prepareIndex(index).setId("noise-" + event + "-" + noise)
                            .setSource(
                                "@timestamp",
                                BASE.plusMillis(10000 + event * 3L + noise).toString(),
                                "key",
                                sharedKey,
                                "disposition",
                                "noise",
                                "payload",
                                payload("noise", event)
                            )
                    );
                }
            }
            expected.add(List.of(sharedKey, "A" + event + "-1", "B" + event, payload("A" + event + "-1", event), payload("B", event)));
        }
        assertFalse(bulk.get().hasFailures());
        refresh(FIRST, SECOND);
        var shape = new Shape(2700, 600, 300);
        Measured complete = measure("many_completed_matches", SEQUENCE + PROJECT + "| sort key | limit 400", shape);
        assertEquals(expected, complete.values());
        assertEquals(900L, complete.source().candidateRowsFetched());
        assertEquals(5L, complete.source().candidateSearchRequests());
        assertEquals(600L, complete.source().payloadRowsFetched());
        assertEquals(12L, complete.source().payloadSearchRequests());
        // Untouched raw payloads load late; the five candidate pages are the only stage executions.
        assertEquals(5L, complete.source().stageDrivers());
        assertEquals(300L, complete.source().completedMatches());
        assertEquals(300L, complete.source().rowsEmitted());
        Measured limited = measure("many_completed_limit_one", SEQUENCE + PROJECT + "| limit 1", shape);
        assertEquals(List.of(expected.getFirst()), limited.values());
        assertEquals(2L, limited.source().payloadRowsFetched());
        assertEquals(2L, limited.source().payloadSearchRequests());
        assertEquals(1L, limited.source().completedMatches());
        assertEquals(1L, limited.source().rowsEmitted());
        assertTrue(limited.source().candidateRowsFetched() < complete.source().candidateRowsFetched());
        assertTrue(
            "LIMIT 1 should stop native searches before all completed matches are materialized",
            limited.search().queryPhases() < complete.search().queryPhases()
        );
    }

    /** Several matches share a source event but select different computed expansion rows across stage output pages. */
    public void testPayloadBatchDeduplicatesEventsAndReplaysSelectedExpansions() {
        createIndices();
        List<String> keys = new ArrayList<>();
        for (int value = 149; value >= 0; value--) {
            keys.add(key(value));
        }
        var bulk = client().prepareBulk();
        for (String index : List.of(FIRST, SECOND)) {
            String payload = index.equals(FIRST) ? "A" : "B";
            bulk.add(
                prepareIndex(index).setId(payload)
                    .setSource(
                        "@timestamp",
                        BASE.plusMillis(index.equals(FIRST) ? 0 : 1).toString(),
                        "key",
                        keys,
                        "disposition",
                        "candidate",
                        "payload",
                        payload
                    )
            );
        }
        assertFalse(bulk.get().hasFailures());
        refresh(FIRST, SECOND);
        String query = """
            sequence by key with {"maxspan":"30s"} PRIMARY 1
            (from sequence-capacity-a metadata _id | where disposition=="candidate" | mv_expand key
              | eval expanded_payload=concat(payload,"/",key) | keep key, _id, expanded_payload),
            (from sequence-capacity-b metadata _id | where disposition=="candidate" | mv_expand key
              | eval expanded_payload=concat(payload,"/",key) | keep key, _id, expanded_payload)
            | EVAL member2__id = MEMBER(2, _id), member2_expanded_payload = MEMBER(2, expanded_payload)
              | keep key, _id, member2__id, expanded_payload, member2_expanded_payload
            | limit 200
            """;
        Measured measured = measure("shared_event_expansions", query, new Shape(2, 1, 1));
        List<List<Object>> expected = new ArrayList<>();
        for (int value = 0; value < 150; value++) {
            String key = key(value);
            expected.add(List.of(key, "A", "B", "A/" + key, "B/" + key));
        }
        assertEquals(expected, measured.values());
        assertEquals(150L, measured.source().completedMatches());
        assertEquals(150L, measured.source().rowsEmitted());
        assertEquals(2L, measured.source().candidateRowsFetched());
        assertEquals(8L, measured.source().payloadRowsFetched());
        assertEquals(8L, measured.source().payloadSearchRequests());
        assertEquals(10L, measured.source().stageDrivers());
    }

    /** Physical continuation skips an unrelated routed copy outside the selected shard's bounded identity batch. */
    public void testPayloadBatchPagesRoutedDuplicateIds() {
        createIndices();
        IndexRouting routing = IndexRouting.fromIndexMetadata(clusterService().state().metadata().getProject().index(FIRST));
        String first = "route0";
        int shard = routing.getShard("duplicate", first);
        String second = null;
        for (int value = 1; value < 1000; value++) {
            String candidate = "route" + value;
            if (routing.getShard("duplicate", candidate) != shard) {
                second = candidate;
                break;
            }
        }
        assertNotNull(second);
        var bulk = client().prepareBulk();
        String[] selectedIds = { "first", "duplicate", "last" };
        for (int event = 0; event < selectedIds.length; event++) {
            bulk.add(
                prepareIndex(FIRST).setId(selectedIds[event])
                    .setRouting(first)
                    .setSource(
                        "@timestamp",
                        BASE.plusMillis(event * 100L).toString(),
                        "key",
                        key(event),
                        "disposition",
                        "candidate",
                        "payload",
                        payload("A", event)
                    )
            );
            bulk.add(
                prepareIndex(SECOND).setId("B" + event)
                    .setSource(
                        "@timestamp",
                        BASE.plusMillis(1000 + event).toString(),
                        "key",
                        key(event),
                        "disposition",
                        "candidate",
                        "payload",
                        payload("B", event)
                    )
            );
        }
        bulk.add(
            prepareIndex(FIRST).setId("duplicate")
                .setRouting(second)
                .setSource(
                    "@timestamp",
                    BASE.plusMillis(150).toString(),
                    "key",
                    "unmatched",
                    "disposition",
                    "candidate",
                    "payload",
                    "unselected-routed-copy"
                )
        );
        assertFalse(bulk.get().hasFailures());
        refresh(FIRST, SECOND);
        Measured measured = measure("routed_batch_paging", SEQUENCE + PROJECT + "| limit 10", new Shape(7, 4, 3));
        List<List<Object>> expected = new ArrayList<>();
        for (int event = 0; event < selectedIds.length; event++) {
            expected.add(List.of(key(event), selectedIds[event], "B" + event, payload("A", event), payload("B", event)));
        }
        assertEquals(expected, measured.values());
        assertEquals(3L, measured.source().completedMatches());
        assertEquals(3L, measured.source().rowsEmitted());
        assertEquals(7L, measured.source().candidateRowsFetched());
        assertEquals(2L, measured.source().candidateSearchRequests());
        assertEquals(6L, measured.source().payloadRowsFetched());
        assertEquals(4L, measured.source().payloadSearchRequests());
        // Physical late-field paging still fetches all selected payloads, without running replay drivers.
        assertEquals(2L, measured.source().stageDrivers());
    }

    /** Physical membership excludes a routed duplicate between selected coordinates for both primaries. */
    public void testPhysicalPayloadPagingSkipsInterleavedRoutedDuplicate() {
        createIndices();
        IndexRouting routing = IndexRouting.fromIndexMetadata(clusterService().state().metadata().getProject().index(FIRST));
        String first = "physical-route0";
        int shard = routing.getShard("middle", first);
        String second = null;
        for (int value = 1; value < 1000; value++) {
            String candidate = "physical-route" + value;
            if (routing.getShard("middle", candidate) != shard) {
                second = candidate;
                break;
            }
        }
        assertNotNull(second);
        String[] selectedIds = { "first", "alpha", "middle", "omega" };
        List<Map<String, Object>> firstBodies = new ArrayList<>();
        List<Map<String, Object>> secondBodies = new ArrayList<>();
        for (int event = 0; event < selectedIds.length; event++) {
            firstBodies.add(physicalPagingBody(event, "A", event * 100L));
            secondBodies.add(physicalPagingBody(event, "B", 1000L + event));
        }
        var bulk = client().prepareBulk();
        // The first routing's bulk order fixes alpha < unselected middle < omega within one Lucene shard.
        bulk.add(prepareIndex(FIRST).setId("first").setRouting(first).setSource(firstBodies.get(0)));
        bulk.add(prepareIndex(FIRST).setId("alpha").setRouting(first).setSource(firstBodies.get(1)));
        bulk.add(
            prepareIndex(FIRST).setId("middle")
                .setRouting(first)
                .setSource(
                    "@timestamp",
                    BASE.plusMillis(250).toString(),
                    "key",
                    "unmatched",
                    "disposition",
                    "candidate",
                    "payload",
                    "unselected-physical-copy"
                )
        );
        bulk.add(prepareIndex(FIRST).setId("omega").setRouting(first).setSource(firstBodies.get(3)));
        bulk.add(prepareIndex(FIRST).setId("middle").setRouting(second).setSource(firstBodies.get(2)));
        for (int event = 0; event < selectedIds.length; event++) {
            bulk.add(prepareIndex(SECOND).setId("B" + event).setSource(secondBodies.get(event)));
        }
        assertFalse(bulk.get().hasFailures());
        refresh(FIRST, SECOND);
        assertPhysicalPagingFixtureOrder();
        for (int primary : List.of(1, 2)) {
            int other = primary == 1 ? 2 : 1;
            String query = """
                SEQUENCE BY key WITH {"maxspan":"30s"} PRIMARY %s
                (FROM sequence-capacity-a METADATA _id, _source | WHERE disposition=="candidate"
                  | EVAL computed_payload=CONCAT(payload, "/computed") | KEEP key, _id, _source, computed_payload),
                (FROM sequence-capacity-b METADATA _id, _source | WHERE disposition=="candidate"
                  | EVAL computed_payload=CONCAT(payload, "/computed") | KEEP key, _id, _source, computed_payload)
                | EVAL other_id=MEMBER(%s, _id), other_payload=MEMBER(%s, computed_payload)
                | KEEP key, _id, other_id, computed_payload, other_payload, _source | LIMIT 10
                """.formatted(primary, other, other);
            Measured measured = measure("physical_routed_batch_primary_" + primary, query, new Shape(9, 5, 4));
            List<List<Object>> expected = new ArrayList<>();
            for (int event = 0; event < selectedIds.length; event++) {
                expected.add(
                    List.of(
                        key(event),
                        primary == 1 ? selectedIds[event] : "B" + event,
                        primary == 1 ? "B" + event : selectedIds[event],
                        payload(primary == 1 ? "A" : "B", event) + "/computed",
                        payload(primary == 1 ? "B" : "A", event) + "/computed",
                        primary == 1 ? firstBodies.get(event) : secondBodies.get(event)
                    )
                );
            }
            assertEquals(expected, measured.values());
            assertEquals(4L, measured.source().completedMatches());
            assertEquals(4L, measured.source().rowsEmitted());
            // Two stages and two output batches fetch only the selected positions, without paging through the routed duplicate.
            assertEquals(4L, measured.source().payloadSearchRequests());
            assertEquals(8L, measured.source().payloadRowsFetched());
        }
    }

    private static Map<String, Object> physicalPagingBody(int event, String stage, long millis) {
        return Map.of(
            "@timestamp",
            BASE.plusMillis(millis).toString(),
            "key",
            key(event),
            "disposition",
            "candidate",
            "payload",
            payload(stage, event),
            "body",
            Map.of("stage", stage, "array", List.of(event, event + 100))
        );
    }

    /** Checks real PIT coordinates before measuring, independently of the order in which PIT shards are enumerated. */
    private void assertPhysicalPagingFixtureOrder() {
        BytesReference pit = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest(FIRST).keepAlive(TimeValue.timeValueMinutes(2))
        ).actionGet().getPointInTimeId();
        try {
            var source = new SearchSourceBuilder().query(idsQuery().addIds("alpha", "middle", "omega"))
                .sort(new FieldSortBuilder(ShardDocSortField.NAME).order(SortOrder.ASC))
                .size(10)
                .fetchSource(true)
                .pointInTimeBuilder(new PointInTimeBuilder(pit).setKeepAlive(TimeValue.timeValueMinutes(2)));
            var response = client().prepareSearch()
                .setSource(source)
                .setPreference(null)
                .setPreFilterShardSize(Integer.MAX_VALUE)
                .setAllowPartialSearchResults(false)
                .get();
            try {
                if (response.pointInTimeId() != null) {
                    pit = response.pointInTimeId();
                }
                assertFalse(response.isTimedOut());
                assertEquals(0, response.getFailedShards());
                assertEquals(4, response.getHits().getHits().length);
                SearchHit alpha = null, omega = null, selected = null, unselected = null;
                for (SearchHit hit : response.getHits().getHits()) {
                    switch (hit.getId()) {
                        case "alpha" -> alpha = hit;
                        case "omega" -> omega = hit;
                        case "middle" -> {
                            if (hit.getSourceAsMap().get("key").equals("unmatched")) {
                                unselected = hit;
                            } else {
                                selected = hit;
                            }
                        }
                        default -> throw new AssertionError("unexpected physical fixture hit " + hit.getId());
                    }
                }
                assertNotNull(alpha);
                assertNotNull(omega);
                assertNotNull(selected);
                assertNotNull(unselected);
                assertEquals(alpha.getShard().getShardId(), unselected.getShard().getShardId());
                assertEquals(alpha.getShard().getShardId(), omega.getShard().getShardId());
                assertNotEquals(alpha.getShard().getShardId(), selected.getShard().getShardId());
                assertTrue((Long) alpha.getSortValues()[0] < (Long) unselected.getSortValues()[0]);
                assertTrue((Long) unselected.getSortValues()[0] < (Long) omega.getSortValues()[0]);
            } finally {
                response.decRef();
            }
        } finally {
            assertTrue(client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pit)).actionGet().isSucceeded());
        }
    }

    private Measured measure(String scenario, String query, Shape shape) {
        SearchCounters before = searchCounters();
        List<CircuitBreaker> breakers = new ArrayList<>();
        for (String node : internalCluster().getNodeNames()) {
            breakers.add(internalCluster().getInstance(CircuitBreakerService.class, node).getBreaker(CircuitBreaker.REQUEST));
        }
        var peak = new AtomicLong();
        var samples = new AtomicLong();
        Runnable sample = () -> {
            long used = breakers.stream().mapToLong(CircuitBreaker::getUsed).sum();
            peak.accumulateAndGet(used, Math::max);
            samples.incrementAndGet();
        };
        ThreadPool pool = internalCluster().getInstance(ThreadPool.class);
        var sampler = pool.scheduleWithFixedDelay(sample, TimeValue.timeValueMillis(5), pool.generic());
        sample.run();
        long elapsedNanos;
        long driverCpuNanos;
        long driverIterations;
        int reportedDrivers;
        SequenceSourceStatus sourceStatus;
        List<List<Object>> values;
        long start = System.nanoTime();
        try (
            var response = run(
                syncEsqlQueryRequest(query).profile(true).pragmas(new QueryPragmas(Settings.builder().put("page_size", 128).build())),
                TimeValue.timeValueMinutes(2)
            )
        ) {
            elapsedNanos = System.nanoTime() - start;
            sample.run();
            values = getValuesList(response);
            assertNotNull(response.profile());
            driverCpuNanos = response.profile().drivers().stream().mapToLong(DriverProfile::cpuNanos).sum();
            driverIterations = response.profile().drivers().stream().mapToLong(DriverProfile::iterations).sum();
            reportedDrivers = response.profile().drivers().size();
            List<SequenceSourceStatus> sources = response.profile()
                .drivers()
                .stream()
                .flatMap(driver -> driver.operators().stream())
                .map(OperatorStatus::status)
                .filter(SequenceSourceStatus.class::isInstance)
                .map(SequenceSourceStatus.class::cast)
                .toList();
            assertEquals("one native source status must survive query profiling", 1, sources.size());
            sourceStatus = sources.getFirst();
            assertTrue("stage driver execution must be represented", sourceStatus.stageDrivers() > 0);
            assertTrue("stage driver iterations must be represented", sourceStatus.stageDriverIterations() > 0);
            assertTrue(sourceStatus.stageDriverCpuNanos() >= 0);
        } finally {
            sampler.cancel();
        }
        SearchCounters search = searchCounters().subtract(before);
        logger.debug(
            "sequence_capacity scenario={} indexed_events={} stage1_candidates={} stage2_candidates={} output_rows={} "
                + "elapsed_ms={} shard_query_phases={} shard_fetch_phases={} shard_query_ms={} shard_fetch_ms={} "
                + "sampled_request_peak_bytes={} breaker_samples={} reported_driver_cpu_nanos={} "
                + "reported_driver_iterations={} reported_drivers={} "
                + "native_status={}",
            scenario,
            shape.indexedEvents(),
            shape.stage1Candidates(),
            shape.stage2Candidates(),
            values.size(),
            elapsedNanos / 1_000_000L,
            search.queryPhases(),
            search.fetchPhases(),
            search.queryMillis(),
            search.fetchMillis(),
            peak.get(),
            samples.get(),
            driverCpuNanos,
            driverIterations,
            reportedDrivers,
            sourceStatus
        );
        return new Measured(values, search, sourceStatus);
    }

    private SearchCounters searchCounters() {
        var stats = indicesAdmin().prepareStats(FIRST, SECOND).clear().setSearch(true).get().getTotal().getSearch().getTotal();
        return new SearchCounters(stats.getQueryCount(), stats.getFetchCount(), stats.getQueryTimeInMillis(), stats.getFetchTimeInMillis());
    }

    private void createIndices() {
        for (String index : List.of(FIRST, SECOND)) {
            assertAcked(
                indicesAdmin().prepareCreate(index)
                    .setSettings(
                        Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
                    .setMapping("@timestamp", "type=date", "key", "type=keyword", "disposition", "type=keyword", "payload", "type=keyword")
            );
        }
    }

    private static String key(int value) {
        return "key-" + (10000 + value);
    }

    private static String payload(String prefix, int value) {
        return prefix + "-" + value + PAYLOAD_SUFFIX;
    }

    @After
    public void checkSequenceResourcesReleased() throws Exception {
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            assertBusy(() -> assertEquals(0, service.getActiveContexts()));
        }
        ensureBlocksReleased();
    }

    /** Describes the fixed dataset separately from measured work so selection costs can be compared. */
    private record Shape(int indexedEvents, int stage1Candidates, int stage2Candidates) {}

    /** Counts shard search phases; one coordinator search can contribute several phases. */
    private record SearchCounters(long queryPhases, long fetchPhases, long queryMillis, long fetchMillis) {
        SearchCounters subtract(SearchCounters previous) {
            return new SearchCounters(
                queryPhases - previous.queryPhases,
                fetchPhases - previous.fetchPhases,
                queryMillis - previous.queryMillis,
                fetchMillis - previous.fetchMillis
            );
        }
    }

    /** Copies values before closing the response and preserves search costs for comparisons on the same dataset. */
    private record Measured(List<List<Object>> values, SearchCounters search, SequenceSourceStatus source) {}
}
