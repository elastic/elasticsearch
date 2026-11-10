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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ShardDocSortField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Proves the search protocol needed by native sequence execution: complete bounded stage pages,
 * global source order, and deferred payload reads from one snapshot spanning indices and nodes.
 * This isolates the matcher/search protocol; native command coverage lives in {@link SequenceNativeIT} and {@link SequenceFailureIT}.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceSnapshotIT extends ESIntegTestCase {
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389Z");
    private static final Comparator<Row> ROW_ORDER = Comparator.comparing(Row::ordinal)
        .thenComparing(Comparator.comparingInt(Row::stage).reversed())
        .thenComparing(Row::key);

    public void testPagesAcrossNodesKeepSnapshotAndPayloadIdentity() throws Exception {
        createStageIndices();
        var bulk = client().prepareBulk();
        for (int cycle = 0; cycle < 16; cycle++) {
            bulk.add(
                prepareIndex("sequence-a").setId("A" + (cycle * 2))
                    .setSource("@timestamp", BASE.plusNanos(cycle * 4L).toString(), "key", "shared", "payload", "original-A" + cycle)
            );
            // Cross-index equal timestamps must retain the shared PIT's implicit document order.
            bulk.add(
                prepareIndex("sequence-a").setId("A" + (cycle * 2 + 1))
                    .setSource("@timestamp", BASE.plusNanos(cycle * 4L + 1).toString(), "key", "shared", "payload", "original-A" + cycle)
            );
            bulk.add(
                prepareIndex("sequence-b").setId("B" + cycle)
                    .setSource("@timestamp", BASE.plusNanos(cycle * 4L + 1).toString(), "key", "shared", "payload", "original-B" + cycle)
            );
        }
        assertFalse(bulk.get().hasFailures());
        refresh("sequence-a", "sequence-b");
        try (Snapshot snapshot = new Snapshot("sequence-a", "sequence-b")) {
            List<List<String>> first = readMerged(snapshot, List.of("sequence-a", "sequence-b"), 1, () -> {
                prepareIndex("sequence-a").setId("A0")
                    .setSource("@timestamp", BASE.toString(), "key", "shared", "payload", "updated-A0")
                    .get();
                prepareIndex("sequence-a").setId("A1")
                    .setSource("@timestamp", BASE.plusNanos(1).toString(), "key", "shared", "payload", "updated-A1")
                    .get();
                client().prepareDelete("sequence-b", "B5").get();
                prepareIndex("sequence-b").setId("new-B")
                    .setSource("@timestamp", BASE.plusNanos(100).toString(), "key", "shared", "payload", "new-B")
                    .get();
                refresh("sequence-a", "sequence-b");
            });
            assertEquals(first.toString(), 16, first.size());
            for (int pageSize : new int[] { 2, 7, 100 }) {
                assertEquals(first, readMerged(snapshot, List.of("sequence-a", "sequence-b"), pageSize, () -> {}));
            }
            for (List<String> sequence : first) {
                for (String handle : sequence) {
                    Map<String, Object> source = snapshot.fetch(handle);
                    assertTrue(source.get("payload").toString().startsWith("original-"));
                }
            }
            assertEquals("original-B5", snapshot.fetch("sequence-b#B5").get("payload"));
            assertTrue("the fixture really crossed pages", snapshot.searches > 40);
        }
        assertContextsReleased();
        // Control: ordinary current-state reads see the updates which the PIT correctly hid.
        assertEquals("updated-A0", client().prepareGet("sequence-a", "A0").get().getSource().get("payload"));
        assertFalse(client().prepareGet("sequence-b", "B5").get().isExists());
    }

    public void testMatchBeyondOrdinaryOutputLimitReadsCompleteCandidates() throws Exception {
        createStageIndices();
        var bulk = client().prepareBulk();
        for (int event = 0; event < 1010; event++) {
            bulk.add(
                prepareIndex("sequence-a").setId("A" + event).setSource("@timestamp", BASE.plusNanos(event).toString(), "key", "shared")
            );
        }
        bulk.add(prepareIndex("sequence-b").setId("B").setSource("@timestamp", BASE.plusNanos(1011).toString(), "key", "shared"));
        assertFalse(bulk.get().hasFailures());
        refresh("sequence-a", "sequence-b");
        try (Snapshot snapshot = new Snapshot("sequence-a", "sequence-b")) {
            assertEquals(
                List.of(List.of("sequence-a#A1009", "sequence-b#B")),
                readMerged(snapshot, List.of("sequence-a", "sequence-b"), 257, () -> {})
            );
            assertEquals("four anchor pages and one completion page", 5, snapshot.searches);
        }
        assertContextsReleased();
    }

    public void testOverlappingStagesAtEqualTimestampAcrossPageBoundaries() throws Exception {
        createStageIndices();
        var bulk = client().prepareBulk();
        for (int event = 0; event < 12; event++) {
            bulk.add(prepareIndex("sequence-a").setId("A" + event).setSource("@timestamp", BASE.toString(), "key", "shared"));
        }
        assertFalse(bulk.get().hasFailures());
        refresh("sequence-a");
        try (Snapshot snapshot = new Snapshot("sequence-a")) {
            List<List<String>> sequences = readMerged(snapshot, List.of("sequence-a", "sequence-a"), 1, () -> {});
            assertEquals(11, sequences.size());
            assertEquals(sequences, readMerged(snapshot, List.of("sequence-a", "sequence-a"), 5, () -> {}));
            for (List<String> sequence : sequences) {
                assertNotEquals("one source event cannot satisfy two stages of the same sequence", sequence.get(0), sequence.get(1));
            }
        }
        assertContextsReleased();
    }

    private void createStageIndices() {
        List<String> nodes = internalCluster().clusterService()
            .state()
            .nodes()
            .getDataNodes()
            .values()
            .stream()
            .map(DiscoveryNode::getName)
            .sorted()
            .toList();
        assertTrue(nodes.size() >= 2);
        for (int stage = 0; stage < 2; stage++) {
            assertAcked(
                indicesAdmin().prepareCreate(stage == 0 ? "sequence-a" : "sequence-b")
                    .setSettings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .put("index.routing.allocation.include._name", nodes.get(stage))
                    )
                    .setMapping("@timestamp", "type=date_nanos", "key", "type=keyword", "payload", "type=keyword")
            );
        }
        ensureGreen("sequence-a", "sequence-b");
    }

    /** Each merge cursor buffers one page. The test's separate seen-ID map detects rereads; it is not a production buffer. */
    private List<List<String>> readMerged(Snapshot snapshot, List<String> stageIndices, int pageSize, Runnable afterInitialPages) {
        List<Cursor> cursors = new ArrayList<>();
        for (int stage = 0; stage < stageIndices.size(); stage++) {
            Cursor cursor = new Cursor(snapshot, stageIndices.get(stage), stage, pageSize);
            cursor.peek();
            cursors.add(cursor);
        }
        afterInitialPages.run();
        List<List<String>> result = new ArrayList<>();
        try (
            SequenceMatcher matcher = new SequenceMatcher(
                stageIndices.size(),
                TimeValue.timeValueSeconds(30),
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                () -> false
            )
        ) {
            while (true) {
                Cursor selected = null;
                for (Cursor cursor : cursors) {
                    Row row = cursor.peek();
                    if (row != null && (selected == null || ROW_ORDER.compare(row, selected.peek()) < 0)) {
                        selected = cursor;
                    }
                }
                if (selected == null) {
                    break;
                }
                Row row = selected.take();
                try (SequenceMatcher.Match match = matcher.match(row.stage, new BytesRef(row.key), row.ordinal, new BytesRef(row.handle))) {
                    if (match != null) {
                        result.add(match.handles().stream().map(BytesRef::utf8ToString).toList());
                    }
                }
            }
        }
        return result;
    }

    private void assertContextsReleased() throws Exception {
        assertBusy(() -> {
            for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                assertEquals(0, service.getActiveContexts());
            }
        });
    }

    private record Row(int stage, String key, SequenceOrdinal ordinal, String handle) {}

    /** Keeps the latest PIT token shared between searches for different stages and the payload fetch phase. */
    private class Snapshot implements Releasable {
        private BytesReference pit;
        private int searches;

        Snapshot(String... indices) {
            pit = client().execute(
                TransportOpenPointInTimeAction.TYPE,
                new OpenPointInTimeRequest(indices).keepAlive(TimeValue.timeValueMinutes(2))
            ).actionGet().getPointInTimeId();
        }

        SearchResponse search(SearchSourceBuilder source) {
            source.pointInTimeBuilder(new PointInTimeBuilder(pit).setKeepAlive(TimeValue.timeValueMinutes(2)));
            // RandomizingClient adds builder preferences; setting PIT through the source does not clear them.
            // Can-match removes shards and renumbers shardRequestIndex, changing _shard_doc's high bits.
            // Keep the PIT shard set fixed even when a stage's query cannot match a shard.
            SearchResponse response = client().prepareSearch()
                .setSource(source)
                .setPreference(null)
                .setPreFilterShardSize(Integer.MAX_VALUE)
                .setAllowPartialSearchResults(false)
                .get();
            boolean success = false;
            try {
                searches++;
                if (response.pointInTimeId() != null) {
                    pit = response.pointInTimeId();
                }
                assertFalse(response.isTimedOut());
                assertEquals(0, response.getFailedShards());
                success = true;
                return response;
            } finally {
                if (success == false) {
                    response.decRef();
                }
            }
        }

        Map<String, Object> fetch(String handle) {
            int separator = handle.indexOf('#');
            SearchResponse response = search(
                new SearchSourceBuilder().query(
                    boolQuery().filter(termQuery("_index", handle.substring(0, separator)))
                        .filter(idsQuery().addIds(handle.substring(separator + 1)))
                ).size(1).fetchSource(true)
            );
            try {
                assertEquals(1, response.getHits().getHits().length);
                return response.getHits().getHits()[0].getSourceAsMap();
            } finally {
                response.decRef();
            }
        }

        @Override
        public void close() {
            assertTrue(client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pit)).actionGet().isSucceeded());
        }
    }

    private class Cursor {
        private final Snapshot snapshot;
        private final String index;
        private final int stage;
        private final int pageSize;
        private List<Row> page = List.of();
        private int offset;
        private Object[] after;
        private SequenceOrdinal previous;
        private final Map<String, SequenceOrdinal> seen = new HashMap<>();
        private boolean exhausted;

        Cursor(Snapshot snapshot, String index, int stage, int pageSize) {
            this.snapshot = snapshot;
            this.index = index;
            this.stage = stage;
            this.pageSize = pageSize;
        }

        Row peek() {
            if (offset == page.size() && exhausted == false) {
                advance();
            }
            return offset < page.size() ? page.get(offset) : null;
        }

        Row take() {
            Row row = peek();
            offset++;
            return row;
        }

        private void advance() {
            SearchSourceBuilder source = new SearchSourceBuilder().query(termQuery("_index", index))
                .size(pageSize)
                .fetchSource(false)
                .docValueField("key")
                .sort(
                    new FieldSortBuilder("@timestamp").order(SortOrder.ASC)
                        .setNumericType("date_nanos")
                        .setFormat("strict_date_optional_time_nanos")
                )
                .sort(new FieldSortBuilder(ShardDocSortField.NAME).order(SortOrder.ASC));
            if (after != null) {
                source.searchAfter(after);
            }
            SearchResponse response = snapshot.search(source);
            try {
                SearchHit[] hits = response.getHits().getHits();
                assertThat(hits.length, lessThanOrEqualTo(pageSize));
                page = new ArrayList<>(hits.length);
                offset = 0;
                exhausted = hits.length < pageSize;
                for (SearchHit hit : hits) {
                    Object[] sort = hit.getSortValues();
                    assertEquals(2, sort.length);
                    Instant timestamp = Instant.parse(sort[0].toString());
                    SequenceOrdinal ordinal = new SequenceOrdinal(
                        timestamp.toEpochMilli(),
                        timestamp.getNano() % 1_000_000,
                        ((Number) sort[1]).longValue()
                    );
                    assertTrue(
                        "stage ["
                            + stage
                            + "] must advance strictly after ["
                            + previous
                            + "] but got ["
                            + ordinal
                            + "] for ["
                            + hit.getId()
                            + "] with search_after ["
                            + java.util.Arrays.toString(after)
                            + "]",
                        previous == null || previous.compareTo(ordinal) < 0
                    );
                    previous = ordinal;
                    assertNull(
                        "stage ["
                            + stage
                            + "] repeated source ["
                            + hit.getId()
                            + "] at ["
                            + ordinal
                            + "] with search_after ["
                            + java.util.Arrays.toString(after)
                            + "]",
                        seen.put(hit.getId(), ordinal)
                    );
                    page.add(new Row(stage, hit.field("key").getValue(), ordinal, hit.getIndex() + "#" + hit.getId()));
                }
                if (hits.length > 0) {
                    after = hits[hits.length - 1].getSortValues();
                }
            } finally {
                response.decRef();
            }
        }
    }
}
