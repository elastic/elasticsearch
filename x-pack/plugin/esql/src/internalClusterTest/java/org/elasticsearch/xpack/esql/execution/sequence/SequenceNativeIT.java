/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongMapperPlugin;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;

/** Native query-action coverage joining real searches, ordinary stage compute, matching and typed payload output. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceNativeIT extends AbstractEsqlIntegTestCase {
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389Z");

    /** Empty filtered aliases still contribute their successfully skipped source shards after retained PIT pages are read. */
    public void testSnapshotPagesPreserveCanMatchSkippedSourceMetadata() {
        createIndices();
        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAlias("sequence-a", "sequence-empty-a", new MatchNoneQueryBuilder())
        );
        assertFalse(
            client().prepareBulk()
                .add(prepareIndex("sequence-a").setId("A").setSource("@timestamp", BASE.toString(), "key", "shared"))
                .add(prepareIndex("sequence-b").setId("B").setSource("@timestamp", BASE.plusNanos(1).toString(), "key", "shared"))
                .get()
                .hasFailures()
        );
        refresh("sequence-a", "sequence-b");
        try (var response = run(syncEsqlQueryRequest("""
            sequence by key with {"maxspan":"30s"}
            (from sequence-empty-a | keep key), (from sequence-b | keep key)
            | limit 10
            """).includeExecutionMetadata(true))) {
            assertFalse(response.values().hasNext());
            EsqlExecutionInfo.Cluster metadata = response.getExecutionInfo().getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertEquals(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, metadata.getStatus());
            assertEquals(Integer.valueOf(4), metadata.getTotalShards());
            assertEquals(Integer.valueOf(4), metadata.getSuccessfulShards());
            assertEquals(Integer.valueOf(2), metadata.getSkippedShards());
            assertEquals(Integer.valueOf(0), metadata.getFailedShards());
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), UnsignedLongMapperPlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        // These fixtures assert doc-value ordering and source-disabled fields; storage preference parity has its own differential suite.
        return new QueryPragmas(Settings.builder().put(super.getPragmas().settings()).put("field_extract_preference", "NONE").build());
    }

    public void testDownstreamQualifiedProjectionWithoutStageKeep() {
        createIndices();
        assertFalse(
            client().prepareBulk()
                .add(prepareIndex("sequence-a").setId("A").setSource("@timestamp", BASE.toString(), "key", "shared", "payload", "anchor"))
                .add(
                    prepareIndex("sequence-b").setId("B")
                        .setSource("@timestamp", BASE.plusNanos(1).toString(), "key", "shared", "payload", "completion")
                )
                .get()
                .hasFailures()
        );
        refresh("sequence-a", "sequence-b");
        for (String projection : List.of("stage1.payload, stage2.payload", "stage*.payload")) {
            try (var response = run(Strings.format("""
                sequence by key with {"maxspan":"30s"} (from sequence-a), (from sequence-b)
                | keep key, %s | limit 1
                """, projection))) {
                assertValues(response.values(), List.of(List.of("shared", "anchor", "completion")));
            }
        }
        try (var response = run("""
            sequence by key with {"maxspan":"30s"}
            (from sequence-a | eval selected=CONCAT(payload, "/done")), (from sequence-b)
            | keep key, stage1.selected | limit 1
            """)) {
            assertValues(response.values(), List.of(List.of("shared", "anchor/done")));
        }
    }

    /** Bare stage patterns retain selected payloads, and a limited query reports successful execution when requested. */
    public void testBareStageWildcardPayloadsWithoutStageKeep() {
        createIndices();
        assertFalse(
            client().prepareBulk()
                .add(prepareIndex("sequence-a").setId("A").setSource("@timestamp", BASE.toString(), "key", "shared", "payload", "anchor"))
                .add(
                    prepareIndex("sequence-b").setId("B")
                        .setSource("@timestamp", BASE.plusNanos(1).toString(), "key", "shared", "payload", "completion")
                )
                .get()
                .hasFailures()
        );
        refresh("sequence-a", "sequence-b");
        for (String pattern : List.of("stage*", "stage1*", "stage2*", "stage*payload")) {
            try (var response = run(syncEsqlQueryRequest(Strings.format("""
                sequence by key with {"maxspan":"30s"} (from sequence-a), (from sequence-b)
                | keep key, %s | limit 1
                """, pattern)).pragmas(getPragmas()).includeExecutionMetadata(true))) {
                var rows = response.values();
                assertTrue(rows.hasNext());
                var values = rows.next();
                Map<String, Object> row = new LinkedHashMap<>();
                for (var column : response.columns()) {
                    row.put(column.name(), values.next());
                }
                assertFalse(values.hasNext());
                assertFalse(rows.hasNext());
                assertEquals("shared", row.get("key"));
                if (pattern.equals("stage2*")) {
                    assertFalse(row.containsKey("stage1.payload"));
                } else {
                    assertEquals("anchor", row.get("stage1.payload"));
                }
                if (pattern.equals("stage1*")) {
                    assertFalse(row.containsKey("stage2.payload"));
                } else {
                    assertEquals("completion", row.get("stage2.payload"));
                }
                assertEquals(
                    EsqlExecutionInfo.Cluster.Status.SUCCESSFUL,
                    response.getExecutionInfo().getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).getStatus()
                );
            }
        }
    }

    /** A sequence view is rejected with and without an output-only request filter, including a nested view reference. */
    public void testViewsContainingSequenceAreRejectedWithOutputFilter() {
        createIndices();
        assertFalse(
            client().prepareBulk()
                .add(prepareIndex("sequence-a").setId("A").setSource("@timestamp", BASE.toString(), "key", "shared"))
                .add(prepareIndex("sequence-b").setId("B").setSource("@timestamp", BASE.plusNanos(1).toString(), "key", "shared"))
                .get()
                .hasFailures()
        );
        refresh("sequence-a", "sequence-b");
        String query = """
            sequence by key with {"maxspan":"30s"} (from sequence-a), (from sequence-b)
            | eval view_marker = "matched" | keep key, view_marker
            """;
        // The same body executes outside a view, proving that the fixture has a completed match.
        try (var response = run(query)) {
            assertValues(response.values(), List.of(List.of("shared", "matched")));
        }
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View("sequence-view", query))
            )
        );
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    new View("sequence-view-wrapper", "from sequence-view")
                )
            )
        );
        for (String view : List.of("sequence-view", "sequence-view-wrapper")) {
            for (boolean filtered : List.of(false, true)) {
                var request = syncEsqlQueryRequest("from " + view + " | keep key, view_marker").pragmas(getPragmas());
                if (filtered) {
                    request.filter(termQuery("view_marker", "matched"));
                }
                VerificationException error = expectThrows(VerificationException.class, () -> {
                    try (var ignored = run(request)) {
                        fail("A view containing SEQUENCE must not execute");
                    }
                });
                assertThat(error.getMessage(), containsString("SEQUENCE is not supported inside view definitions [sequence-view]"));
            }
        }
    }

    /** Expression-contained sequences are rejected before wrapping, for scalar/tuple IN, output filters and nested views. */
    public void testViewsContainingSequenceInMembershipSubqueryAreRejected() {
        createIndices();
        assertFalse(
            client().prepareBulk()
                .add(prepareIndex("sequence-a").setId("A").setSource("@timestamp", BASE.toString(), "key", "shared"))
                .add(prepareIndex("sequence-b").setId("B").setSource("@timestamp", BASE.plusNanos(1).toString(), "key", "shared"))
                .get()
                .hasFailures()
        );
        refresh("sequence-a", "sequence-b");
        for (boolean multiColumn : List.of(false, true)) {
            String sequence = "sequence by key with {\"maxspan\":\"30s\"} (from sequence-a), (from sequence-b)"
                + (multiColumn ? " | eval key_copy = key | keep key, key_copy" : " | keep key");
            String template = "from sequence-a | where "
                + (multiColumn ? "(key, key)" : "key")
                + " IN (%s) | eval view_marker = \"matched\" | keep key, view_marker";
            if (multiColumn == false) {
                // IN's lexer does not enter command mode for bare SEQUENCE; FROM supplies the supported subquery entry.
                ParsingException parsing = expectThrows(ParsingException.class, () -> {
                    try (var ignored = run(Strings.format(template, sequence))) {
                        fail("A bare SEQUENCE cannot start an IN subquery");
                    }
                });
                assertThat(parsing.getMessage(), containsString("mismatched input 'by'"));
            }
            String query = Strings.format(template, "from (" + sequence + ")");
            // Successful controls prove parsing, IN execution and a completed match independently of view rejection.
            try (var response = run(query)) {
                assertValues(response.values(), List.of(List.of("shared", "matched")));
            }
            String viewName = multiColumn ? "sequence-in-view-multi" : "sequence-in-view";
            assertAcked(
                client().execute(
                    PutViewAction.INSTANCE,
                    new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(viewName, query))
                )
            );
            assertAcked(
                client().execute(
                    PutViewAction.INSTANCE,
                    new PutViewAction.Request(
                        TEST_REQUEST_TIMEOUT,
                        TEST_REQUEST_TIMEOUT,
                        new View(viewName + "-wrapper", "from " + viewName)
                    )
                )
            );
            for (String view : List.of(viewName, viewName + "-wrapper")) {
                for (boolean filtered : List.of(false, true)) {
                    var request = syncEsqlQueryRequest("from " + view + " | keep key, view_marker").pragmas(getPragmas());
                    if (filtered) {
                        request.filter(termQuery("view_marker", "matched"));
                    }
                    VerificationException error = expectThrows(VerificationException.class, () -> {
                        try (var ignored = run(request)) {
                            fail("A view containing SEQUENCE in an IN expression must not execute");
                        }
                    });
                    assertThat(error.getMessage(), containsString("SEQUENCE is not supported inside view definitions [" + viewName + "]"));
                }
            }
        }
    }

    public void testLimitBySelectsCompletedSequencesAfterSort() {
        createIndices();
        var bulk = client().prepareBulk();
        for (int cycle = 0; cycle < 2; cycle++) {
            bulk.add(
                prepareIndex("sequence-a").setId("A" + cycle)
                    .setSource("@timestamp", BASE.plusMillis(cycle * 2L).toString(), "key", "shared", "payload", "A" + cycle)
            );
            bulk.add(
                prepareIndex("sequence-b").setId("B" + cycle)
                    .setSource("@timestamp", BASE.plusMillis(cycle * 2L + 1).toString(), "key", "shared", "payload", "B" + cycle)
            );
        }
        assertFalse(bulk.get().hasFailures());
        refresh("sequence-a", "sequence-b");
        try (var response = run("""
            sequence by key with {"maxspan":"30s"} (from sequence-a), (from sequence-b)
            | keep key, stage1.payload, stage2.payload
            | sort stage1.payload desc | limit 1 by key
            """)) {
            assertValues(response.values(), List.of(List.of("shared", "A1", "B1")));
        }
    }

    public void testStageIndexExclusionsRemainLocalToEachSource() {
        createIndices();
        assertFalse(
            client().prepareBulk()
                .add(prepareIndex("sequence-a").setId("A").setSource("@timestamp", BASE.toString(), "key", "shared", "payload", "anchor"))
                .add(
                    prepareIndex("sequence-b").setId("B")
                        .setSource("@timestamp", BASE.plusNanos(1).toString(), "key", "shared", "payload", "completion")
                )
                .get()
                .hasFailures()
        );
        refresh("sequence-a", "sequence-b");
        for (String firstSource : List.of("sequence-a", "sequence-*,-sequence-b", "sequence-a,sequence-missing")) {
            try (var response = run(Strings.format("""
                sequence by key with {"maxspan":"30s"}
                (from %s), (from sequence-*,-sequence-a)
                | keep key, stage1.payload, stage2.payload | limit 10
                """, firstSource))) {
                assertValues(response.values(), List.of(List.of("shared", "anchor", "completion")));
            }
        }
    }

    public void testLatestPredecessorBeyondInputLimitWithTypedPayload() {
        createIndices();
        var bulk = client().prepareBulk();
        for (int event = 0; event < 1010; event++) {
            bulk.add(
                prepareIndex("sequence-a").setId("A" + event)
                    .setSource("@timestamp", BASE.plusMillis(event).toString(), "key", "shared", "payload", "value-" + event)
            );
        }
        bulk.add(
            prepareIndex("sequence-b").setId("B")
                .setSource("@timestamp", BASE.plusMillis(1011).plusNanos(1).toString(), "key", "shared", "payload", "completion")
        );
        assertFalse(bulk.get().hasFailures());
        refresh("sequence-a", "sequence-b");
        try (var response = run(syncEsqlQueryRequest("""
            sequence by key with {"maxspan":"30s"}
            (from sequence-a metadata _id, _index, _version | keep key, payload, _id, _index, _version),
            (from sequence-b metadata _id, _index, _version | eval derived = CONCAT(payload, "/done")
              | keep key, derived, _id, _index, _version)
            | keep key, stage1._id, stage1._index, stage1._version, stage1.payload, stage2._id, stage2.derived
            | limit 1
            """).includeExecutionMetadata(true))) {
            assertValues(response.values(), List.of(List.of("shared", "A1009", "sequence-a", 1L, "value-1009", "B", "completion/done")));
            EsqlExecutionInfo.Cluster metadata = response.getExecutionInfo().getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertEquals(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, metadata.getStatus());
            assertEquals(Integer.valueOf(4), metadata.getTotalShards());
            assertEquals(Integer.valueOf(4), metadata.getSuccessfulShards());
            assertEquals(Integer.valueOf(0), metadata.getSkippedShards());
            assertEquals(Integer.valueOf(0), metadata.getFailedShards());
        }
        try (var response = run("""
            sequence with {"maxspan":"30s"} (from sequence-a), (from sequence-b)
            | stats count(*)
            | limit 1
            """)) {
            assertValues(response.values(), List.of(List.of(1L)));
        }
    }

    public void testExpandedKeysAndSelectedExpansionPayload() {
        createIndices();
        assertFalse(
            client().prepareBulk()
                .add(
                    prepareIndex("sequence-a").setId("A")
                        .setSource("@timestamp", BASE.toString(), "labels", List.of("B", "a", "B"), "payload", "anchor")
                )
                .add(
                    prepareIndex("sequence-b").setId("B")
                        .setSource("@timestamp", BASE.plusMillis(1).toString(), "labels", List.of("b", "A"), "payload", "completion")
                )
                .get()
                .hasFailures()
        );
        refresh("sequence-a", "sequence-b");
        try (var response = run("""
            sequence by normalized with {"maxspan":"30s"}
            (from sequence-a | where MV_COUNT(labels)==2
              | mv_expand labels | eval normalized=TO_LOWER(labels), selected=CONCAT(payload, "/", labels)
              | keep normalized, selected),
            (from sequence-b | mv_expand labels | eval normalized=TO_LOWER(labels), selected=CONCAT(payload, "/", labels)
              | keep normalized, selected)
            | keep normalized, stage1.selected, stage2.selected
            | sort normalized
            | limit 10
            """)) {
            assertValues(response.values(), List.of(List.of("a", "anchor/a", "completion/A"), List.of("b", "anchor/B", "completion/b")));
        }
    }

    public void testSequenceAndOrdinaryBranchExecutionMetadata() {
        createIndices();
        assertFalse(
            client().prepareBulk()
                .add(prepareIndex("sequence-a").setId("A").setSource("@timestamp", BASE.toString(), "key", "shared"))
                .add(prepareIndex("sequence-b").setId("B").setSource("@timestamp", BASE.plusNanos(1).toString(), "key", "shared"))
                .get()
                .hasFailures()
        );
        refresh("sequence-a", "sequence-b");
        try (var response = run(syncEsqlQueryRequest("""
            from (
                sequence by key with {"maxspan":"30s"}
                (from sequence-a | keep key), (from sequence-b | keep key)
                | keep key
            ), (from sequence-a | keep key)
            | stats count(*)
            | limit 1
            """).includeExecutionMetadata(true))) {
            assertValues(response.values(), List.of(List.of(2L)));
            EsqlExecutionInfo.Cluster metadata = response.getExecutionInfo().getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertEquals(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, metadata.getStatus());
            // Native stages share four PIT shards; the independent ordinary branch contributes its two shard searches.
            assertEquals(Integer.valueOf(6), metadata.getTotalShards());
            assertEquals(Integer.valueOf(6), metadata.getSuccessfulShards());
            assertEquals(Integer.valueOf(0), metadata.getFailedShards());
        }
    }

    public void testRequestFilterAppliesInsideSnapshotSearches() {
        createIndices();
        assertFalse(
            client().prepareBulk()
                .add(prepareIndex("sequence-a").setId("A1").setSource("@timestamp", BASE.toString(), "key", "shared", "payload", "allowed"))
                .add(
                    prepareIndex("sequence-a").setId("A2")
                        .setSource("@timestamp", BASE.plusMillis(1).toString(), "key", "shared", "payload", "excluded")
                )
                .add(
                    prepareIndex("sequence-b").setId("B")
                        .setSource("@timestamp", BASE.plusMillis(2).toString(), "key", "shared", "payload", "allowed")
                )
                .get()
                .hasFailures()
        );
        refresh("sequence-a", "sequence-b");
        var request = syncEsqlQueryRequest("""
            sequence by key with {"maxspan":"30s"}
            (from sequence-a metadata _id | keep key, _id),
            (from sequence-b metadata _id | keep key, _id)
            | keep stage1._id, stage2._id
            | limit 1
            """).filter(termQuery("payload", "allowed"));
        try (var response = run(request)) {
            assertValues(response.values(), List.of(List.of("A1", "B")));
        }
    }

    public void testThreeStagesTotalSpanAndDownstreamSelection() {
        createIndices();
        var bulk = client().prepareBulk();
        for (String key : List.of("valid", "outside")) {
            bulk.add(prepareIndex("sequence-a").setId("A-" + key).setSource("@timestamp", BASE.toString(), "key", key, "payload", "good"));
            bulk.add(
                prepareIndex("sequence-b").setId("B-" + key)
                    .setSource("@timestamp", BASE.plusSeconds(20).toString(), "key", key, "payload", "middle")
            );
            bulk.add(
                prepareIndex("sequence-b").setId("C-" + key)
                    .setSource(
                        "@timestamp",
                        BASE.plusSeconds(30).plusNanos(key.equals("outside") ? 1 : 0).toString(),
                        "key",
                        key,
                        "payload",
                        "end"
                    )
            );
        }
        bulk.add(
            prepareIndex("sequence-a").setId("old-good")
                .setSource("@timestamp", BASE.plusSeconds(1).toString(), "key", "selected", "payload", "good")
        );
        bulk.add(
            prepareIndex("sequence-a").setId("new-bad")
                .setSource("@timestamp", BASE.plusSeconds(2).toString(), "key", "selected", "payload", "bad")
        );
        bulk.add(
            prepareIndex("sequence-b").setId("middle")
                .setSource("@timestamp", BASE.plusSeconds(3).toString(), "key", "selected", "payload", "middle")
        );
        bulk.add(
            prepareIndex("sequence-b").setId("end")
                .setSource("@timestamp", BASE.plusSeconds(4).toString(), "key", "selected", "payload", "end")
        );
        assertFalse(bulk.get().hasFailures());
        refresh("sequence-a", "sequence-b");
        try (var response = run("""
            sequence by key with {"maxspan":"30s"}
            (from sequence-a metadata _id | keep key, payload, _id),
            (from sequence-b metadata _id | where payload=="middle" | keep key, _id),
            (from sequence-b metadata _id | where payload=="end" | keep key, _id)
            | where stage1.payload=="good"
            | keep key, stage1._id, stage2._id, stage3._id
            | limit 10
            """)) {
            assertValues(response.values(), List.of(List.of("valid", "A-valid", "B-valid", "C-valid")));
        }
    }

    public void testExpansionOrderingAcrossComputePagesAndEarlyLimit() {
        createIndices();
        List<String> labels = new ArrayList<>();
        for (int value = 0; value < 3500; value++) {
            String label = "label" + String.format(java.util.Locale.ROOT, "%05d", value);
            labels.add(label);
            labels.add(label.toUpperCase(java.util.Locale.ROOT));
        }
        assertFalse(
            client().prepareBulk()
                .add(prepareIndex("sequence-a").setId("A").setSource("@timestamp", BASE.toString(), "labels", labels))
                .add(prepareIndex("sequence-b").setId("B").setSource("@timestamp", BASE.plusMillis(1).toString(), "labels", "label00001"))
                .get()
                .hasFailures()
        );
        refresh("sequence-a", "sequence-b");
        try (var response = run("""
            sequence by normalized with {"maxspan":"30s"}
            (from sequence-a | mv_expand labels | eval normalized=TO_LOWER(labels), selected=labels | keep normalized, selected),
            (from sequence-b | mv_expand labels | eval normalized=TO_LOWER(labels) | keep normalized)
            | keep normalized, stage1.selected
            | limit 1
            """)) {
            assertValues(response.values(), List.of(List.of("label00001", "LABEL00001")));
        }
    }

    public void testSourceDisabledIndicesKeepDocValueKeys() {
        assertAcked(
            indicesAdmin().prepareCreate("sequence-no-source")
                .setMapping(
                    Map.of(
                        "_source",
                        Map.of("enabled", false),
                        "properties",
                        Map.of("@timestamp", Map.of("type", "date_nanos"), "key", Map.of("type", "keyword"))
                    )
                )
        );
        assertFalse(
            client().prepareBulk()
                .add(prepareIndex("sequence-no-source").setId("A").setSource("@timestamp", BASE.toString(), "key", "shared"))
                .add(prepareIndex("sequence-no-source").setId("B").setSource("@timestamp", BASE.plusNanos(1).toString(), "key", "shared"))
                .get()
                .hasFailures()
        );
        refresh("sequence-no-source");
        try (var response = run("""
            sequence by key with {"maxspan":"30s"}
            (from sequence-no-source metadata _id | where _id=="A" | keep key, _id),
            (from sequence-no-source metadata _id | where _id=="B" | keep key, _id)
            | keep key, stage1._id, stage2._id
            | limit 1
            """)) {
            assertValues(response.values(), List.of(List.of("shared", "A", "B")));
        }
    }

    public void testFilteredAliasKeepsItsOwnScopeInSharedSnapshot() {
        createIndices();
        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAlias("sequence-a", "sequence-source", termQuery("payload", "allowed"))
                .addAlias("sequence-a", "sequence-end", termQuery("payload", "end"))
        );
        assertFalse(
            client().prepareBulk()
                .add(
                    prepareIndex("sequence-a").setId("allowed")
                        .setSource("@timestamp", BASE.toString(), "key", "shared", "payload", "allowed")
                )
                .add(
                    prepareIndex("sequence-a").setId("excluded")
                        .setSource("@timestamp", BASE.plusMillis(1).toString(), "key", "shared", "payload", "excluded")
                )
                .add(
                    prepareIndex("sequence-a").setId("end")
                        .setSource("@timestamp", BASE.plusMillis(2).toString(), "key", "shared", "payload", "end")
                )
                .get()
                .hasFailures()
        );
        refresh("sequence-a");
        for (String terminal : List.of("sequence-end", "sequence-a")) {
            try (var response = run(Strings.format("""
                sequence by key with {"maxspan":"30s"}
                (from sequence-source metadata _id | keep key, _id),
                (from %s metadata _id | where payload=="end" | keep key, _id)
                | keep key, stage1._id, stage2._id
                | limit 10
                """, terminal))) {
                assertValues(response.values(), List.of(List.of("shared", "allowed", "end")));
            }
        }
    }

    public void testRoutedAliasKeepsItsOwnShardsInSharedSnapshot() {
        createIndices();
        String[] routing = distinctRouting();
        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(
                    IndicesAliasesRequest.AliasActions.add().index("sequence-a").alias("sequence-routed").searchRouting(routing[0])
                )
        );
        assertFalse(
            client().prepareBulk()
                .add(
                    prepareIndex("sequence-a").setId("allowed")
                        .setRouting(routing[0])
                        .setSource("@timestamp", BASE.toString(), "key", "shared", "payload", "source")
                )
                .add(
                    prepareIndex("sequence-a").setId("excluded")
                        .setRouting(routing[1])
                        .setSource("@timestamp", BASE.plusMillis(1).toString(), "key", "shared", "payload", "source")
                )
                .add(
                    prepareIndex("sequence-a").setId("end")
                        .setRouting(routing[0])
                        .setSource("@timestamp", BASE.plusMillis(2).toString(), "key", "shared", "payload", "end")
                )
                .get()
                .hasFailures()
        );
        refresh("sequence-a");
        try (var response = run("""
            sequence by key with {"maxspan":"30s"}
            (from sequence-routed metadata _id | where payload=="source" | keep key, _id),
            (from sequence-a metadata _id | where payload=="end" | keep key, _id)
            | keep key, stage1._id, stage2._id
            | limit 10
            """)) {
            assertValues(response.values(), List.of(List.of("shared", "allowed", "end")));
        }
    }

    public void testDuplicateIdsOnDifferentShardsReplayExactSelectedEvent() {
        createIndices();
        String[] routing = distinctRouting();
        var bulk = client().prepareBulk();
        for (int shard = 0; shard < routing.length; shard++) {
            String key = "key" + shard;
            bulk.add(
                prepareIndex("sequence-a").setId("duplicate")
                    .setRouting(routing[shard])
                    .setSource("@timestamp", BASE.toString(), "key", key, "payload", "source")
            );
            bulk.add(
                prepareIndex("sequence-b").setId("end" + shard)
                    .setSource("@timestamp", BASE.plusMillis(shard + 1).toString(), "key", key, "payload", "end")
            );
        }
        assertFalse(bulk.get().hasFailures());
        refresh("sequence-a", "sequence-b");
        try (var response = run("""
            sequence by key with {"maxspan":"30s"}
            (from sequence-a metadata _id | keep key, _id),
            (from sequence-b metadata _id | keep key, _id)
            | keep key, stage1._id, stage2._id
            | sort key
            | limit 10
            """)) {
            assertValues(response.values(), List.of(List.of("key0", "duplicate", "end0"), List.of("key1", "duplicate", "end1")));
        }
    }

    public void testSourcePatternMayIncludeUnmappedTimestampShards() {
        createIndices();
        assertAcked(indicesAdmin().prepareCreate("sequence-unmapped").setMapping("key", "type=keyword"));
        assertFalse(
            client().prepareBulk()
                .add(prepareIndex("sequence-a").setId("A").setSource("@timestamp", BASE.toString(), "key", "shared"))
                .add(prepareIndex("sequence-b").setId("B").setSource("@timestamp", BASE.plusMillis(1).toString(), "key", "shared"))
                .add(prepareIndex("sequence-unmapped").setId("excluded").setSource("key", "shared"))
                .get()
                .hasFailures()
        );
        refresh("sequence-a", "sequence-b", "sequence-unmapped");
        try (var response = run("""
            sequence by key with {"maxspan":"30s"}
            (from sequence-a,sequence-unmapped metadata _id | keep key, _id),
            (from sequence-b metadata _id | keep key, _id)
            | keep key, stage1._id, stage2._id
            | limit 1
            """)) {
            assertValues(response.values(), List.of(List.of("shared", "A", "B")));
        }
    }

    public void testWideSourceDisabledProjectionIgnoresSearchApiDocValueFieldLimit() {
        List<String> fields = new ArrayList<>();
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("@timestamp", Map.of("type", "date_nanos"));
        properties.put("key", Map.of("type", "keyword"));
        properties.put("phase", Map.of("type", "keyword"));
        properties.put("labels", Map.of("type", "keyword"));
        properties.put("unsigned", Map.of("type", "unsigned_long"));
        properties.put("precise", Map.of("type", "date_nanos"));
        Map<String, Object> event = new LinkedHashMap<>();
        long checksum = 0;
        for (int column = 0; column < 128; column++) {
            String field = "value" + column;
            fields.add(field);
            properties.put(field, Map.of("type", "long"));
            long value = 9_007_199_254_740_993L + column;
            event.put(field, value);
            checksum += value;
        }
        event.put("key", "shared");
        event.put("labels", List.of("z", "a", "z"));
        event.put("unsigned", "18446744073709551615");
        event.put("precise", BASE.plusNanos(7).toString());
        String projection = "key, value*, labels, unsigned, precise, checksum";
        String expression = String.join(" + ", fields);
        for (int limit : List.of(0, 100)) {
            String index = "sequence-wide-" + limit;
            assertAcked(
                indicesAdmin().prepareCreate(index)
                    .setSettings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .put("index.max_docvalue_fields_search", limit)
                    )
                    .setMapping(Map.of("_source", Map.of("enabled", false), "properties", properties))
            );
            var bulk = client().prepareBulk();
            for (String phase : List.of("A", "B")) {
                Map<String, Object> document = new LinkedHashMap<>(event);
                document.put("phase", phase);
                document.put("@timestamp", BASE.plusNanos(phase.equals("A") ? 0 : 1).toString());
                bulk.add(prepareIndex(index).setId(phase).setSource(document));
            }
            assertFalse(bulk.get().hasFailures());
            refresh(index);
            String firstStage = "from "
                + index
                + " | where phase==\"A\" | eval checksum="
                + expression
                + " | where checksum=="
                + checksum
                + " | keep "
                + projection;
            try (
                var ordinary = run(firstStage + " | limit 1");
                var sequence = run(
                    "sequence by key with {\"maxspan\":\"30s\"} ("
                        + firstStage
                        + "), (from "
                        + index
                        + " | where phase==\"B\" | keep key) | keep key, stage1.* | limit 1"
                )
            ) {
                assertEquals(133, sequence.columns().size());
                assertEquals(
                    ordinary.columns().stream().map(column -> column.type()).toList(),
                    sequence.columns().stream().map(column -> column.type()).toList()
                );
                assertValues(sequence.values(), ordinary.rows());
            }
        }
    }

    private String[] distinctRouting() {
        IndexMetadata metadata = clusterService().state().metadata().getProject().index("sequence-a");
        IndexRouting routing = IndexRouting.fromIndexMetadata(metadata);
        String first = "route0";
        int shard = routing.getShard("duplicate", first);
        for (int value = 1; value < 1000; value++) {
            String other = "route" + value;
            if (routing.getShard("duplicate", other) != shard) {
                return new String[] { first, other };
            }
        }
        throw new AssertionError("could not find routes to two different shards");
    }

    public void testUnionStagesAndMembershipSubqueriesPreserveSourceIdentity() {
        createIndices();
        assertFalse(
            client().prepareBulk()
                .add(prepareIndex("sequence-a").setId("A").setSource("@timestamp", BASE.toString(), "key", "shared", "payload", "anchor"))
                .add(
                    prepareIndex("sequence-b").setId("extra")
                        .setSource("@timestamp", BASE.plusMillis(1).toString(), "key", "shared", "payload", "extra")
                )
                .add(
                    prepareIndex("sequence-b").setId("complete")
                        .setSource("@timestamp", BASE.plusMillis(2).toString(), "key", "shared", "payload", "completion")
                )
                .add(
                    prepareIndex("sequence-b").setId("lookup")
                        .setSource("@timestamp", BASE.plusMillis(3).toString(), "key", "shared", "payload", "lookup")
                )
                .get()
                .hasFailures()
        );
        refresh("sequence-a", "sequence-b");
        try (var response = run("""
            sequence by key with {"maxspan":"30s"}
            (from (from sequence-a metadata _id, _index | where payload=="anchor" | keep key, payload, _id, _index),
                  (from sequence-b metadata _id, _index | where payload=="extra" | keep key, payload, _id, _index)),
            (from sequence-b metadata _id | where payload=="completion" | keep key, _id)
            | keep key, stage1._id, stage1._index, stage1.payload, stage2._id | limit 1
            """)) {
            assertValues(response.values(), List.of(List.of("shared", "extra", "sequence-b", "extra", "complete")));
        }
        try (var response = run("""
            sequence by key with {"maxspan":"30s"}
            (from sequence-a metadata _id
              | where key IN (from sequence-b | where payload=="lookup" | stats by key | keep key)
              | keep key, _id),
            (from sequence-b metadata _id | where payload=="completion" | keep key, _id)
            | keep key, stage1._id, stage2._id | limit 1
            """)) {
            assertValues(response.values(), List.of(List.of("shared", "A", "complete")));
        }
    }

    public void testStageAliasesDoNotHideOtherStageSourceFields() {
        createIndices();
        assertFalse(
            client().prepareBulk()
                .add(prepareIndex("sequence-a").setSource("@timestamp", BASE.toString(), "payload", "shared"))
                .add(
                    prepareIndex("sequence-b").setSource(
                        "@timestamp",
                        BASE.plusMillis(1).toString(),
                        "key",
                        "shared",
                        "payload",
                        "completion"
                    )
                )
                .get()
                .hasFailures()
        );
        refresh("sequence-a", "sequence-b");
        try (var response = run("""
            sequence by key with {"maxspan":"30s"}
            (from sequence-a | eval key=payload, payload="shadow" | keep key, payload),
            (from sequence-b | where key=="shared" AND payload=="completion" | keep key, payload)
            | keep key, stage1.payload, stage2.payload | limit 1
            """)) {
            assertValues(response.values(), List.of(List.of("shared", "shadow", "completion")));
        }
    }

    public void testScorePredicatesStayInStageCompute() {
        createIndices();
        assertFalse(
            client().prepareBulk()
                .add(prepareIndex("sequence-a").setSource("@timestamp", BASE.toString(), "key", "shared"))
                .add(prepareIndex("sequence-b").setSource("@timestamp", BASE.plusMillis(1).toString(), "key", "shared"))
                .get()
                .hasFailures()
        );
        refresh("sequence-a", "sequence-b");
        for (String predicate : List.of("", "| where _score == 0", "| where NOT (_score != 0)")) {
            try (var response = run(Strings.format("""
                sequence by key with {"maxspan":"30s"}
                (from sequence-a metadata _score %s | keep key, _score),
                (from sequence-b metadata _score %s | keep key, _score)
                | keep key, stage1._score, stage2._score | limit 1
                """, predicate, predicate))) {
                assertValues(response.values(), List.of(List.of("shared", 0.0, 0.0)));
            }
        }
    }

    private void createIndices() {
        for (String index : List.of("sequence-a", "sequence-b")) {
            assertAcked(
                indicesAdmin().prepareCreate(index)
                    .setSettings(
                        Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
                    .setMapping(
                        "@timestamp",
                        "type=" + (index.equals("sequence-a") ? "date" : "date_nanos"),
                        "key",
                        "type=keyword",
                        "labels",
                        "type=keyword",
                        "payload",
                        "type=keyword"
                    )
            );
        }
        ensureGreen("sequence-a", "sequence-b");
    }

    @After
    public void assertSequenceResourcesReleased() throws Exception {
        assertBusy(() -> {
            for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                assertEquals(0, service.getActiveContexts());
            }
        });
        ensureBlocksReleased();
    }
}
