/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockFactoryBuilder;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.esql.execution.sequence.SequenceDocValuesSearchExt;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

/** Parse the authored query through the ordinary request path and consume genuine returned schema and values. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceQueryOutputIT extends AbstractSequenceIntegTestCase {
    private static final String A = "demo-downloads";
    private static final String B = "demo-executions";
    private MockBlockFactory blocks;
    private Map<String, Object> fixture;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        assertTrue(plugins.removeIf(EsqlPlugin.class::isAssignableFrom));
        plugins.add(TrackingPlugin.class);
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    /** Track the real query blocks as well as deserialized consumer blocks. */
    public static class TrackingPlugin extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        protected BlockFactoryProvider blockFactoryProvider(BlockFactoryBuilder builder) {
            return new BlockFactoryProvider(new MockBlockFactory(builder));
        }
    }

    @Override
    protected Settings nodeSettings(int ordinal, Settings other) {
        return Settings.builder().put(super.nodeSettings(ordinal, other)).put("indices.breaker.request.limit", "128mb").build();
    }

    @Before
    public void seed() throws Exception {
        blocks = new MockBlockFactory(
            BlockFactory.builder(new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(32)).withCircuitBreaking())
        );
        fixture = objectJson(resource("fixture.synthetic.json"));
        String mapping;
        try (var builder = JsonXContent.contentBuilder()) {
            builder.map(object(fixture.get("shared_mapping")));
            mapping = BytesReference.bytes(builder).utf8ToString();
        }
        for (String index : List.of(A, B)) {
            assertAcked(indicesAdmin().prepareCreate(index).setSettings(indexSettings(1, 0)).setMapping(mapping));
        }
        ensureGreen(A, B);
        for (Object entry : list(fixture.get("documents"))) {
            Map<String, Object> document = object(entry);
            prepareIndex((String) document.get("_index")).setId((String) document.get("_id"))
                .setSource(object(document.get("_source")))
                .get();
        }
        refresh(A, B);
    }

    public void testAuthoredQueryMatchesExactResultAndConsumers() throws Exception {
        String query = resource("query.txt");
        Map<String, Object> expected = objectJson(resource("response.expected.json"));
        try (var response = query(query)) {
            Map<String, Object> actual = objectJson(json(response, false, false));
            assertEquals(expected.get("columns"), actual.get("columns"));
            assertEquals(expected.get("values"), actual.get("values"));
            Decoded decoded = checkConsumers(response);
            assertEquals(List.of("process.name", "user.name", "download_domain", "delay_seconds"), decoded.ordinaryNames);
            assertEquals(List.of("demotool.exe", "runner", "example.test", 2), decoded.rows.getFirst().ordinary);
            assertEquals(2, decoded.primary);
            assertEquals(List.of("D1", "E1"), decoded.rows.getFirst().members.stream().map(Member::id).toList());
            for (int i = 0; i < 2; i++) {
                assertEquals(object(list(fixture.get("documents")).get(i)).get("_source"), decoded.rows.getFirst().members.get(i).source);
            }
        }
    }

    /** Projecting the other stage must preserve the selected primary payloads, including retained output. */
    public void testAsymmetricStageProjectionPreservesSiblingSchema() throws Exception {
        for (int primary : List.of(1, 2)) {
            String first = "(FROM demo-downloads" + (primary == 2 ? " | KEEP host.id" : "") + ")";
            String second = "(FROM demo-executions" + (primary == 1 ? " | KEEP host.id" : "") + ")";
            String actor = primary == 1 ? "downloader" : "runner";
            for (int mode : List.of(1, 2)) {
                String presentation = " PRIMARY " + primary;
                String retention = mode == 2 ? " | RETURN MATCHES" : "";
                try (var response = query("""
                    SEQUENCE BY host.id WITH {"maxspan":"1m"}%s
                    %s, %s
                    | LIMIT 10%s
                    """.formatted(presentation, first, second, retention))) {
                    var names = response.columns().stream().map(ColumnInfo::name).toList();
                    String payload = "user.name";
                    int column = names.indexOf(payload);
                    assertTrue("Missing sibling payload [" + payload + "] in " + names, column >= 0);
                    var values = response.column(column);
                    assertTrue(values.hasNext());
                    assertEquals(actor, values.next());
                    assertFalse(values.hasNext());
                    assertFalse(names.stream().anyMatch(name -> name.startsWith("stage1.") || name.startsWith("stage2.")));
                    if (mode == 2) {
                        Decoded decoded = checkConsumers(response);
                        assertEquals(1, decoded.rows.size());
                        assertEquals(List.of("D1", "E1"), decoded.rows.getFirst().members.stream().map(Member::id).toList());
                        for (int i = 0; i < 2; i++) {
                            assertEquals(
                                object(list(fixture.get("documents")).get(i)).get("_source"),
                                decoded.rows.getFirst().members.get(i).source
                            );
                        }
                    }
                }
            }
        }
    }

    /** Explaining primary-only sequence output keeps the ordinary plan-table response. */
    public void testExplainSequenceWithoutRetentionUsesOrdinaryPlanSchema() {
        assertExplainedSequence("""
            SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY 1
            (FROM demo-downloads), (FROM demo-executions)
            | KEEP host.id, user.name | LIMIT 10
            """);
    }

    /** EXPLAIN must prepare PRIMARY and MEMBER before their plans reach analysis. */
    public void testExplainPrimarySequenceUsesOrdinaryPlanSchema() {
        assertExplainedSequence("""
            SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY 2
            (FROM demo-downloads), (FROM demo-executions)
            | EVAL download_actor = MEMBER(1, user.name)
            | KEEP user.name, download_actor | LIMIT 10
            """);
    }

    /** Inner retained originals must not turn EXPLAIN's plan-table columns into evidence columns. */
    public void testExplainRetainedSequenceUsesOrdinaryPlanSchema() {
        assertExplainedSequence("""
            SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY 2
            (FROM demo-downloads), (FROM demo-executions)
            | EVAL download_actor = MEMBER(1, user.name)
            | KEEP user.name, download_actor | LIMIT 10 | RETURN MATCHES
            """);
    }

    /** Stage membership filters the event population before matching; probe its original retention independently of outer joins. */
    public void testRetainedStageMembershipPreservesMatchedOriginals() throws Exception {
        for (String predicate : List.of(
            "host.id IN (FROM demo-executions | KEEP host.id)",
            "host.id NOT IN (FROM demo-executions | WHERE user.name == \"missing\" | KEEP host.id)",
            "(host.id, file.hash.sha256) IN (FROM demo-executions"
                + " | EVAL file.hash.sha256 = process.hash.sha256 | KEEP host.id, file.hash.sha256)"
        )) {
            try (var response = query("""
                SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY 2
                (FROM demo-downloads | WHERE %s), (FROM demo-executions)
                | KEEP user.name | LIMIT 10 | RETURN MATCHES
                """.formatted(predicate))) {
                Decoded decoded = checkConsumers(response);
                assertEquals(1, decoded.rows.size());
                assertEquals(List.of("runner"), decoded.rows.getFirst().ordinary);
                assertEquals(List.of("demo-host-1"), decoded.rows.getFirst().keys);
                assertEquals(List.of("D1", "E1"), decoded.rows.getFirst().members.stream().map(Member::id).toList());
                for (int i = 0; i < 2; i++) {
                    assertEquals(
                        object(list(fixture.get("documents")).get(i)).get("_source"),
                        decoded.rows.getFirst().members.get(i).source
                    );
                }
            }
        }
    }

    /** Retained output must reject its unsupported lowered outer join with a located author-facing error. */
    public void testRetainedOuterInSubqueryHasLocatedRejection() {
        assertRetainedOuterSubqueryRejected("host.id IN (FROM demo-executions | KEEP host.id)");
    }

    /** Negated membership has the same retained-output boundary as positive membership. */
    public void testRetainedOuterNotInSubqueryHasLocatedRejection() {
        assertRetainedOuterSubqueryRejected("host.id NOT IN (FROM demo-executions | KEEP host.id)");
    }

    /** Tuple membership must not bypass the retained-output boundary through a different expression node. */
    public void testRetainedOuterMultiColumnInSubqueryHasLocatedRejection() {
        assertRetainedOuterSubqueryRejected("(host.id, user.name) IN (FROM demo-executions | KEEP host.id, user.name)");
    }

    /** Aggregation consumes ordinary selected rows when complete-match evidence was not requested. */
    public void testPrimaryPresentationSupportsOrdinaryAggregation() throws Exception {
        for (int primary : List.of(1, 2)) {
            try (var response = query("""
                SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY %s
                (FROM demo-downloads), (FROM demo-executions)
                | EVAL other = MEMBER(%s, user.name)
                | STATS matches = COUNT(*) BY user.name, other
                | LIMIT 10
                """.formatted(primary, 3 - primary))) {
                assertEquals(List.of("matches", "user.name", "other"), response.columns().stream().map(ColumnInfo::name).toList());
                assertFalse(response.columns().stream().anyMatch(column -> column.meta() != null && column.meta().containsKey("sequence")));
                assertValues(
                    response.values(),
                    List.of(List.of(1L, primary == 1 ? "downloader" : "runner", primary == 1 ? "runner" : "downloader"))
                );
            }
        }
    }

    /** PRIMARY preserves the explicit scalar-key expansion in the sequence stage contract. */
    public void testPrimaryPresentationSupportsExpandedStageKeys() throws Exception {
        for (int i = 0; i < 2; i++) {
            Map<String, Object> document = object(list(fixture.get("documents")).get(i));
            Map<String, Object> source = object(document.get("_source"));
            source.put("host", Map.of("id", List.of("demo-host-1", "demo-host-2")));
            prepareIndex((String) document.get("_index")).setId((String) document.get("_id")).setSource(source).get();
        }
        refresh(A, B);
        for (int primary : List.of(1, 2)) {
            try (var response = query("""
                SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY %s
                (FROM demo-downloads | MV_EXPAND host.id), (FROM demo-executions | MV_EXPAND host.id)
                | EVAL other = MEMBER(%s, user.name)
                | KEEP host.id, user.name, other
                | SORT host.id | LIMIT 10
                """.formatted(primary, 3 - primary))) {
                String actor = primary == 1 ? "downloader" : "runner";
                String other = primary == 1 ? "runner" : "downloader";
                assertEquals(List.of("host.id", "user.name", "other"), response.columns().stream().map(ColumnInfo::name).toList());
                assertValues(response.values(), List.of(List.of("demo-host-1", actor, other), List.of("demo-host-2", actor, other)));
            }
        }
    }

    /** Existing event-stage extraction remains ordinary processing when no original-evidence contract is requested. */
    public void testPrimaryPresentationSupportsStageExtraction() throws Exception {
        for (String extraction : List.of("DISSECT user.name \"%{actor}\"", "GROK user.name \"%{WORD:actor}\"")) {
            for (int primary : List.of(1, 2)) {
                try (var response = query("""
                    SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY %s
                    (FROM demo-downloads | %s | KEEP host.id, actor),
                    (FROM demo-executions | %s | KEEP host.id, actor)
                    | EVAL other = MEMBER(%s, actor)
                    | KEEP actor, other | LIMIT 10
                    """.formatted(primary, extraction, extraction, 3 - primary))) {
                    assertEquals(List.of("actor", "other"), response.columns().stream().map(ColumnInfo::name).toList());
                    assertValues(
                        response.values(),
                        List.of(List.of(primary == 1 ? "downloader" : "runner", primary == 1 ? "runner" : "downloader"))
                    );
                }
            }
        }
    }

    /** Parsed stage keys and projected payloads retain the complete selected physical events through all consumers. */
    public void testRetainedParsingUsesParsedKeysAndAuthenticOriginals() throws Exception {
        for (String extraction : List.of(
            "DISSECT raw \"%{parsed_key}:%{payload}:%{unused}\"",
            "GROK raw \"^%{DATA:parsed_key}:%{WORD:payload}:%{WORD:unused}$\""
        )) {
            for (int primary : List.of(1, 2)) {
                String stage = "EVAL raw = CONCAT(host.id, \":\", user.name, \":unused\") | "
                    + extraction
                    + " | RENAME payload AS actor | DROP unused | KEEP parsed_key, actor";
                try (var response = query("""
                    SEQUENCE BY parsed_key WITH {"maxspan":"1m"} PRIMARY %s
                    (FROM demo-downloads | %s), (FROM demo-executions | %s)
                    | EVAL other = MEMBER(%s, actor)
                    | KEEP actor, other | LIMIT 10 | RETURN MATCHES
                    """.formatted(primary, stage, stage, 3 - primary))) {
                    Decoded decoded = checkConsumers(response);
                    assertRetainedFixture(decoded, primary);
                    assertEquals(List.of("actor", "other"), decoded.ordinaryNames);
                    assertEquals(
                        List.of(primary == 1 ? "downloader" : "runner", primary == 1 ? "runner" : "downloader"),
                        decoded.rows.getFirst().ordinary
                    );
                }
            }
        }
    }

    /** GROK numeric captures keep their types and physical output positions after a sibling capture is removed. */
    public void testRetainedGrokTypedCapturesAfterPartialProjection() throws Exception {
        String extraction = "GROK raw \"^%{DATA:parsed_key}:%{INT:number:int}:%{NUMBER:ratio:double}:%{WORD:unused}$\""
            + " | DROP unused | RENAME number AS parsed_number | KEEP parsed_key, parsed_number, ratio";
        for (int primary : List.of(1, 2)) {
            try (var response = query("""
                SEQUENCE BY parsed_key WITH {"maxspan":"1m"} PRIMARY %s
                (FROM demo-downloads | EVAL raw = CONCAT(host.id, ":7:1.5:unused") | %s),
                (FROM demo-executions | EVAL raw = CONCAT(host.id, ":9:2.5:unused") | %s)
                | EVAL other_number = MEMBER(%s, parsed_number), other_ratio = MEMBER(%s, ratio)
                | KEEP parsed_number, ratio, other_number, other_ratio | LIMIT 10 | RETURN MATCHES
                """.formatted(primary, extraction, extraction, 3 - primary, 3 - primary))) {
                Decoded decoded = checkConsumers(response);
                assertRetainedFixture(decoded, primary);
                assertEquals(
                    List.of("integer", "double", "integer", "double"),
                    response.columns().subList(0, 4).stream().map(ColumnInfo::outputType).toList()
                );
                assertEquals(primary == 1 ? List.of(7, 1.5, 9, 2.5) : List.of(9, 2.5, 7, 1.5), decoded.rows.getFirst().ordinary);
            }
        }
    }

    /** Null, unmatched and multivalued parsing payloads follow ordinary extraction without changing their supporting events. */
    public void testRetainedParsingNullNoMatchAndMultivaluedInput() throws Exception {
        for (String extraction : List.of("DISSECT raw \"%{parsed}:%{unused}\"", "GROK raw \"^%{WORD:parsed}:%{INT:unused:int}$\"")) {
            for (String input : List.of("null", "\"unmatched\"", "MV_APPEND(\"left:1\", \"right:2\")")) {
                Object expected = input.startsWith("MV_APPEND") ? List.of("left", "right") : null;
                for (String index : List.of(A, B)) {
                    try (
                        var standalone = query(
                            "FROM " + index + " | EVAL raw = " + input + " | " + extraction + " | KEEP parsed | LIMIT 10"
                        )
                    ) {
                        var values = standalone.column(0);
                        assertTrue(values.hasNext());
                        assertEquals(expected, values.next());
                        assertFalse(values.hasNext());
                    }
                }
                for (int primary : List.of(1, 2)) {
                    try (var response = query("""
                        SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY %s
                        (FROM demo-downloads | EVAL raw = %s | %s | KEEP host.id, parsed),
                        (FROM demo-executions | EVAL raw = %s | %s | KEEP host.id, parsed)
                        | EVAL other = MEMBER(%s, parsed)
                        | KEEP parsed, other | LIMIT 10 | RETURN MATCHES
                        """.formatted(primary, input, extraction, input, extraction, 3 - primary))) {
                        Decoded decoded = checkConsumers(response);
                        assertRetainedFixture(decoded, primary);
                        assertEquals(List.of("parsed", "other"), decoded.ordinaryNames);
                        assertEquals(Collections.nCopies(2, expected), decoded.rows.getFirst().ordinary);
                    }
                }
            }
        }
    }

    /** Parsed public metadata names can change presentation without replacing the retained identity or body. */
    public void testRetainedParsingMetadataTargetsPreserveOriginals() throws Exception {
        for (String field : List.of("_id", "_index", "_source")) {
            for (String extraction : List.of("DISSECT user.name \"%{" + field + "}\"", "GROK user.name \"%{WORD:" + field + "}\"")) {
                for (int primary : List.of(1, 2)) {
                    try (var response = query("""
                        SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY %s
                        (FROM demo-downloads | %s | KEEP host.id, %s),
                        (FROM demo-executions | %s | KEEP host.id, %s)
                        | EVAL other = MEMBER(%s, %s)
                        | KEEP %s, other | LIMIT 10 | RETURN MATCHES
                        """.formatted(primary, extraction, field, extraction, field, 3 - primary, field, field))) {
                        Decoded decoded = checkConsumers(response);
                        assertRetainedFixture(decoded, primary);
                        assertEquals(List.of(field, "other"), decoded.ordinaryNames);
                        assertEquals(
                            List.of(primary == 1 ? "downloader" : "runner", primary == 1 ? "runner" : "downloader"),
                            decoded.rows.getFirst().ordinary
                        );
                    }
                }
            }
        }
    }

    /** A multivalued parsed key requires explicit MV_EXPAND before it reaches the matcher. */
    public void testRetainedParsingRejectsMultivaluedKeys() {
        for (String extraction : List.of("DISSECT raw \"%{parsed_key}\"", "GROK raw \"^%{WORD:parsed_key}$\"")) {
            Exception failure = expectThrows(Exception.class, () -> { try (var ignored = query("""
                SEQUENCE BY parsed_key WITH {"maxspan":"1m"} PRIMARY 2
                (FROM demo-downloads | EVAL raw = MV_APPEND("left", "right") | %s),
                (FROM demo-executions | EVAL raw = "left" | %s)
                | KEEP parsed_key | LIMIT 10 | RETURN MATCHES
                """.formatted(extraction, extraction))) {} });
            assertThat(failure.getMessage(), containsString("is multivalued; use MV_EXPAND inside the stage"));
        }
    }

    /** Direct and correlated branches remain ordinary rows, including duplicates of the same selected event. */
    public void testOrdinaryUnionPreservesDirectAndCorrelatedDuplicates() throws Exception {
        String correlated = """
            SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY 2
            (FROM demo-downloads), (FROM demo-executions METADATA _id)
            | KEEP _id, user.name
            """;
        String direct = "FROM demo-executions METADATA _id | KEEP _id, user.name";
        for (int degree : List.of(1, 2)) {
            for (List<String> branches : List.of(List.of(correlated, direct), List.of(direct, correlated))) {
                String union = "FROM (" + branches.get(0) + "), (" + branches.get(1) + ")";
                try (var response = query(union + " | WHERE user.name == \"runner\" | SORT _id | LIMIT 10", degree)) {
                    assertOrdinaryUnionConsumers(
                        response,
                        List.of("_id", "user.name"),
                        List.of(List.of("E1", "runner"), List.of("E1", "runner"))
                    );
                }
                try (var response = query(union + " | STATS copies = COUNT(*) BY _id | SORT _id | LIMIT 1", degree)) {
                    assertOrdinaryUnionConsumers(response, List.of("copies", "_id"), List.of(List.of(2L, "E1")));
                }
            }
        }
    }

    /** Two sequence branches use different PRIMARY and MEMBER contexts while sharing ordinary union schema alignment. */
    public void testOrdinaryUnionKeepsIndependentSequenceContexts() throws Exception {
        String first = """
            SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY 1
            (FROM demo-downloads), (FROM demo-executions)
            | EVAL other = MEMBER(2, user.name), branch = "first"
            | KEEP branch, user.name, other
            """;
        String second = """
            SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY 2
            (FROM demo-downloads), (FROM demo-executions)
            | EVAL other = MEMBER(1, user.name), branch = "second"
            | KEEP branch, user.name, other
            """;
        for (int degree : List.of(1, 2)) {
            for (List<String> branches : List.of(List.of(first, second), List.of(second, first))) {
                try (var response = query("FROM (" + branches.get(0) + "), (" + branches.get(1) + ") | SORT branch | LIMIT 10", degree)) {
                    assertOrdinaryUnionConsumers(
                        response,
                        List.of("branch", "user.name", "other"),
                        List.of(List.of("first", "downloader", "runner"), List.of("second", "runner", "downloader"))
                    );
                    var accounting = response.getExecutionInfo().getCluster("");
                    assertEquals(Integer.valueOf(4), accounting.getTotalShards());
                    assertEquals(Integer.valueOf(4), accounting.getSuccessfulShards());
                    assertEquals(Integer.valueOf(0), accounting.getFailedShards());
                }
            }
        }
    }

    /** The same request filter reaches each direct source and each independent sequence's PIT reads. */
    public void testOrdinaryUnionAppliesRequestFilterToEveryBranch() throws Exception {
        for (int stage = 0; stage < 2; stage++) {
            Map<String, Object> document = object(list(fixture.get("documents")).get(stage));
            Map<String, Object> source = object(document.get("_source"));
            source.put("@timestamp", "2026-11-08T12:01:0" + (stage * 2) + ".000Z");
            prepareIndex((String) document.get("_index")).setId(stage == 0 ? "D2" : "E2").setSource(source).get();
        }
        refresh(A, B);
        String sequence = """
            SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY 2
            (FROM demo-downloads), (FROM demo-executions METADATA _id)
            | KEEP _id
            """;
        for (int degree : List.of(1, 2)) {
            try (
                var response = query(
                    "FROM (" + sequence + "), (" + sequence + "), (FROM demo-executions METADATA _id | KEEP _id) | SORT _id | LIMIT 10",
                    degree
                )
            ) {
                assertOrdinaryUnionConsumers(response, List.of("_id"), List.of(List.of("E1"), List.of("E1"), List.of("E1")));
            }
        }
    }

    /** Failure in a sibling sequence must release any completed or concurrently running branch resources. */
    public void testOrdinaryUnionSequenceFailureReleasesBranches() throws Exception {
        String successful = """
            SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY 2
            (FROM demo-downloads), (FROM demo-executions)
            | KEEP host.id
            """;
        String failing = successful.replace(
            "(FROM demo-downloads)",
            "(FROM demo-downloads | EVAL host.id = MV_APPEND(host.id, \"other\"))"
        );
        for (int degree : List.of(1, 2)) {
            for (List<String> branches : List.of(List.of(successful, failing), List.of(failing, successful))) {
                Exception failure = expectThrows(Exception.class, () -> {
                    try (var ignored = query("FROM (" + branches.get(0) + "), (" + branches.get(1) + ") | LIMIT 10", degree)) {}
                });
                assertThat(failure.getMessage(), containsString("is multivalued; use MV_EXPAND inside the stage"));
                for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                    assertBusy(() -> assertEquals(0, service.getActiveContexts()));
                }
            }
        }
    }

    /** Cancellation releases both live sequence PITs, or the live PIT plus an undispatched branch at degree one. */
    public void testOrdinaryUnionCancellationClosesEveryOwnedPit() throws Exception {
        String sequence = """
            SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY 2
            (FROM demo-downloads), (FROM demo-executions)
            | KEEP host.id
            """;
        for (int degree : List.of(1, 2)) {
            try (var gate = new UnionSearchGate()) {
                var request = EsqlQueryRequest.syncEsqlQueryRequest("FROM (" + sequence + "), (" + sequence + ") | LIMIT 10")
                    .pragmas(new QueryPragmas(Settings.builder().put("branch_parallel_degree", degree).build()));
                var future = client().execute(EsqlQueryAction.INSTANCE, request);
                try {
                    assertBusy(() -> {
                        assertFalse("query finished before its native sources were parked", future.isDone());
                        int contexts = 0;
                        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                            contexts += service.getActiveContexts();
                        }
                        assertEquals("each dispatched sequence owns both source shards", degree * 2, contexts);
                        assertTrue(gate.pending.get() > 0);
                    });
                    var roots = client().admin().cluster().prepareListTasks().setActions(EsqlQueryAction.NAME).get().getTasks();
                    assertEquals(1, roots.size());
                    var cancellation = new CancelTasksRequest().setTargetTaskId(roots.getFirst().taskId()).setReason("sequence union test");
                    cancellation.setWaitForCompletion(false);
                    client().execute(TransportCancelTasksAction.TYPE, cancellation).actionGet();
                    gate.release();
                    Exception failure = expectThrows(Exception.class, () -> future.actionGet(30, TimeUnit.SECONDS));
                    assertNotNull(ExceptionsHelper.unwrap(failure, TaskCancelledException.class));
                } finally {
                    gate.release();
                    try {
                        future.actionGet(30, TimeUnit.SECONDS).close();
                    } catch (Exception failure) {
                        assertTrue("cancelled query must finish after its gate is released", future.isDone());
                    }
                }
            }
            for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                assertBusy(() -> assertEquals(0, service.getActiveContexts()));
            }
            assertBusy(
                () -> assertTrue(client().admin().cluster().prepareListTasks().setActions(EsqlQueryAction.NAME).get().getTasks().isEmpty())
            );
        }
    }

    /** Park actual shard requests off transport threads, retaining each request until delegation or failure completes. */
    private final class UnionSearchGate implements AutoCloseable {
        private final CountDownLatch proceed = new CountDownLatch(1);
        private final AtomicInteger pending = new AtomicInteger();
        private volatile boolean closed;

        private UnionSearchGate() {
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService transport = MockTransportService.getInstance(node);
                transport.<ShardSearchRequest>addRequestHandlingBehavior(
                    SearchTransportService.QUERY_ACTION_NAME,
                    (handler, request, channel, task) -> {
                        if (closed
                            || request.source() == null
                            || request.source().ext().stream().noneMatch(SequenceDocValuesSearchExt.class::isInstance)) {
                            handler.messageReceived(request, channel, task);
                            return;
                        }
                        request.incRef();
                        pending.incrementAndGet();
                        transport.getThreadPool().executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {
                            @Override
                            protected void doRun() throws Exception {
                                if (proceed.await(30, TimeUnit.SECONDS) == false) {
                                    throw new IllegalStateException("sequence union search gate was not released");
                                }
                                handler.messageReceived(request, channel, task);
                            }

                            @Override
                            public void onFailure(Exception failure) {
                                if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
                                channel.sendResponse(failure);
                            }

                            @Override
                            public void onAfter() {
                                request.decRef();
                                pending.decrementAndGet();
                            }
                        });
                    }
                );
            }
        }

        private void release() {
            proceed.countDown();
        }

        @Override
        public void close() throws Exception {
            closed = true;
            release();
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node).clearAllRules();
            }
            assertBusy(() -> assertEquals(0, pending.get()));
        }
    }

    /** Ordinary union responses carry no retained-match metadata and remain ordinary through JSON and transport. */
    private void assertOrdinaryUnionConsumers(EsqlQueryResponse response, List<String> names, List<Iterable<Object>> rows)
        throws Exception {
        assertEquals(names, response.columns().stream().map(ColumnInfo::name).toList());
        assertTrue(response.columns().stream().noneMatch(column -> column.meta() != null && column.meta().containsKey("sequence")));
        assertValues(response.values(), rows);
        // JSON carries numeric values without Java's boxed width; compare both through the same JSON parser.
        try (var expected = JsonXContent.contentBuilder()) {
            expected.startObject().field("values", rows).endObject();
            assertEquals(
                objectJson(BytesReference.bytes(expected).utf8ToString()).get("values"),
                objectJson(json(response, false, false)).get("values")
            );
        }
        try (var output = new BytesStreamOutput()) {
            output.setTransportVersion(TransportVersion.current());
            response.writeTo(output);
            try (var input = output.bytes().streamInput()) {
                input.setTransportVersion(TransportVersion.current());
                try (var copy = EsqlQueryResponse.reader(blocks).read(input)) {
                    assertEquals(names, copy.columns().stream().map(ColumnInfo::name).toList());
                    assertValues(copy.values(), rows);
                }
            }
        }
    }

    /** A key is exported by every stage even though the matching source emits one shared key column. */
    public void testOtherMemberCorrelationKeyAvailableForEitherPrimary() throws Exception {
        for (int primary : List.of(2, 1)) {
            String query = """
                SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY %s
                (FROM demo-downloads), (FROM demo-executions)
                | EVAL other_host = MEMBER(%s, host.id)
                | KEEP host.id, other_host | LIMIT 10 | RETURN MATCHES
                """.formatted(primary, 3 - primary);
            try (var response = query(query)) {
                Decoded result = checkConsumers(response);
                assertEquals(1, result.rows.size());
                assertEquals(List.of("demo-host-1", "demo-host-1"), result.rows.getFirst().ordinary);
                assertEquals(List.of("D1", "E1"), result.rows.getFirst().members.stream().map(Member::id).toList());
            }
        }
    }

    public void testKeylessAndEmptyResultsKeepDescription() throws Exception {
        String query = resource("query.txt").replace(" BY host.id, match_hash", "");
        try (var response = query(query)) {
            Decoded decoded = checkConsumers(response);
            assertEquals(1, decoded.rows.size());
            assertEquals(List.of(), decoded.rows.getFirst().keys);
            assertEquals(10, response.columns().size());
        }
        try (var response = query(query.replace("user.name == \"runner\"", "user.name == \"absent\""))) {
            Decoded decoded = checkConsumers(response);
            assertEquals(0, decoded.rows.size());
            assertEquals(2, decoded.primary);
            assertEquals(10, response.columns().size());
            expectThrows(IllegalArgumentException.class, () -> fromJson(json(response, false, true)));
        }
    }

    public void testOuterEditsAndForgedNamesPreserveAuthenticMembers() throws Exception {
        String query = resource("query.txt").replace("| SORT delay_seconds ASC", """
            | EVAL user.name = "calculated", _id = "forged", _index = "forged"
            | RENAME user.name AS actor
            | SORT delay_seconds ASC
            """);
        try (var response = query(query)) {
            Decoded decoded = checkConsumers(response);
            assertEquals(List.of("process.name", "download_domain", "delay_seconds", "actor", "_id", "_index"), decoded.ordinaryNames);
            assertEquals(List.of("demotool.exe", "example.test", 2, "calculated", "forged", "forged"), decoded.rows.getFirst().ordinary);
            assertEquals("runner", object(decoded.rows.getFirst().members.get(1).source.get("user")).get("name"));
            assertEquals("E1", decoded.rows.getFirst().members.get(1).id);
            assertEquals(B, decoded.rows.getFirst().members.get(1).index);
        }
    }

    /** Ordinary calculations may use published role names without replacing protected originals or frozen keys. */
    public void testOuterEvalDoesNotShadowOriginalsOrKeys() throws Exception {
        List<String> names = List.of(
            "member_1_index",
            "member_1_id",
            "member_1_id_1",
            "member_1_source",
            "member_2_index",
            "member_2_id",
            "member_2_source",
            "match_key_1",
            "match_key_1_1"
        );
        for (int primary : List.of(1, 2)) {
            String fields = String.join(", ", names);
            String calculations = String.join(", ", names.stream().map(name -> name + " = \"ordinary\"").toList());
            try (var response = query("""
                SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY %s
                (FROM demo-downloads), (FROM demo-executions)
                | EVAL %s
                | KEEP %s | LIMIT 10 | RETURN MATCHES
                """.formatted(primary, calculations, fields))) {
                Decoded decoded = checkConsumers(response);
                assertRetainedFixture(decoded, primary);
                assertEquals(names, decoded.ordinaryNames);
                assertEquals(names.stream().map(ignored -> "ordinary").toList(), decoded.rows.getFirst().ordinary);
                List<String> columnNames = response.columns().stream().map(ColumnInfo::name).toList();
                assertEquals(columnNames.size(), new HashSet<>(columnNames).size());
            }
        }
    }

    /** Rename targets must remain ordinary query fields even when their names also label protected output roles. */
    public void testOuterRenameDoesNotShadowOriginalsOrKeys() throws Exception {
        for (int primary : List.of(1, 2)) {
            try (var response = query("""
                SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY %s
                (FROM demo-downloads), (FROM demo-executions)
                | EVAL identity = "ordinary-id", key = "ordinary-key"
                | RENAME identity AS member_1_id, key AS match_key_1
                | KEEP member_1_id, match_key_1 | LIMIT 10 | RETURN MATCHES
                """.formatted(primary))) {
                Decoded decoded = checkConsumers(response);
                assertRetainedFixture(decoded, primary);
                assertEquals(List.of("member_1_id", "match_key_1"), decoded.ordinaryNames);
                assertEquals(List.of("ordinary-id", "ordinary-key"), decoded.rows.getFirst().ordinary);
            }
        }
    }

    /** Stage calculations must not overwrite the authentic metadata captured below the stage pipeline. */
    public void testStageEvalDoesNotShadowOriginalInputBindings() throws Exception {
        for (int primary : List.of(1, 2)) {
            String value = primary == 1 ? "sequence_input0_id" : "MEMBER(1, sequence_input0_id)";
            try (var response = query("""
                SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY %s
                (FROM demo-downloads
                 | EVAL sequence_input0_index = "ordinary-index", sequence_input0_id = "ordinary-id",
                        sequence_input0_source = "ordinary-source"),
                (FROM demo-executions)
                | EVAL ordinary = %s
                | KEEP ordinary | LIMIT 10 | RETURN MATCHES
                """.formatted(primary, value))) {
                Decoded decoded = checkConsumers(response);
                assertRetainedFixture(decoded, primary);
                assertEquals(List.of("ordinary"), decoded.ordinaryNames);
                assertEquals(List.of("ordinary-id"), decoded.rows.getFirst().ordinary);
            }
        }
    }

    /** A stage rename can replace an ordinary field without removing an internal original-event binding. */
    public void testStageRenameDoesNotShadowOriginalInputBindings() throws Exception {
        for (int primary : List.of(1, 2)) {
            String value = primary == 1 ? "sequence_input0_id" : "MEMBER(1, sequence_input0_id)";
            try (var response = query("""
                SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY %s
                (FROM demo-downloads | EVAL ordinary = "ordinary-id" | RENAME ordinary AS sequence_input0_id),
                (FROM demo-executions)
                | EVAL ordinary = %s
                | KEEP ordinary | LIMIT 10 | RETURN MATCHES
                """.formatted(primary, value))) {
                Decoded decoded = checkConsumers(response);
                assertRetainedFixture(decoded, primary);
                assertEquals(List.of("ordinary"), decoded.ordinaryNames);
                assertEquals(List.of("ordinary-id"), decoded.rows.getFirst().ordinary);
            }
        }
    }

    /** An existing mapping must not become ambiguous when original-event retention adds its own bindings. */
    public void testMappedFieldDoesNotShadowOriginalInputBinding() throws Exception {
        String field = "sequence_input1_id";
        assertAcked(indicesAdmin().preparePutMapping(B).setSource(Map.of("properties", Map.of(field, Map.of("type", "keyword")))));
        Map<String, Object> original = object(object(list(fixture.get("documents")).get(1)).get("_source"));
        original.put(field, "ordinary-mapped");
        prepareIndex(B).setId("E1").setSource(original).get();
        refresh(B);
        for (int primary : List.of(1, 2)) {
            String value = primary == 2 ? field : "MEMBER(2, " + field + ")";
            try (var response = query("""
                SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY %s
                (FROM demo-downloads), (FROM demo-executions)
                | EVAL ordinary = %s
                | KEEP ordinary | LIMIT 10 | RETURN MATCHES
                """.formatted(primary, value))) {
                Decoded decoded = checkConsumers(response);
                assertEquals(primary, decoded.primary);
                assertEquals(1, decoded.rows.size());
                Row row = decoded.rows.getFirst();
                assertEquals(List.of("ordinary-mapped"), row.ordinary);
                assertEquals(List.of("demo-host-1"), row.keys);
                assertEquals(List.of("D1", "E1"), row.members.stream().map(Member::id).toList());
                assertEquals(List.of(A, B), row.members.stream().map(Member::index).toList());
                assertEquals(object(object(list(fixture.get("documents")).getFirst()).get("_source")), row.members.getFirst().source);
                assertEquals(original, row.members.get(1).source);
            }
        }
    }

    /** The compiler-name convention remains legal user data through stage/outer edits and normal result consumers. */
    public void testQuotedTemporaryNamesRemainOrdinaryFields() throws Exception {
        for (int primary : List.of(1, 2)) {
            for (String stageEdit : List.of(
                "EVAL `$$sequence_original$1` = \"stage-value\"",
                "EVAL value = \"stage-value\" | RENAME value AS `$$sequence_original$1`"
            )) {
                String stageValue = primary == 1 ? "`$$sequence_original$1`" : "MEMBER(1, `$$sequence_original$1`)";
                try (var response = query("""
                    SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY %s
                    (FROM demo-downloads | %s | KEEP host.id, `$$sequence_original$1`), (FROM demo-executions)
                    | EVAL stage_value = %s, `$$sequence_key$1` = "key-value", value = "member-value"
                    | RENAME value AS `$$sequence_member$1`
                    | KEEP stage_value, `$$sequence_key$1`, `$$sequence_member$1`
                    | LIMIT 10 | RETURN MATCHES
                    """.formatted(primary, stageEdit, stageValue))) {
                    Decoded decoded = checkConsumers(response);
                    assertRetainedFixture(decoded, primary);
                    assertEquals(List.of("stage_value", "$$sequence_key$1", "$$sequence_member$1"), decoded.ordinaryNames);
                    assertEquals(List.of("stage-value", "key-value", "member-value"), decoded.rows.getFirst().ordinary);
                }
            }
        }
    }

    /** Quoted compiler-shaped mapped names do not become reserved or disappear from the caller-visible original source. */
    public void testMappedQuotedTemporaryNamesPreserveCallerValues() throws Exception {
        List<String> fields = List.of("$$sequence_original$1", "$$sequence_original$1_1", "$$sequence_key$1", "$$sequence_member$1");
        Map<String, Object> properties = new LinkedHashMap<>();
        fields.forEach(name -> properties.put(name, Map.of("type", "keyword")));
        List<Map<String, Object>> originals = new ArrayList<>();
        for (Object value : list(fixture.get("documents"))) {
            Map<String, Object> document = object(value);
            String index = (String) document.get("_index");
            assertAcked(indicesAdmin().preparePutMapping(index).setSource(Map.of("properties", properties)));
            Map<String, Object> original = object(document.get("_source"));
            fields.forEach(name -> original.put(name, "mapped-value"));
            originals.add(original);
            prepareIndex(index).setId((String) document.get("_id")).setSource(original).get();
        }
        refresh(A, B);
        String kept = String.join(", ", fields.stream().map(name -> "`" + name + "`").toList());
        for (int primary : List.of(1, 2)) {
            try (var response = query("""
                SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY %s
                (FROM demo-downloads | KEEP host.id, %s), (FROM demo-executions | KEEP host.id, %s)
                | KEEP %s | LIMIT 10 | RETURN MATCHES
                """.formatted(primary, kept, kept, kept))) {
                Decoded decoded = checkConsumers(response);
                assertEquals(primary, decoded.primary);
                assertEquals(fields, decoded.ordinaryNames);
                assertEquals(List.of("mapped-value", "mapped-value", "mapped-value", "mapped-value"), decoded.rows.getFirst().ordinary);
                assertEquals(List.of("demo-host-1"), decoded.rows.getFirst().keys);
                assertEquals(List.of("D1", "E1"), decoded.rows.getFirst().members.stream().map(Member::id).toList());
                assertEquals(originals, decoded.rows.getFirst().members.stream().map(Member::source).toList());
            }
        }
    }

    private void assertRetainedFixture(Decoded decoded, int primary) {
        assertEquals(primary, decoded.primary);
        assertEquals(1, decoded.rows.size());
        Row row = decoded.rows.getFirst();
        assertEquals(List.of("demo-host-1"), row.keys);
        assertEquals(List.of("D1", "E1"), row.members.stream().map(Member::id).toList());
        assertEquals(List.of(A, B), row.members.stream().map(Member::index).toList());
        for (int i = 0; i < 2; i++) {
            assertEquals(object(object(list(fixture.get("documents")).get(i)).get("_source")), row.members.get(i).source);
        }
    }

    public void testProjectionDoesNotRestoreMemberOrPrimaryFields() throws Exception {
        String query = resource("query.txt");
        Exception removedMember = expectThrows(Exception.class, () -> {
            try (
                var ignored = query(
                    query.replace("| SORT delay_seconds ASC", "| EVAL restored = MEMBER(1, url.domain) | SORT delay_seconds ASC")
                )
            ) {}
        });
        assertThat(removedMember.getMessage(), containsString("Unknown column"));
        Exception removedPrimary = expectThrows(Exception.class, () -> {
            try (
                var ignored = query(
                    query.replace("| SORT delay_seconds ASC", "| EVAL restored = process.command_line | SORT delay_seconds ASC")
                )
            ) {}
        });
        assertThat(removedPrimary.getMessage(), containsString("Unknown column"));
    }

    public void testOuterFilterDoesNotChooseAnotherPredecessor() throws Exception {
        Map<String, Object> newer = object(object(list(fixture.get("documents")).getFirst()).get("_source"));
        newer.put("@timestamp", "2026-11-08T12:00:01.000Z");
        newer.put("url", Map.of("domain", "other.test"));
        prepareIndex(A).setId("D2").setSource(newer).get();
        refresh(A);
        String original = resource("query.txt");
        String unfiltered = original.replaceAll(" AND\\s+MEMBER\\(1, url\\.domain\\) == \"example\\.test\"", "");
        assertNotEquals(original, unfiltered);
        try (var response = query(unfiltered)) {
            Decoded decoded = checkConsumers(response);
            assertEquals("D2", decoded.rows.getFirst().members.getFirst().id);
        }
        try (var response = query(original)) {
            assertEquals(0, checkConsumers(response).rows.size());
        }
    }

    /** A quoted ordinary field must not acquire MEMBER semantics from a neighboring call. */
    public void testQuotedMemberNameDoesNotResolveMissingOrdinaryField() throws Exception {
        Exception missing = expectThrows(Exception.class, () -> { try (var ignored = query("""
            SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY 2
            (FROM demo-downloads), (FROM demo-executions)
            | EVAL ordinary = `MEMBER(1, url.domain)`, other_domain = MEMBER(1, url.domain)
            | KEEP ordinary, other_domain | LIMIT 10 | RETURN MATCHES
            """)) {} });
        assertThat(missing.getMessage(), containsString("Unknown column [MEMBER(1, url.domain)]"));
    }

    /** An ordinary alias cannot shadow a structurally different member binding. */
    public void testQuotedMemberAliasDoesNotShadowMember() throws Exception {
        try (var response = query("""
            SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY 2
            (FROM demo-downloads), (FROM demo-executions)
            | EVAL `MEMBER(1, url.domain)` = "ordinary"
            | EVAL ordinary = `MEMBER(1, url.domain)`, other_domain = MEMBER(1, url.domain)
            | KEEP ordinary, other_domain | LIMIT 10 | RETURN MATCHES
            """)) {
            Decoded result = checkConsumers(response);
            assertEquals(1, result.rows.size());
            assertEquals(List.of("ordinary", "example.test"), result.rows.getFirst().ordinary);
        }
    }

    /** Field discovery must retain both a real quoted mapping and the member's underlying field. */
    public void testMappedMemberDisplayNameRemainsOrdinary() throws Exception {
        String field = "MEMBER(1, payload)";
        assertAcked(indicesAdmin().preparePutMapping(B).setSource(Map.of("properties", Map.of(field, Map.of("type", "keyword")))));
        Map<String, Object> original = object(object(list(fixture.get("documents")).get(1)).get("_source"));
        original.put(field, "ordinary-mapped");
        prepareIndex(B).setId("E1").setSource(original).get();
        refresh(B);
        try (var response = query("""
            SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY 2
            (FROM demo-downloads | EVAL payload = url.domain), (FROM demo-executions)
            | EVAL ordinary = `MEMBER(1, payload)`, other_domain = MEMBER(1, payload)
            | KEEP ordinary, other_domain | LIMIT 10 | RETURN MATCHES
            """)) {
            Decoded result = checkConsumers(response);
            assertEquals(1, result.rows.size());
            assertEquals(List.of("ordinary-mapped", "example.test"), result.rows.getFirst().ordinary);
        }
    }

    /** Normal pipeline operations keep a live member demand, without publishing its internal binding. */
    public void testMemberBindingSurvivesStarAndUnrelatedEdits() throws Exception {
        try (var response = query("""
            SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY 2
            (FROM demo-downloads), (FROM demo-executions)
            | KEEP * | DROP process.command_line
            | EVAL domain = MEMBER(1, url.domain)
            | RENAME domain AS download_domain
            | WHERE MEMBER(1, url.domain) == "example.test"
            | KEEP download_domain | LIMIT 10
            """)) {
            assertEquals(List.of("download_domain"), response.columns().stream().map(ColumnInfo::name).toList());
            assertEquals(List.of(List.of("example.test")), objectJson(json(response, false, false)).get("values"));
        }
    }

    /** A forged public alias must not restore a member binding removed by KEEP. */
    public void testProjectedMemberCannotBeRestoredByQuotedAlias() throws Exception {
        Exception missing = expectThrows(Exception.class, () -> { try (var ignored = query("""
            SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY 2
            (FROM demo-downloads), (FROM demo-executions)
            | EVAL initial = MEMBER(1, url.domain)
            | KEEP initial
            | EVAL `MEMBER(1, url.domain)` = "forged"
            | EVAL restored = MEMBER(1, url.domain)
            | KEEP restored | LIMIT 10 | RETURN MATCHES
            """)) {} });
        assertThat(missing.getMessage(), containsString("Unknown column [MEMBER(1, url.domain)]"));
    }

    /** Explicit public SOURCE is an additional query column, separate from each system-retained original. */
    public void testExplicitPublicSourceRemainsSeparateFromRetainedOriginal() throws Exception {
        try (var response = query("""
            SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY 2
            (FROM demo-downloads), (FROM demo-executions METADATA _source)
            | KEEP _source | LIMIT 10 | RETURN MATCHES
            """)) {
            Decoded result = checkConsumers(response);
            assertEquals(1, result.rows.size());
            assertEquals(List.of("_source"), result.ordinaryNames);
            assertEquals(result.rows.getFirst().members.get(1).source, result.rows.getFirst().ordinary.getFirst());
            assertEquals(3L, response.columns().stream().filter(column -> column.outputType().equals("_source")).count());
        }
    }

    private void assertExplainedSequence(String statement) {
        try (var response = query("EXPLAIN (" + statement + ")")) {
            assertEquals(List.of("cluster", "node", "role", "type", "plan"), response.columns().stream().map(ColumnInfo::name).toList());
            assertTrue(response.columns().stream().allMatch(column -> column.outputType().equals("keyword")));
            assertFalse(response.columns().stream().anyMatch(column -> column.meta() != null && column.meta().containsKey("sequence")));
            var plans = response.column(4);
            assertTrue(plans.hasNext());
            boolean includesSequence = false;
            while (plans.hasNext()) {
                includesSequence |= plans.next().toString().contains("Sequence");
            }
            assertTrue("EXPLAIN must include the sequence plan", includesSequence);
        }
    }

    private void assertRetainedOuterSubqueryRejected(String predicate) {
        ParsingException error = expectThrows(ParsingException.class, () -> { try (var ignored = query("""
            SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY 2
            (FROM demo-downloads), (FROM demo-executions)
            | WHERE %s
            | LIMIT 10 | RETURN MATCHES
            """.formatted(predicate))) {} });
        assertEquals(3, error.getLineNumber());
        assertTrue("The unsupported predicate must have a source column", error.getColumnNumber() > 0);
        assertThat(error.getErrorMessage(), containsString("outside the sequence output prototype"));
    }

    private EsqlQueryResponse query(String text) {
        return query(EsqlQueryRequest.syncEsqlQueryRequest(text));
    }

    private EsqlQueryResponse query(String text, int branchParallelDegree) {
        return query(
            EsqlQueryRequest.syncEsqlQueryRequest(text)
                .pragmas(new QueryPragmas(Settings.builder().put("branch_parallel_degree", branchParallelDegree).build()))
        );
    }

    private EsqlQueryResponse query(EsqlQueryRequest request) {
        request.filter(rangeQuery("@timestamp").gte("2026-11-08T12:00:00.000Z").lt("2026-11-08T12:01:00.000Z"));
        var response = run(request);
        assertFalse(response.isPartial());
        assertFalse(response.isRunning());
        return response;
    }

    private Decoded checkConsumers(EsqlQueryResponse response) throws Exception {
        Decoded decoded = consume(response.responseInternal());
        assertEquals(decoded, fromJson(json(response, false, false)));
        assertEquals(decoded, fromJson(json(response, true, false)));
        try (var output = new BytesStreamOutput()) {
            output.setTransportVersion(TransportVersion.current());
            response.writeTo(output);
            try (var input = output.bytes().streamInput()) {
                input.setTransportVersion(TransportVersion.current());
                try (var copy = EsqlQueryResponse.reader(blocks).read(input)) {
                    assertEquals(decoded, consume(copy.responseInternal()));
                    assertEquals(decoded, fromJson(json(copy, false, false)));
                }
            }
        }
        return decoded;
    }

    private record Member(int stage, String index, String id, Map<String, Object> source) {}

    private record Row(List<Object> ordinary, List<Object> keys, List<Member> members) {}

    private record Decoded(int primary, List<String> ordinaryNames, List<Row> rows) {}

    private static Decoded consume(EsqlResponse response) {
        List<List<Object>> rows = new ArrayList<>();
        for (var row : response.rows()) {
            List<Object> values = new ArrayList<>();
            row.forEach(values::add);
            rows.add(values);
        }
        return decode(response.columns(), rows);
    }

    private static Decoded fromJson(String json) throws IOException {
        List<ColumnInfo> columns = new ArrayList<>();
        List<List<Object>> rows = new ArrayList<>();
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) throw new IllegalArgumentException("response object required");
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String field = parser.currentName();
                parser.nextToken();
                if (field.equals("all_columns")) throw new IllegalArgumentException("column elision outside consumer profile");
                if (field.equals("columns")) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY)
                        columns.add(ColumnInfoImpl.fromXContent(parser));
                } else if (field.equals("values")) {
                    for (Object item : parser.list())
                        rows.add(new ArrayList<>(list(item)));
                } else parser.skipChildren();
            }
        }
        return decode(columns, rows);
    }

    private static Decoded decode(List<? extends ColumnInfo> columns, List<List<Object>> rows) {
        var description = SequenceMatchDescription.fromColumns(columns);
        List<Row> decoded = new ArrayList<>();
        for (List<Object> values : rows) {
            var row = description.decodeRow(values, SequenceMatchDescription.SourcePolicy.ALLOW_NULL);
            decoded.add(
                new Row(
                    row.ordinary(),
                    row.keys(),
                    row.members().stream().map(member -> new Member(member.stage(), member.index(), member.id(), member.source())).toList()
                )
            );
        }
        return new Decoded(description.primaryStage(), description.ordinaryNames(), decoded);
    }

    private static Map<String, Object> object(Object value) {
        if ((value instanceof Map<?, ?>) == false) throw new IllegalArgumentException("object required");
        Map<String, Object> result = new LinkedHashMap<>();
        for (var entry : ((Map<?, ?>) value).entrySet())
            result.put((String) entry.getKey(), entry.getValue());
        return result;
    }

    private static List<?> list(Object value) {
        if (value instanceof List<?> result) return result;
        throw new IllegalArgumentException("array required");
    }

    private static Map<String, Object> objectJson(String json) throws IOException {
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
            return parser.map();
        }
    }

    private static String json(EsqlQueryResponse response, boolean pretty, boolean dropNull) throws IOException {
        var params = new ToXContent.MapParams(Map.of("drop_null_columns", Boolean.toString(dropNull)));
        try (var builder = JsonXContent.contentBuilder()) {
            if (pretty) builder.prettyPrint();
            var chunks = response.toXContentChunked(params);
            while (chunks.hasNext())
                chunks.next().toXContent(builder, params);
            return BytesReference.bytes(builder).utf8ToString();
        }
    }

    private static String resource(String name) throws IOException {
        try (var input = SequenceQueryOutputIT.class.getResourceAsStream("/sequence-query-output/" + name)) {
            if (input == null) throw new IllegalArgumentException("missing fixture " + name);
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @After
    public void release() throws Exception {
        blocks.ensureAllBlocksAreReleased();
        assertEquals(0L, blocks.breaker().getUsed());
        for (SearchService service : internalCluster().getInstances(SearchService.class))
            assertBusy(() -> assertEquals(0, service.getActiveContexts()));
        ensureBlocksReleased();
        for (CircuitBreakerService service : internalCluster().getInstances(CircuitBreakerService.class)) {
            assertBusy(() -> assertEquals(0L, service.getBreaker(CircuitBreaker.REQUEST).getUsed()));
        }
        assertAcked(indicesAdmin().prepareDelete(A, B));
    }
}
