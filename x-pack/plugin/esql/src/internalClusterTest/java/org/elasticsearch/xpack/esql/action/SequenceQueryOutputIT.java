/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockFactoryBuilder;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
        }
    }

    /** Projecting the other stage must preserve the selected primary payloads. */
    public void testAsymmetricStageProjectionPreservesSiblingSchema() throws Exception {
        for (int primary : List.of(1, 2)) {
            String first = "(FROM demo-downloads" + (primary == 2 ? " | KEEP host.id" : "") + ")";
            String second = "(FROM demo-executions" + (primary == 1 ? " | KEEP host.id" : "") + ")";
            try (var response = query("""
                SEQUENCE BY host.id WITHIN 1m PRIMARY %s
                %s, %s
                | LIMIT 10
                """.formatted(primary, first, second))) {
                var names = response.columns().stream().map(ColumnInfo::name).toList();
                int column = names.indexOf("user.name");
                assertTrue("Missing sibling payload in " + names, column >= 0);
                var values = response.column(column);
                assertTrue(values.hasNext());
                assertEquals(primary == 1 ? "downloader" : "runner", values.next());
                assertFalse(values.hasNext());
                assertFalse(names.stream().anyMatch(name -> name.startsWith("stage1.") || name.startsWith("stage2.")));
            }
        }
    }

    /** Explaining primary-only sequence output keeps the ordinary plan-table response. */
    public void testExplainSequenceUsesOrdinaryPlanSchema() {
        assertExplainedSequence("""
            SEQUENCE BY host.id WITHIN 1m PRIMARY 1
            (FROM demo-downloads), (FROM demo-executions)
            | KEEP host.id, user.name | LIMIT 10
            """);
    }

    /** EXPLAIN must prepare PRIMARY and MEMBER before their plans reach analysis. */
    public void testExplainPrimarySequenceUsesOrdinaryPlanSchema() {
        assertExplainedSequence("""
            SEQUENCE BY host.id WITHIN 1m PRIMARY 2
            (FROM demo-downloads), (FROM demo-executions)
            | EVAL download_actor = MEMBER(1, user.name)
            | KEEP user.name, download_actor | LIMIT 10
            """);
    }

    /** Stage membership filters the event population before matching. */
    public void testStageMembershipPreservesOrdinaryOutput() throws Exception {
        for (String predicate : List.of(
            "host.id IN (FROM demo-executions | KEEP host.id)",
            "host.id NOT IN (FROM demo-executions | WHERE user.name == \"missing\" | KEEP host.id)",
            "(host.id, file.hash.sha256) IN (FROM demo-executions"
                + " | EVAL file.hash.sha256 = process.hash.sha256 | KEEP host.id, file.hash.sha256)"
        )) {
            try (var response = query("""
                SEQUENCE BY host.id WITHIN 1m PRIMARY 2
                (FROM demo-downloads | WHERE %s), (FROM demo-executions)
                | KEEP user.name | LIMIT 10
                """.formatted(predicate))) {
                Decoded decoded = checkConsumers(response);
                assertEquals(1, decoded.rows.size());
                assertEquals(List.of("runner"), decoded.rows.getFirst().ordinary);
            }
        }
    }

    /** Aggregation consumes ordinary selected rows. */
    public void testPrimaryPresentationSupportsOrdinaryAggregation() throws Exception {
        for (int primary : List.of(1, 2)) {
            try (var response = query("""
                SEQUENCE BY host.id WITHIN 1m PRIMARY %s
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
                SEQUENCE BY host.id WITHIN 1m PRIMARY %s
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

    /** Event-stage extraction remains ordinary processing. */
    public void testPrimaryPresentationSupportsStageExtraction() throws Exception {
        for (String extraction : List.of("DISSECT user.name \"%{actor}\"", "GROK user.name \"%{WORD:actor}\"")) {
            for (int primary : List.of(1, 2)) {
                try (var response = query("""
                    SEQUENCE BY host.id WITHIN 1m PRIMARY %s
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

    /** Parsed keys and projected PRIMARY/MEMBER payloads survive all ordinary consumers. */
    public void testParsingUsesParsedKeysAndProcessedValues() throws Exception {
        for (String extraction : List.of(
            "DISSECT raw \"%{parsed_key}:%{payload}:%{unused}\"",
            "GROK raw \"^%{DATA:parsed_key}:%{WORD:payload}:%{WORD:unused}$\""
        )) {
            for (int primary : List.of(1, 2)) {
                String stage = "EVAL raw = CONCAT(host.id, \":\", user.name, \":unused\") | "
                    + extraction
                    + " | RENAME payload AS actor | DROP unused | KEEP parsed_key, actor";
                try (var response = query("""
                    SEQUENCE BY parsed_key WITHIN 1m PRIMARY %s
                    (FROM demo-downloads | %s), (FROM demo-executions | %s)
                    | EVAL other = MEMBER(%s, actor)
                    | KEEP actor, other | LIMIT 10
                    """.formatted(primary, stage, stage, 3 - primary))) {
                    Decoded decoded = checkConsumers(response);
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
    public void testGrokTypedCapturesAfterPartialProjection() throws Exception {
        String extraction = "GROK raw \"^%{DATA:parsed_key}:%{INT:number:int}:%{NUMBER:ratio:double}:%{WORD:unused}$\""
            + " | DROP unused | RENAME number AS parsed_number | KEEP parsed_key, parsed_number, ratio";
        for (int primary : List.of(1, 2)) {
            try (var response = query("""
                SEQUENCE BY parsed_key WITHIN 1m PRIMARY %s
                (FROM demo-downloads | EVAL raw = CONCAT(host.id, ":7:1.5:unused") | %s),
                (FROM demo-executions | EVAL raw = CONCAT(host.id, ":9:2.5:unused") | %s)
                | EVAL other_number = MEMBER(%s, parsed_number), other_ratio = MEMBER(%s, ratio)
                | KEEP parsed_number, ratio, other_number, other_ratio | LIMIT 10
                """.formatted(primary, extraction, extraction, 3 - primary, 3 - primary))) {
                Decoded decoded = checkConsumers(response);
                assertEquals(
                    List.of("integer", "double", "integer", "double"),
                    response.columns().subList(0, 4).stream().map(ColumnInfo::outputType).toList()
                );
                assertEquals(primary == 1 ? List.of(7, 1.5, 9, 2.5) : List.of(9, 2.5, 7, 1.5), decoded.rows.getFirst().ordinary);
            }
        }
    }

    /** Null, unmatched and multivalued parsing payloads follow ordinary extraction. */
    public void testParsingNullNoMatchAndMultivaluedInput() throws Exception {
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
                        SEQUENCE BY host.id WITHIN 1m PRIMARY %s
                        (FROM demo-downloads | EVAL raw = %s | %s | KEEP host.id, parsed),
                        (FROM demo-executions | EVAL raw = %s | %s | KEEP host.id, parsed)
                        | EVAL other = MEMBER(%s, parsed)
                        | KEEP parsed, other | LIMIT 10
                        """.formatted(primary, input, extraction, input, extraction, 3 - primary))) {
                        Decoded decoded = checkConsumers(response);
                        assertEquals(List.of("parsed", "other"), decoded.ordinaryNames);
                        assertEquals(Collections.nCopies(2, expected), decoded.rows.getFirst().ordinary);
                    }
                }
            }
        }
    }

    /** Parsed metadata-shaped names remain ordinary PRIMARY/MEMBER values. */
    public void testParsingMetadataTargetsRemainOrdinary() throws Exception {
        for (String field : List.of("_id", "_index", "_source")) {
            for (String extraction : List.of("DISSECT user.name \"%{" + field + "}\"", "GROK user.name \"%{WORD:" + field + "}\"")) {
                for (int primary : List.of(1, 2)) {
                    try (var response = query("""
                        SEQUENCE BY host.id WITHIN 1m PRIMARY %s
                        (FROM demo-downloads | %s | KEEP host.id, %s),
                        (FROM demo-executions | %s | KEEP host.id, %s)
                        | EVAL other = MEMBER(%s, %s)
                        | KEEP %s, other | LIMIT 10
                        """.formatted(primary, extraction, field, extraction, field, 3 - primary, field, field))) {
                        Decoded decoded = checkConsumers(response);
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
    public void testParsingRejectsMultivaluedKeys() {
        for (String extraction : List.of("DISSECT raw \"%{parsed_key}\"", "GROK raw \"^%{WORD:parsed_key}$\"")) {
            Exception failure = expectThrows(Exception.class, () -> { try (var ignored = query("""
                SEQUENCE BY parsed_key WITHIN 1m PRIMARY 2
                (FROM demo-downloads | EVAL raw = MV_APPEND("left", "right") | %s),
                (FROM demo-executions | EVAL raw = "left" | %s)
                | KEEP parsed_key | LIMIT 10
                """.formatted(extraction, extraction))) {} });
            assertThat(failure.getMessage(), containsString("is multivalued; use MV_EXPAND inside the stage"));
        }
    }

    /** A key is exported by every stage even though the matching source emits one shared key column. */
    public void testOtherMemberCorrelationKeyAvailableForEitherPrimary() throws Exception {
        for (int primary : List.of(2, 1)) {
            String query = """
                SEQUENCE BY host.id WITHIN 1m PRIMARY %s
                (FROM demo-downloads), (FROM demo-executions)
                | EVAL other_host = MEMBER(%s, host.id)
                | KEEP host.id, other_host | LIMIT 10
                """.formatted(primary, 3 - primary);
            try (var response = query(query)) {
                Decoded result = checkConsumers(response);
                assertEquals(1, result.rows.size());
                assertEquals(List.of("demo-host-1", "demo-host-1"), result.rows.getFirst().ordinary);
            }
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

    /** A post-match predicate must not backtrack to an older predecessor. */
    public void testOuterFilterDoesNotChooseAnotherPredecessor() throws Exception {
        Map<String, Object> newer = object(object(list(fixture.get("documents")).getFirst()).get("_source"));
        newer.put("@timestamp", "2026-11-08T12:00:01.000Z");
        newer.put("url", Map.of("domain", "other.test"));
        prepareIndex(A).setId("D2").setSource(newer).get();
        refresh(A);
        String original = resource("query.txt").replace("FROM demo-downloads", "FROM demo-downloads METADATA _id")
            .replace(
                "| KEEP process.name, user.name, download_domain, delay_seconds",
                "| EVAL selected_id=MEMBER(1,_id) | KEEP selected_id,delay_seconds"
            );
        String unfiltered = original.replaceAll(" AND\\s+MEMBER\\(1, url\\.domain\\) == \"example\\.test\"", "");
        assertNotEquals(original, unfiltered);
        try (var response = query(unfiltered)) {
            assertEquals(List.of(List.of("D2", 1)), checkConsumers(response).rows.stream().map(Row::ordinary).toList());
        }
        try (var response = query(original)) {
            assertEquals(0, checkConsumers(response).rows.size());
        }
    }

    /** A quoted ordinary field must not acquire MEMBER semantics from a neighboring call. */
    public void testQuotedMemberNameDoesNotResolveMissingOrdinaryField() throws Exception {
        Exception missing = expectThrows(Exception.class, () -> { try (var ignored = query("""
            SEQUENCE BY host.id WITHIN 1m PRIMARY 2
            (FROM demo-downloads), (FROM demo-executions)
            | EVAL ordinary = `MEMBER(1, url.domain)`, other_domain = MEMBER(1, url.domain)
            | KEEP ordinary, other_domain | LIMIT 10
            """)) {} });
        assertThat(missing.getMessage(), containsString("Unknown column [MEMBER(1, url.domain)]"));
    }

    /** An ordinary alias cannot shadow a structurally different member binding. */
    public void testQuotedMemberAliasDoesNotShadowMember() throws Exception {
        try (var response = query("""
            SEQUENCE BY host.id WITHIN 1m PRIMARY 2
            (FROM demo-downloads), (FROM demo-executions)
            | EVAL `MEMBER(1, url.domain)` = "ordinary"
            | EVAL ordinary = `MEMBER(1, url.domain)`, other_domain = MEMBER(1, url.domain)
            | KEEP ordinary, other_domain | LIMIT 10
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
            SEQUENCE BY host.id WITHIN 1m PRIMARY 2
            (FROM demo-downloads | EVAL payload = url.domain), (FROM demo-executions)
            | EVAL ordinary = `MEMBER(1, payload)`, other_domain = MEMBER(1, payload)
            | KEEP ordinary, other_domain | LIMIT 10
            """)) {
            Decoded result = checkConsumers(response);
            assertEquals(1, result.rows.size());
            assertEquals(List.of("ordinary-mapped", "example.test"), result.rows.getFirst().ordinary);
        }
    }

    /** Normal pipeline operations keep a live member demand, without publishing its internal binding. */
    public void testMemberBindingSurvivesStarAndUnrelatedEdits() throws Exception {
        try (var response = query("""
            SEQUENCE BY host.id WITHIN 1m PRIMARY 2
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
            SEQUENCE BY host.id WITHIN 1m PRIMARY 2
            (FROM demo-downloads), (FROM demo-executions)
            | EVAL initial = MEMBER(1, url.domain)
            | KEEP initial
            | EVAL `MEMBER(1, url.domain)` = "forged"
            | EVAL restored = MEMBER(1, url.domain)
            | KEEP restored | LIMIT 10
            """)) {} });
        assertThat(missing.getMessage(), containsString("Unknown column [MEMBER(1, url.domain)]"));
    }

    /** Explicit METADATA _source remains an ordinary requested output. */
    public void testExplicitPublicSourceRemainsOrdinary() throws Exception {
        try (var response = query("""
            SEQUENCE BY host.id WITHIN 1m PRIMARY 2
            (FROM demo-downloads), (FROM demo-executions METADATA _source)
            | KEEP _source | LIMIT 10
            """)) {
            Decoded result = checkConsumers(response);
            assertEquals(1, result.rows.size());
            assertEquals(List.of("_source"), result.ordinaryNames);
            assertEquals(object(list(fixture.get("documents")).get(1)).get("_source"), result.rows.getFirst().ordinary.getFirst());
            assertEquals(1L, response.columns().stream().filter(column -> column.outputType().equals("_source")).count());
        }
    }

    /** Every entry carries its own stage and authentic document identity, independently of edited stage fields. */
    public void testProvenanceIncludesEveryStageForFirstAndLastPrimary() throws Exception {
        Map<String, Object> third = object(object(list(fixture.get("documents")).get(1)).get("_source"));
        third.put("@timestamp", "2026-11-08T12:00:04.000Z");
        third.put("user", Map.of("name", "last"));
        prepareIndex(B).setId("E2").setSource(third).get();
        refresh(B);
        for (int stages : List.of(2, 3)) {
            for (int primary : List.of(1, stages)) {
                String first = "(FROM demo-downloads | EVAL role=\"first\", _id=\"edited-1\", _index=\"edited-index-1\")";
                String second =
                    "(FROM demo-executions | WHERE user.name==\"runner\" | EVAL role=\"second\", _id=\"edited-2\", _index=\"edited-index-2\")";
                String last = stages == 2
                    ? ""
                    : ", (FROM demo-executions | WHERE user.name==\"last\" | EVAL role=\"third\", _id=\"edited-3\", _index=\"edited-index-3\")";
                try (
                    var response = query(
                        "SEQUENCE BY host.id WITHIN 1m PRIMARY "
                            + primary
                            + " "
                            + first
                            + ", "
                            + second
                            + last
                            + " | KEEP role,_id,_index,sequence_provenance | LIMIT 10"
                    )
                ) {
                    Decoded decoded = checkConsumers(response);
                    assertEquals(1, decoded.rows.size());
                    List<Object> row = decoded.rows.getFirst().ordinary;
                    assertEquals(primary == 1 ? "first" : stages == 2 ? "second" : "third", row.get(0));
                    assertEquals("edited-" + primary, row.get(1));
                    assertEquals("edited-index-" + primary, row.get(2));
                    List<SequenceProvenance.Member> expected = new ArrayList<>(
                        List.of(new SequenceProvenance.Member(1, A, "D1"), new SequenceProvenance.Member(2, B, "E1"))
                    );
                    if (stages == 3) expected.add(new SequenceProvenance.Member(3, B, "E2"));
                    assertEquals(expected, SequenceProvenance.decode(row.get(3)));
                }
            }
        }
    }

    /** Expanded rows preserve the supporting document id rather than inventing a row id. */
    public void testProvenanceExpandedRowsKeepDocumentIdentity() throws Exception {
        for (Object value : list(fixture.get("documents"))) {
            Map<String, Object> document = object(value);
            Map<String, Object> source = object(document.get("_source"));
            source.put("host", Map.of("id", List.of("one", "two")));
            prepareIndex((String) document.get("_index")).setId((String) document.get("_id")).setSource(source).get();
        }
        refresh(A, B);
        for (int primary : List.of(1, 2)) {
            try (var response = query("""
                SEQUENCE BY host.id WITHIN 1m PRIMARY %s
                (FROM demo-downloads | MV_EXPAND host.id), (FROM demo-executions | MV_EXPAND host.id)
                | KEEP host.id,sequence_provenance | SORT host.id | LIMIT 10
                """.formatted(primary))) {
                Decoded decoded = checkConsumers(response);
                assertEquals(List.of("one", "two"), decoded.rows.stream().map(row -> row.ordinary.getFirst()).toList());
                for (Row row : decoded.rows) {
                    assertEquals(
                        List.of(new SequenceProvenance.Member(1, A, "D1"), new SequenceProvenance.Member(2, B, "E1")),
                        SequenceProvenance.decode(row.ordinary.get(1))
                    );
                }
            }
        }
    }

    /** A consumer can associate each row's ordinary values with exactly that row's selected events. */
    public void testProvenanceAssociatesValuesWithEachMatch() throws Exception {
        for (int stage = 0; stage < 2; stage++) {
            Map<String, Object> document = object(list(fixture.get("documents")).get(stage));
            Map<String, Object> source = object(document.get("_source"));
            source.put("@timestamp", stage == 0 ? "2026-11-08T12:00:03.000Z" : "2026-11-08T12:00:04.000Z");
            source.put("user", Map.of("name", stage == 0 ? "later-downloader" : "later-runner"));
            prepareIndex(stage == 0 ? A : B).setId(stage == 0 ? "D2" : "E2").setSource(source).get();
        }
        refresh(A, B);
        for (int primary : List.of(1, 2)) {
            try (var response = query("""
                SEQUENCE BY host.id WITHIN 1m PRIMARY %s (FROM demo-downloads), (FROM demo-executions)
                | KEEP user.name,sequence_provenance | LIMIT 10
                """.formatted(primary))) {
                Decoded decoded = checkConsumers(response);
                assertEquals(2, decoded.rows.size());
                assertEquals(
                    primary == 1 ? List.of("downloader", "later-downloader") : List.of("later-runner", "runner"),
                    decoded.rows.stream().map(row -> (String) row.ordinary.getFirst()).sorted().toList()
                );
                for (Row row : decoded.rows) {
                    String actor = (String) row.ordinary.getFirst();
                    boolean later = actor.startsWith("later-");
                    assertEquals(primary == 1 ? (later ? "later-downloader" : "downloader") : (later ? "later-runner" : "runner"), actor);
                    assertEquals(
                        List.of(
                            new SequenceProvenance.Member(1, A, later ? "D2" : "D1"),
                            new SequenceProvenance.Member(2, B, later ? "E2" : "E1")
                        ),
                        SequenceProvenance.decode(row.ordinary.get(1))
                    );
                }
            }
        }
    }

    /** Public projection and edits remain ordinary even when their names resemble the compiler marker. */
    public void testProvenanceCanBeDroppedRenamedAndReplacedWithoutNameCapture() throws Exception {
        String base = "SEQUENCE BY host.id WITHIN 1m PRIMARY 2 (FROM demo-downloads), (FROM demo-executions)";
        try (var response = query(base + " | DROP sequence_provenance | KEEP user.name")) {
            assertEquals(List.of(List.of("runner")), checkConsumers(response).rows.stream().map(Row::ordinary).toList());
        }
        try (var response = query(base + " | KEEP sequence_provenance | RENAME sequence_provenance AS events")) {
            assertEquals(List.of("events"), response.columns().stream().map(ColumnInfo::name).toList());
            assertEquals(
                List.of(new SequenceProvenance.Member(1, A, "D1"), new SequenceProvenance.Member(2, B, "E1")),
                SequenceProvenance.decode(checkConsumers(response).rows.getFirst().ordinary.getFirst())
            );
        }
        try (var response = query(base + " | EVAL sequence_provenance=\"replacement\" | KEEP sequence_provenance")) {
            assertEquals(List.of(List.of("replacement")), checkConsumers(response).rows.stream().map(Row::ordinary).toList());
        }
    }

    public void testPrimaryMappingCannotReplaceGeneratedProvenance() throws Exception {
        String step = "FROM demo-executions";
        String statement = "SEQUENCE BY host.id WITHIN 1m PRIMARY 2 (FROM demo-downloads), (" + step + ") | KEEP sequence_provenance";
        var expected = List.of(new SequenceProvenance.Member(1, A, "D1"), new SequenceProvenance.Member(2, B, "E1"));
        try (var response = query(statement)) {
            assertEquals(
                List.of(expected),
                checkConsumers(response).rows.stream().map(row -> SequenceProvenance.decode(row.ordinary.getFirst())).toList()
            );
        }

        assertAcked(
            indicesAdmin().preparePutMapping(B).setSource(Map.of("properties", Map.of("sequence_provenance", Map.of("type", "keyword"))))
        );
        for (String query : List.of(statement, statement.replace("KEEP sequence_provenance", "KEEP user.name"))) {
            Exception failure = expectThrows(Exception.class, () -> { try (var ignored = query(query)) {} });
            assertThat(
                failure.getMessage(),
                containsString("Column [sequence_provenance] is reserved for SEQUENCE output. DROP or RENAME it inside the PRIMARY step.")
            );
        }

        for (String allowed : List.of(
            statement.replace("PRIMARY 2", "PRIMARY 1"),
            statement.replace(step, step + " | DROP sequence_provenance"),
            statement.replace(step, step + " | RENAME sequence_provenance AS source_provenance")
        )) {
            try (var response = query(allowed)) {
                assertEquals(
                    List.of(expected),
                    checkConsumers(response).rows.stream().map(row -> SequenceProvenance.decode(row.ordinary.getFirst())).toList()
                );
            }
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

    private EsqlQueryResponse query(String text) {
        return query(EsqlQueryRequest.syncEsqlQueryRequest(text));
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

    private record Row(List<Object> ordinary) {}

    private record Decoded(List<String> ordinaryNames, List<Row> rows) {}

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
        return new Decoded(columns.stream().map(ColumnInfo::name).toList(), rows.stream().map(Row::new).toList());
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
