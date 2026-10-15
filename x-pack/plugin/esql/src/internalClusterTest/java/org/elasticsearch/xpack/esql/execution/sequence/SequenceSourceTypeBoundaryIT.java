/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.After;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

/** Checks source type errors precede snapshot execution while ordinary membership queries keep their full type support. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class SequenceSourceTypeBoundaryIT extends AbstractEsqlIntegTestCase {
    public void testRetainedSpatialFieldFailsVerificationWhilePrunedFieldWorks() {
        createEvents();
        var error = expectThrows(VerificationException.class, () -> run("""
            sequence by key with {"maxspan":"30s"}
            (from sequence-spatial | where phase=="A"), (from sequence-spatial | where phase=="B")
            | keep key, stage1.location | limit 1
            """));
        assertThat(error.getMessage(), containsString("SEQUENCE stage [1] cannot read source column [location] of type [geo_point]"));
        for (String source : List.of("from sequence-spatial", "from (from sequence-spatial), (from sequence-spatial)")) {
            try (var result = run(Strings.format("""
                sequence by key with {"maxspan":"30s"}
                (%s | where phase=="A"), (from sequence-spatial | where phase=="B")
                | keep key | limit 1
                """, source))) {
                assertValues(result.values(), List.of(List.of("shared")));
            }
        }
    }

    public void testSpatialStagePredicateFailsVerification() {
        createEvents();
        var error = expectThrows(VerificationException.class, () -> run("""
            sequence by key with {"maxspan":"30s"}
            (from sequence-spatial | where phase=="A" AND ST_X(location)>0 | keep key),
            (from sequence-spatial | where phase=="B" | keep key)
            | keep key | limit 1
            """));
        assertThat(error.getMessage(), containsString("cannot read source column [location] of type [geo_point]"));
    }

    public void testSpatialMembershipSubqueryUsesOrdinaryExecution() {
        createEvents();
        try (var result = run("""
            sequence by key with {"maxspan":"30s"}
            (from sequence-spatial
              | where phase=="A" AND key IN (from sequence-spatial | where ST_X(location)>0 | keep key)
              | keep key),
            (from sequence-spatial | where phase=="B" | keep key)
            | keep key | limit 1
            """)) {
            assertValues(result.values(), List.of(List.of("shared")));
        }
    }

    /** Source values which ordinary LOAD can read must fail before sequence execution when they are required. */
    public void testLoadSourceOnlyPayloadPredicateAndKeyFailVerification() {
        createSourceOnlyEvents();
        try (var result = run("""
            SET unmapped_fields="LOAD";
            from sequence-source-only | sort phase | keep phase, source_payload, source_key
            """)) {
            assertValues(result.values(), List.of(List.of("A", "payload-A", "shared"), List.of("B", "payload-B", "shared")));
        }
        for (SourceOnlyQuery query : sourceOnlyQueries()) {
            var error = expectThrows(VerificationException.class, () -> run("SET unmapped_fields=\"LOAD\"; " + query.text));
            assertThat(error.getMessage(), containsString("SEQUENCE stage [1] cannot load unmapped source column [" + query.column + "]"));
        }
    }

    /** LOAD_ALL retains its existing command guardrail even when an explicit projection names the source-only fields. */
    public void testLoadAllSourceOnlyPayloadPredicateAndKeyKeepCommandGuardrail() {
        assumeTrue("Requires OPTIONAL_FIELDS_LOAD_ALL_V2", EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_V2.isEnabled());
        createSourceOnlyEvents();
        for (SourceOnlyQuery query : sourceOnlyQueries()) {
            var error = expectThrows(VerificationException.class, () -> run("SET unmapped_fields=\"LOAD_ALL\"; " + query.text));
            assertThat(error.getMessage(), containsString("unmapped_fields=\"LOAD_ALL\" only supports"));
        }
    }

    /** Payload pruning must remove an unused source-only input before checking the sequence reader's boundary. */
    public void testUnusedSourceOnlyLoadAttributesArePruned() {
        createSourceOnlyEvents();
        for (String projection : List.of("keep key, source_payload", "eval unused=source_payload | keep key, unused")) {
            try (var result = run(Strings.format("""
                SET unmapped_fields="LOAD";
                sequence by key with {"maxspan":"30s"}
                (from sequence-source-only | where phase=="A" | %s),
                (from sequence-source-only | where phase=="B" | %s)
                | keep key | limit 1
                """, projection, projection))) {
                assertValues(result.values(), List.of(List.of("shared")));
            }
        }
    }

    /** NULLIFY provides null inputs rather than source loading, including a keyword null computed inside each stage. */
    public void testNullifySourceOnlyAttributesRemainTypedNulls() {
        createSourceOnlyEvents();
        try (var result = run("""
            SET unmapped_fields="NULLIFY";
            sequence by key with {"maxspan":"30s"}
            (from sequence-source-only | where phase=="A"
              | eval typed_null=source_payload::keyword | keep key, source_payload, typed_null),
            (from sequence-source-only | where phase=="B"
              | eval typed_null=source_payload::keyword | keep key, source_payload, typed_null)
            | keep key, stage1.source_payload, stage1.typed_null, stage2.source_payload, stage2.typed_null
            | limit 1
            """)) {
            assertColumnTypes(result.columns(), List.of("keyword", "null", "keyword", "null", "keyword"));
            assertValues(result.values(), List.of(Arrays.asList("shared", null, null, null, null)));
        }
    }

    /** Source timestamp identity remains required after KEEP, while different uniform timestamp types may serve separate stages. */
    public void testHiddenMixedTimestampMappingFailsWhileUniformStagesSucceed() {
        for (String type : List.of("date", "date_nanos")) {
            String index = type.equals("date") ? "sequence-timestamp-millis" : "sequence-timestamp-nanos";
            assertAcked(indicesAdmin().prepareCreate(index).setMapping("@timestamp", "type=" + type, "key", "type=keyword"));
            prepareIndex(index).setId(type)
                .setSource(
                    "@timestamp",
                    type.equals("date") ? "2026-09-25T21:14:51.389Z" : "2026-09-25T21:14:51.389000001Z",
                    "key",
                    "shared"
                )
                .get();
        }
        refresh("sequence-timestamp-millis", "sequence-timestamp-nanos");
        var error = expectThrows(VerificationException.class, () -> run("""
            sequence by key with {"maxspan":"30s"}
            (from sequence-timestamp-* | keep key), (from sequence-timestamp-nanos | keep key)
            | keep key | limit 1
            """));
        assertThat(error.getMessage(), containsString("SEQUENCE source [@timestamp] requires a consistent mapped type"));
        try (var result = run("""
            sequence by key with {"maxspan":"30s"}
            (from sequence-timestamp-millis | keep key), (from sequence-timestamp-nanos | keep key)
            | keep key | limit 1
            """)) {
            assertValues(result.values(), List.of(List.of("shared")));
        }
    }

    /** Matching requires one original event time even when KEEP hides it or MV_EXPAND would scalarize the visible value. */
    public void testMultivaluedOriginalTimestampCannotBeHiddenOrExpanded() {
        assertAcked(
            indicesAdmin().prepareCreate("sequence-multitime")
                .setMapping("@timestamp", "type=date_nanos", "key", "type=keyword", "phase", "type=keyword")
        );
        Map<String, String> timestamps = Map.of("A", "2026-09-25T21:14:51.389Z", "B", "2026-09-25T21:14:52.389Z");
        for (String invalidStage : List.of("A", "B")) {
            var bulk = client().prepareBulk();
            for (String stage : List.of("A", "B")) {
                List<String> times = stage.equals(invalidStage)
                    ? List.of(timestamps.get(stage), "2026-09-25T21:16:31.389Z")
                    : List.of(timestamps.get(stage));
                bulk.add(prepareIndex("sequence-multitime").setId(stage).setSource("@timestamp", times, "key", "shared", "phase", stage));
            }
            assertFalse(bulk.get().hasFailures());
            refresh("sequence-multitime");
            for (String projection : List.of(
                "keep key, @timestamp",
                "keep key",
                "mv_expand @timestamp | keep key, @timestamp",
                "mv_expand @timestamp | keep key"
            )) {
                String output = projection.endsWith("@timestamp") ? "key, stage1.@timestamp, stage2.@timestamp" : "key";
                RuntimeException error = expectThrows(RuntimeException.class, () -> run(Strings.format("""
                    sequence by key with {"maxspan":"30s"}
                    (from sequence-multitime | where phase=="A" | %s),
                    (from sequence-multitime | where phase=="B" | %s)
                    | keep %s | limit 1
                    """, projection, projection, output)));
                assertThat(
                    ExceptionsHelper.stackTrace(error),
                    containsString("SEQUENCE source [@timestamp] must hold a single mapped date or date_nanos value")
                );
            }
        }
        // Single-valued arrays remain valid source timestamps; the mapping representation contains one date value.
        prepareIndex("sequence-multitime").setId("B")
            .setSource("@timestamp", List.of(timestamps.get("B")), "key", "shared", "phase", "B")
            .get();
        refresh("sequence-multitime");
        try (var result = run("""
            sequence by key with {"maxspan":"30s"}
            (from sequence-multitime | where phase=="A" | keep key),
            (from sequence-multitime | where phase=="B" | keep key)
            | keep key | limit 1
            """)) {
            assertValues(result.values(), List.of(List.of("shared")));
        }
    }

    private static List<SourceOnlyQuery> sourceOnlyQueries() {
        return List.of(
            new SourceOnlyQuery(
                sourceOnlySequence("key", "| keep key, source_payload", "| keep key, stage1.source_payload"),
                "source_payload"
            ),
            new SourceOnlyQuery(
                sourceOnlySequence("key", "| where source_payload==\"payload-A\" | keep key", "| keep key"),
                "source_payload"
            ),
            new SourceOnlyQuery(sourceOnlySequence("source_key", "| keep source_key", "| keep source_key"), "source_key")
        );
    }

    private static String sourceOnlySequence(String key, String firstStage, String projection) {
        return Strings.format("""
            sequence by %s with {"maxspan":"30s"}
            (from sequence-source-only | where phase=="A" %s),
            (from sequence-source-only | where phase=="B" | keep %s)
            %s | limit 1
            """, key, firstStage, key, projection);
    }

    private void createSourceOnlyEvents() {
        assertAcked(
            indicesAdmin().prepareCreate("sequence-source-only")
                .setMapping(
                    Map.of(
                        "dynamic",
                        false,
                        "properties",
                        Map.of(
                            "@timestamp",
                            Map.of("type", "date_nanos"),
                            "key",
                            Map.of("type", "keyword"),
                            "phase",
                            Map.of("type", "keyword")
                        )
                    )
                )
        );
        assertFalse(
            client().prepareBulk()
                .add(
                    prepareIndex("sequence-source-only").setId("A")
                        .setSource(
                            "@timestamp",
                            "2026-09-25T21:14:51.389Z",
                            "key",
                            "shared",
                            "phase",
                            "A",
                            "source_payload",
                            "payload-A",
                            "source_key",
                            "shared"
                        )
                )
                .add(
                    prepareIndex("sequence-source-only").setId("B")
                        .setSource(
                            "@timestamp",
                            "2026-09-25T21:14:51.389000001Z",
                            "key",
                            "shared",
                            "phase",
                            "B",
                            "source_payload",
                            "payload-B",
                            "source_key",
                            "shared"
                        )
                )
                .get()
                .hasFailures()
        );
        refresh("sequence-source-only");
    }

    private record SourceOnlyQuery(String text, String column) {}

    private void createEvents() {
        assertAcked(
            indicesAdmin().prepareCreate("sequence-spatial")
                .setMapping("@timestamp", "type=date_nanos", "key", "type=keyword", "phase", "type=keyword", "location", "type=geo_point")
        );
        assertFalse(
            client().prepareBulk()
                .add(
                    prepareIndex("sequence-spatial").setId("A")
                        .setSource("@timestamp", "2026-09-25T21:14:51.389Z", "key", "shared", "phase", "A", "location", "POINT (1 2)")
                )
                .add(
                    prepareIndex("sequence-spatial").setId("B")
                        .setSource("@timestamp", "2026-09-25T21:14:51.389000001Z", "key", "shared", "phase", "B", "location", "POINT (1 2)")
                )
                .get()
                .hasFailures()
        );
        refresh("sequence-spatial");
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
