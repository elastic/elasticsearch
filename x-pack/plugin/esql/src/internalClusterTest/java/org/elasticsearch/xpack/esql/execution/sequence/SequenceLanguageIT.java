/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.kql.KqlPlugin;
import org.junit.After;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/** Native checks for the small R6 language admissions through real searches and stage compute. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceLanguageIT extends AbstractSequenceIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), KqlPlugin.class);
    }

    /** A newer nonmatching event must not replace the indexed text match, and local scalar filters still apply. */
    public void testExactFullTextSourceConjuncts() {
        createEvents();
        for (String predicate : List.of(
            "MATCH(message, \"trigger\")",
            "MATCH(message, \"trigger\", {\"boost\": 2.0})",
            "message : \"trigger\"",
            "MATCH_PHRASE(message, \"rare trigger\")",
            "QSTR(\"message:trigger\")",
            "KQL(\"message:trigger or message:missing\")"
        )) {
            try (var response = run("""
                SEQUENCE BY key WITHIN 30s PRIMARY 2
                (FROM sequence-language-a | WHERE %s AND number == 1 | KEEP key,payload),
                (FROM sequence-language-b | WHERE KQL("message:completion") | KEEP key,payload)
                | EVAL earlier_payload=MEMBER(1,payload) | KEEP key,earlier_payload,payload | LIMIT 10
                """.formatted(predicate))) {
                assertValues(response.values(), List.of(List.of("shared", "start", "finish")));
            }
        }
    }

    /** Consecutive filters bind before KEEP; the selected output can then be enriched and read through MEMBER. */
    public void testLookupJoinAfterConsecutiveMemberWheres() {
        createEvents();
        assertAcked(
            indicesAdmin().prepareCreate("sequence-language-lookup")
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.mode", "lookup"))
                .setMapping("key", "type=keyword", "indicator", "type=keyword")
        );
        ensureGreen("sequence-language-lookup");
        prepareIndex("sequence-language-lookup").setId("known").setSource("key", "shared", "indicator", "known").get();
        refresh("sequence-language-lookup");
        for (int primary : List.of(1, 2)) {
            try (var response = run("""
                SEQUENCE BY key WITHIN 30s PRIMARY %s
                (FROM sequence-language-a | WHERE payload == "start" | KEEP key,payload,number),
                (FROM sequence-language-b | EVAL own=number+1 | WHERE payload == "finish"
                 | WHERE own == MEMBER(1,number)+1 | WHERE key == MEMBER(1,key) | KEEP key,payload)
                | LOOKUP JOIN sequence-language-lookup ON key
                | EVAL other=MEMBER(%s,payload) | KEEP key,payload,other,indicator | LIMIT 10
                """.formatted(primary, 3 - primary))) {
                assertValues(
                    response.values(),
                    List.of(List.of("shared", primary == 1 ? "start" : "finish", primary == 1 ? "finish" : "start", "known"))
                );
            }
        }
    }

    /** A native positive-score control makes the new refusal explicit while scalar and runtime scoring stay usable. */
    public void testIndexedFullTextScoreBoundary() {
        createEvents();
        try (var ordinary = run("""
            FROM sequence-language-a METADATA _score | WHERE MATCH(message,"trigger") | KEEP _score | LIMIT 1
            """)) {
            Number score = (Number) getValuesList(ordinary).getFirst().getFirst();
            assertTrue("ordinary full-text query must produce a positive relevance score", score.doubleValue() > 0);
        }
        for (String commands : List.of("WHERE MATCH(message,\"trigger\")", "EVAL searched=payload | WHERE MATCH(searched,\"start\")")) {
            Exception failure = expectThrows(Exception.class, () -> run("""
                SEQUENCE BY key WITHIN 30s PRIMARY 1
                (FROM sequence-language-a METADATA _score | %s | KEEP key,_score),
                (FROM sequence-language-b | WHERE payload == "finish" | KEEP key)
                | KEEP key,_score | LIMIT 1
                """.formatted(commands)));
            Throwable cause = ExceptionsHelper.unwrapCause(failure);
            assertThat(cause, instanceOf(VerificationException.class));
            assertThat(cause.getMessage(), containsString("does not support METADATA [_score]"));
        }
        for (var control : java.util.Map.of(
            "WHERE payload == \"start\"",
            0.0,
            "EVAL searched=TO_UPPER(payload) | WHERE MATCH(searched,\"START\")",
            1.0
        ).entrySet()) {
            try (var accepted = run("""
                SEQUENCE BY key WITHIN 30s PRIMARY 1
                (FROM sequence-language-a METADATA _score | %s | KEEP key,_score),
                (FROM sequence-language-b | WHERE payload == "finish" | KEEP key)
                | KEEP key,_score | LIMIT 1
                """.formatted(control.getKey()))) {
                assertValues(accepted.values(), List.of(List.of("shared", control.getValue())));
            }
        }
    }

    private void createEvents() {
        for (String index : List.of("sequence-language-a", "sequence-language-b")) {
            assertAcked(
                indicesAdmin().prepareCreate(index)
                    .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                    .setMapping(
                        "@timestamp",
                        "type=date_nanos",
                        "key",
                        "type=keyword",
                        "message",
                        "type=text",
                        "payload",
                        "type=keyword",
                        "number",
                        "type=long"
                    )
            );
        }
        ensureGreen("sequence-language-a", "sequence-language-b");
        assertFalse(
            client().prepareBulk()
                .add(
                    prepareIndex("sequence-language-a").setId("a")
                        .setSource(
                            "@timestamp",
                            "2026-01-01T00:00:00Z",
                            "key",
                            "shared",
                            "message",
                            "rare trigger",
                            "payload",
                            "start",
                            "number",
                            1
                        )
                )
                .add(
                    prepareIndex("sequence-language-a").setId("a-noise")
                        .setSource(
                            "@timestamp",
                            "2026-01-01T00:00:01Z",
                            "key",
                            "shared",
                            "message",
                            "noise",
                            "payload",
                            "wrong-start",
                            "number",
                            1
                        )
                )
                .add(
                    prepareIndex("sequence-language-b").setId("b")
                        .setSource(
                            "@timestamp",
                            "2026-01-01T00:00:02Z",
                            "key",
                            "shared",
                            "message",
                            "completion ready",
                            "payload",
                            "finish",
                            "number",
                            1
                        )
                )
                .add(
                    prepareIndex("sequence-language-b").setId("b-noise")
                        .setSource(
                            "@timestamp",
                            "2026-01-01T00:00:03Z",
                            "key",
                            "shared",
                            "message",
                            "noise",
                            "payload",
                            "wrong-finish",
                            "number",
                            1
                        )
                )
                .get()
                .hasFailures()
        );
        refresh("sequence-language-a", "sequence-language-b");
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
