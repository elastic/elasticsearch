/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.expression.function.fulltext.RuntimeBm25RowStats;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

/**
 * The 400s of {@code MATCH} and {@code MATCH_PHRASE} with {@code similarity_function: bm25} on runtime text, Stage 1.
 * Every case pins the minimum transport version, because the test analyzer otherwise picks a random one and an older
 * version fails every query that sets the option.
 */
public class RuntimeBm25VerifierTests extends ESTestCase {

    private static final String TEXT = "eval t = to_text(concat(title, body))";
    private static final String BM25 = "{\"similarity_function\": \"bm25\"}";

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    private static TestAnalyzer bm25() {
        return analyzer().addIndex("test", "mapping-full_text_search.json")
            .stripErrorPrefix(true)
            .minimumTransportVersion(TransportVersion.current());
    }

    public void testSupportedQueries() {
        bm25().query("from test metadata _score | " + TEXT + " | where match(t, \"cat\", " + BM25 + ") | sort _score desc");
        bm25().query("from test metadata _score | " + TEXT + " | where match_phrase(t, \"cat dog\", " + BM25 + ")");
        bm25().query("from test metadata _score | " + TEXT + " | where id > 1 | where match(t, \"cat\", " + BM25 + ") | keep t, _score");
        bm25().query("from test | " + TEXT + " | where match(t, \"cat\", {\"similarity_function\": \"boolean\"})");
        bm25().query(
            "from test metadata _score | eval t = to_text(concat(title, body), {\"analyzer\": \"whitespace\"}) "
                + "| where match(t, \"cat\", {\"similarity_function\": \"bm25\", \"analyzer\": \"whitespace\"})"
        );
    }

    public void testInvalidValue() {
        bm25().error(
            "from test | " + TEXT + " | where match(t, \"cat\", {\"similarity_function\": \"bm42\"})",
            allOf(containsString("Invalid option [similarity_function]"), containsString("expected one of [bm25, boolean]"))
        );
    }

    public void testIndexedField() {
        bm25().error(
            "from test | where match(title, \"cat\", " + BM25 + ")",
            containsString("[MATCH] option [similarity_function] is only supported when [title] is a non-index-mapped field or expression")
        );
        bm25().error(
            "from test | where match_phrase(title, \"cat\", {\"similarity_function\": \"boolean\"})",
            containsString("[MATCH_PHRASE] option [similarity_function] is only supported when [title] is a non-index-mapped field")
        );
    }

    public void testNonTextField() {
        bm25().error(
            "from test | eval k = concat(title, body) | where match(k, \"cat\", " + BM25 + ")",
            containsString(
                "[similarity_function] option is not supported for [MATCH] on non-index-mapped, non-TEXT field [k]; "
                    + "convert the field with TO_TEXT(k) to score it with BM25"
            )
        );
        bm25().error(
            "from test | eval k = concat(title, body) | where match_phrase(k, \"cat\", " + BM25 + ")",
            containsString(
                "[MATCH_PHRASE] option [similarity_function] is not supported on non-index-mapped, non-TEXT field [k]; "
                    + "convert the field with TO_TEXT(k) to score it with BM25"
            )
        );
    }

    public void testTermExpandingOptions() {
        Map<String, String> values = Map.of(
            "fuzziness",
            "\"AUTO\"",
            "prefix_length",
            "1",
            "max_expansions",
            "10",
            "fuzzy_transpositions",
            "true",
            "fuzzy_rewrite",
            "\"constant_score\""
        );
        values.forEach(
            (key, value) -> bm25().error(
                "from test | " + TEXT + " | where match(t, \"cat\", {\"similarity_function\": \"bm25\", \"" + key + "\": " + value + "})",
                containsString("[MATCH] option [" + key + "] is not supported with [similarity_function] = [bm25]")
            )
        );
    }

    public void testQueryAnalyzerMustBeTheValuesAnalyzer() {
        bm25().error(
            "from test | " + TEXT + " | where match(t, \"cat\", {\"similarity_function\": \"bm25\", \"analyzer\": \"whitespace\"})",
            containsString("[MATCH] option [analyzer] must be [standard] with [similarity_function] = [bm25]")
        );
    }

    public void testInsideScore() {
        bm25().error(
            "from test metadata _score | " + TEXT + " | eval s = score(match(t, \"cat\", " + BM25 + "))",
            containsString("[MATCH] with [similarity_function] = [bm25] is not supported inside SCORE in this version")
        );
    }

    /** Commands that decide which rows exist before the MATCH; the fix is to move them after it. */
    public void testRowLimitingCommandBeforeMatch() {
        Map<String, String> commands = Map.of("limit 10", "limit 10", "sort title | limit 10", "limit 10", "sample 0.5", "sample 0.5");
        for (String metadata : new String[] { " metadata _score", "" }) {
            commands.forEach(
                (command, blocker) -> bm25().error(
                    "from test" + metadata + " | " + TEXT + " | " + command + " | where match(t, \"cat\", " + BM25 + ")",
                    containsString(
                        "[MATCH] with [similarity_function] = [bm25] cannot compute statistics after ["
                            + blocker
                            + "], which decides which rows exist before it; move ["
                            + blocker
                            + "] after the WHERE, or set [similarity_function] to [boolean]"
                    )
                )
            );
        }
    }

    /** Commands Stage 1 does not support before the MATCH, which have no "move it" fix. */
    public void testOtherCommandBeforeMatch() {
        for (String command : new String[] { "mv_expand t", "dissect body \"%{a}\"", "grok body \"%{WORD:a}\"" }) {
            bm25().error(
                "from test metadata _score | " + TEXT + " | " + command + " | where match(t, \"cat\", " + BM25 + ")",
                containsString(
                    "[MATCH] with [similarity_function] = [bm25] cannot compute statistics after ["
                        + command
                        + "] in this version; set [similarity_function] to [boolean]"
                )
            );
        }
    }

    public void testOtherFullTextFunctionInTheStretch() {
        bm25().error(
            "from test metadata _score | where kql(\"title:cat\") | " + TEXT + " | where match(t, \"cat\", " + BM25 + ")",
            containsString(
                "[kql(\"title:cat\")] cannot be used in the same part of the query as [MATCH] with [similarity_function] = [bm25]"
            )
        );
    }

    /** A filter or sort reading _score bounds a stretch, so a BM25 call on each side makes two stretches. */
    public void testSecondStretch() {
        bm25().error(
            "from test metadata _score | "
                + TEXT
                + " | where match(t, \"cat\", "
                + BM25
                + ") | sort _score desc | where match(t, \"dog\", "
                + BM25
                + ")",
            containsString("[MATCH] with [similarity_function] = [bm25] can be used in only one part of a query in this version")
        );
    }

    /** An older node or remote rejects the key even with {@code boolean}, so any value fails at the coordinator. */
    public void testOlderMinimumVersion() {
        TransportVersion older = TransportVersionUtils.randomVersionNotSupporting(RuntimeBm25RowStats.ESQL_RUNTIME_MATCH_BM25);
        for (String value : new String[] { "bm25", "boolean" }) {
            bm25().minimumTransportVersion(older)
                .error(
                    "from test metadata _score | " + TEXT + " | where match(t, \"cat\", {\"similarity_function\": \"" + value + "\"})",
                    containsString(
                        "[MATCH] option [similarity_function] is not supported by every node and remote cluster this query targets"
                    )
                );
        }
    }
}
