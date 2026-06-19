/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.inference.InferenceFunction;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class PreAnalyzerTests extends ESTestCase {

    public void testCollectInferenceIds() {
        PreAnalyzer preAnalyzer = new PreAnalyzer();

        // Rerank inference plan
        assertCollectInferenceIds(
            preAnalyzer,
            "FROM books METADATA _score | RERANK \"italian food recipe\" ON title WITH { \"inference_id\": \"rerank-inference-id\" }",
            List.of("rerank-inference-id")
        );

        // Completion inference plan
        assertCollectInferenceIds(
            preAnalyzer,
            "FROM books METADATA _score | COMPLETION \"italian food recipe\" WITH { \"inference_id\": \"completion-inference-id\" }",
            List.of("completion-inference-id")
        );

        // Text embedding function
        assertCollectInferenceIds(
            preAnalyzer,
            "FROM books METADATA _score | EVAL embedding = TEXT_EMBEDDING(\"description\", \"text-embedding-inference-id\")",
            List.of("text-embedding-inference-id")
        );

        // Embedding function
        assertCollectInferenceIds(
            preAnalyzer,
            "FROM books METADATA _score | EVAL embedding = EMBEDDING(\"description\", \"embedding-inference-id\")",
            List.of("embedding-inference-id")
        );

        // Nested inference functions
        assertCollectInferenceIds(
            preAnalyzer,
            "FROM books METADATA _score | EVAL embedding = TEXT_EMBEDDING(TEXT_EMBEDDING(\"nested\", \"nested-id\"), \"outer-id\")",
            List.of("nested-id", "outer-id")
        );

        // Inference function wrapping a regular (non-inference) function: the cheap short-circuit must
        // skip CONCAT but still collect the inference function's id.
        assertCollectInferenceIds(
            preAnalyzer,
            "FROM books METADATA _score | EVAL embedding = TEXT_EMBEDDING(CONCAT(\"a\", \"b\"), \"text-embedding-inference-id\")",
            List.of("text-embedding-inference-id")
        );

        // Multiple inference plans
        assertCollectInferenceIds(preAnalyzer, """
            FROM books METADATA _score
            | RERANK "italian food recipe" ON title WITH { "inference_id": "rerank-inference-id" }
            | COMPLETION "italian food recipe" WITH { "inference_id": "completion-inference-id" }
            """, List.of("rerank-inference-id", "completion-inference-id"));

        // No inference operations
        assertCollectInferenceIds(preAnalyzer, "FROM books | WHERE title:\"test\"", List.of());

        // No inference operations, but several regular functions are present: the cheap short-circuit must
        // skip every one of them without collecting any inference id.
        assertCollectInferenceIds(
            preAnalyzer,
            "FROM books | EVAL x = LENGTH(CONCAT(TO_LOWER(title), \"!\")) | WHERE x > ABS(-1)",
            List.of()
        );
    }

    /**
     * Guards that a newly registered {@link InferenceFunction} is also added to
     * {@link PreAnalyzer#INFERENCE_FUNCTION_DEFINITIONS} for inference id collection.
     */
    public void testRegisteredInferenceFunctionsIncludedInPreAnalyzer() {
        Set<String> registeredInferenceFunctions = TEST_FUNCTION_REGISTRY.listFunctions()
            .stream()
            .filter(def -> InferenceFunction.class.isAssignableFrom(def.clazz()))
            .map(FunctionDefinition::name)
            .collect(Collectors.toCollection(TreeSet::new));

        Set<String> preAnalysisInferenceFunctions = PreAnalyzer.INFERENCE_FUNCTION_DEFINITIONS.stream()
            .map(FunctionDefinition::name)
            .collect(Collectors.toCollection(TreeSet::new));

        assertEquals(
            "registered inference functions must match PreAnalyzer.INFERENCE_FUNCTION_DEFINITIONS",
            registeredInferenceFunctions,
            preAnalysisInferenceFunctions
        );
    }

    private void assertCollectInferenceIds(PreAnalyzer preAnalyzer, String query, List<String> expectedInferenceIds) {
        List<String> inferenceIds = preAnalyzer.preAnalyze(TEST_PARSER.parseQuery(query)).inferenceIds();
        assertThat(inferenceIds, containsInAnyOrder(expectedInferenceIds.toArray(new String[0])));
    }

    /**
     * A LOOKUP JOIN that is not nested inside a subquery is part of the whole query's main FROM, so its lookup index must be associated
     * with the top-level main index pattern(s) (rather than being absent from the map). The main FROM may target remote clusters; those
     * are preserved verbatim in the main pattern. The lookup index itself is always local - remote clusters are not allowed on the
     * right-hand side of a LOOKUP JOIN.
     */
    public void testTopLevelLookupIndexMapsToMainPattern() {
        // Single main pattern.
        assertLookupToMainPatterns("FROM logs | LOOKUP JOIN lookup_index ON f1", Map.of("lookup_index", Set.of("logs")));

        // A comma-separated FROM is a single index pattern, so the lookup is associated with that whole pattern.
        assertLookupToMainPatterns("FROM logs,more | LOOKUP JOIN lookup_index ON f1", Map.of("lookup_index", Set.of("logs,more")));

        // A remote main FROM is kept cluster-qualified in the main pattern; the (local) lookup index is associated with it.
        assertLookupToMainPatterns(
            "FROM remote_cluster:logs | LOOKUP JOIN lookup_index ON f1",
            Map.of("lookup_index", Set.of("remote_cluster:logs"))
        );

        // A mix of local and remote (wildcard) clusters in the main FROM is a single comma-separated pattern.
        assertLookupToMainPatterns("FROM logs,*:logs | LOOKUP JOIN lookup_index ON f1", Map.of("lookup_index", Set.of("logs,*:logs")));

        // Several top-level lookup joins against a remote main FROM are each associated with the same top-level main pattern.
        assertLookupToMainPatterns(
            "FROM *:logs | LOOKUP JOIN lookup_a ON f1 | LOOKUP JOIN lookup_b ON f2",
            Map.of("lookup_a", Set.of("*:logs"), "lookup_b", Set.of("*:logs"))
        );
    }

    /**
     * A LOOKUP JOIN in the main query (not nested in a subquery) runs after the subqueries' outputs are unioned with the top-level main
     * FROM, so its lookup index must exist on every cluster the query touches. It is therefore associated with both the top-level main
     * index pattern and every subquery's (here remote) main index pattern.
     */
    public void testMainQueryLookupIndexMapsToMainAndSubqueryPatterns() {
        // Top-level main FROM plus remote subqueries: the main-query lookup spans the top-level main pattern and both subqueries' remote
        // main patterns.
        assertLookupToMainPatterns(
            "FROM main_index, (FROM cluster-a:index1), (FROM cluster-b:index2) | LOOKUP JOIN lookup_index ON f",
            Map.of("lookup_index", Set.of("main_index", "cluster-a:index1", "cluster-b:index2"))
        );

        // No top-level main FROM (only remote subqueries): the main-query lookup spans both subqueries' remote main patterns.
        assertLookupToMainPatterns(
            "FROM (FROM cluster-a:index1), (FROM cluster-b:index2) | LOOKUP JOIN lookup_index ON f",
            Map.of("lookup_index", Set.of("cluster-a:index1", "cluster-b:index2"))
        );
    }

    /**
     * Nested subqueries are rejected later by the verifier, but pre-analysis still runs on the parsed plan. Each lookup index is scoped to
     * the main patterns of the scope that directly contains it: a scope's main patterns are its directly-owned mains plus every nested
     * subquery's mains (those are unioned into the scope before its own lookups run). So a main-query LOOKUP JOIN spans the whole query, an
     * outer subquery's lookup spans that subquery (including its nested subquery's mains), and a deeply nested lookup is scoped to its own
     * inner data only.
     */
    public void testMainQueryLookupIncludesNestedSubqueryMainPatterns() {
        // Main patterns only (no lookup joins inside the subqueries): the main-query lookup spans the top-level main and every (nested)
        // subquery main.
        assertLookupToMainPatterns(
            "FROM idxA, (FROM idxB, (FROM idxC)) | LOOKUP JOIN lkTop ON f",
            Map.of("lkTop", Set.of("idxA", "idxB", "idxC"))
        );

        // Remote clusters, with a LOOKUP JOIN inside each subquery level plus one in the main query:
        // - lookup_c is inside the nested subquery and only sees cluster-c:idxC (it runs before any union), so it is scoped to that alone;
        // - lookup_b is inside the outer subquery, which unions cluster-b:idxB with the nested subquery's output, so it spans both
        // cluster-b:idxB and cluster-c:idxC;
        // - main_lookup runs after the whole-query union and spans every main pattern (cluster-a:idxA plus both nested subquery mains).
        assertLookupToMainPatterns(
            """
                FROM cluster-a:idxA,
                  (FROM cluster-b:idxB,
                     (FROM cluster-c:idxC | LOOKUP JOIN lookup_c ON g)
                   | LOOKUP JOIN lookup_b ON f)
                | LOOKUP JOIN main_lookup ON h
                """,
            Map.of(
                "lookup_c",
                Set.of("cluster-c:idxC"),
                "lookup_b",
                Set.of("cluster-b:idxB", "cluster-c:idxC"),
                "main_lookup",
                Set.of("cluster-a:idxA", "cluster-b:idxB", "cluster-c:idxC")
            )
        );
    }

    /**
     * Subqueries that each contain a LOOKUP JOIN, with a top-level main FROM present. Each subquery's lookup index is associated with
     * that subquery's own main patterns; the top-level main FROM contributes no mapping of its own (it has no lookup join). When the two
     * subqueries join against the same lookup index, that index is associated with the union of both subqueries' main patterns. A LOOKUP
     * JOIN in the main query (outside the subqueries) runs on the unioned output and so spans the whole query's main patterns.
     */
    public void testSubqueriesWithLookupJoinsAndMainIndexPattern() {
        // A different lookup index in each subquery: each is scoped to its own subquery's main pattern. main_index is not a key in the
        // map (it has no lookup join) and does not leak into either lookup's main patterns.
        assertLookupToMainPatterns("""
            FROM main_index,
              (FROM cluster-a:logs | LOOKUP JOIN lookup_a ON f),
              (FROM cluster-b:logs | LOOKUP JOIN lookup_b ON g)
            """, Map.of("lookup_a", Set.of("cluster-a:logs"), "lookup_b", Set.of("cluster-b:logs")));

        // The same lookup index in both subqueries: associated with the union of both subqueries' main patterns.
        assertLookupToMainPatterns("""
            FROM main_index,
              (FROM cluster-a:logs | LOOKUP JOIN shared_lookup ON f),
              (FROM cluster-b:logs | LOOKUP JOIN shared_lookup ON g)
            """, Map.of("shared_lookup", Set.of("cluster-a:logs", "cluster-b:logs")));

        // An extra LOOKUP JOIN in the main query (outside the subqueries): main_lookup spans the whole query's main patterns (the top-level
        // main FROM plus both subqueries' main patterns), while each in-subquery lookup stays scoped to its own subquery.
        assertLookupToMainPatterns(
            """
                FROM main_index,
                  (FROM cluster-a:logs | LOOKUP JOIN lookup_a ON f),
                  (FROM cluster-b:logs | LOOKUP JOIN lookup_b ON g)
                | LOOKUP JOIN main_lookup ON h
                """,
            Map.of(
                "lookup_a",
                Set.of("cluster-a:logs"),
                "lookup_b",
                Set.of("cluster-b:logs"),
                "main_lookup",
                Set.of("main_index", "cluster-a:logs", "cluster-b:logs")
            )
        );

        // The same index joined both inside a subquery and in the main query: required on that subquery's clusters (from the in-subquery
        // join) unioned with the whole query's clusters (from the main-query join), which is the whole query's main patterns.
        assertLookupToMainPatterns("""
            FROM main_index,
              (FROM cluster-a:logs | LOOKUP JOIN shared_lookup ON f),
              (FROM cluster-b:logs)
            | LOOKUP JOIN shared_lookup ON h
            """, Map.of("shared_lookup", Set.of("main_index", "cluster-a:logs", "cluster-b:logs")));
    }

    /**
     * Subqueries that each contain a LOOKUP JOIN, with no top-level main FROM (the whole query is made up entirely of subqueries). The
     * in-subquery mappings are the same as when a main FROM is present, because the top-level main FROM never contributes a lookup
     * mapping in these queries. A LOOKUP JOIN in the main query (outside the subqueries) runs on the unioned output and so spans every
     * subquery's main pattern.
     */
    public void testSubqueriesWithLookupJoinsAndNoMainIndexPattern() {
        // A different lookup index in each subquery.
        assertLookupToMainPatterns("""
            FROM
              (FROM cluster-a:logs | LOOKUP JOIN lookup_a ON f),
              (FROM cluster-b:logs | LOOKUP JOIN lookup_b ON g)
            """, Map.of("lookup_a", Set.of("cluster-a:logs"), "lookup_b", Set.of("cluster-b:logs")));

        // The same lookup index in both subqueries: associated with the union of both subqueries' main patterns.
        assertLookupToMainPatterns("""
            FROM
              (FROM cluster-a:logs | LOOKUP JOIN shared_lookup ON f),
              (FROM cluster-b:logs | LOOKUP JOIN shared_lookup ON g)
            """, Map.of("shared_lookup", Set.of("cluster-a:logs", "cluster-b:logs")));

        // An extra LOOKUP JOIN in the main query (outside the subqueries), with no top-level main FROM: main_lookup spans both subqueries'
        // main patterns, while each in-subquery lookup stays scoped to its own subquery.
        assertLookupToMainPatterns(
            """
                FROM
                  (FROM cluster-a:logs | LOOKUP JOIN lookup_a ON f),
                  (FROM cluster-b:logs | LOOKUP JOIN lookup_b ON g)
                | LOOKUP JOIN main_lookup ON h
                """,
            Map.of(
                "lookup_a",
                Set.of("cluster-a:logs"),
                "lookup_b",
                Set.of("cluster-b:logs"),
                "main_lookup",
                Set.of("cluster-a:logs", "cluster-b:logs")
            )
        );
    }

    /**
     * An {@code IN}/{@code NOT IN} subquery (a {@code SemiJoin}/{@code AntiJoin}) only filters the rows of the enclosing query; its data is
     * never unioned in. So a {@code LOOKUP JOIN} inside the subquery is scoped to that subquery's own main patterns, and a
     * {@code LOOKUP JOIN} in the enclosing query is scoped to the enclosing query's main patterns only - it is never required on the
     * (possibly remote) clusters referenced solely inside the {@code IN}/{@code NOT IN} subquery.
     */
    public void testInSubqueryLookupsScopedToSubquery() {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());

        // IN subquery (SemiJoin): the lookup inside the subquery is scoped to the subquery's remote main pattern, while the main-query
        // lookup is scoped to the main FROM only - it does not pick up cluster-a referenced inside the IN subquery.
        assertInSubqueryLookupToMainPatterns("""
            FROM main_index
            | WHERE f IN (FROM cluster-a:logs | LOOKUP JOIN sub_lookup ON g)
            | LOOKUP JOIN main_lookup ON h
            """, Map.of("sub_lookup", Set.of("cluster-a:logs"), "main_lookup", Set.of("main_index")));

        // NOT IN subquery (AntiJoin): same scoping as IN.
        assertInSubqueryLookupToMainPatterns("""
            FROM main_index
            | WHERE f NOT IN (FROM cluster-a:logs | LOOKUP JOIN sub_lookup ON g)
            | LOOKUP JOIN main_lookup ON h
            """, Map.of("sub_lookup", Set.of("cluster-a:logs"), "main_lookup", Set.of("main_index")));

        // The enclosing query is itself a union of FROM subqueries: the main-query lookup spans those FROM subqueries' main patterns (they
        // are unioned in) but still excludes cluster-a referenced only inside the IN subquery.
        assertInSubqueryLookupToMainPatterns("""
            FROM main_index, (FROM cluster-b:logs)
            | WHERE f IN (FROM cluster-a:logs)
            | LOOKUP JOIN main_lookup ON h
            """, Map.of("main_lookup", Set.of("main_index", "cluster-b:logs")));

        assertInSubqueryLookupToMainPatterns("""
            FROM main_index, (FROM cluster-b:logs)
            | WHERE f IN (FROM cluster-a:logs | LOOKUP JOIN sub_lookup ON g)
            | LOOKUP JOIN main_lookup ON h
            """, Map.of("main_lookup", Set.of("main_index", "cluster-b:logs"), "sub_lookup", Set.of("cluster-a:logs")));

        // The IN subquery body is itself a union of remote FROM subqueries: the lookup inside it spans both of those remote main patterns,
        // while the main-query lookup stays scoped to the main FROM.
        assertInSubqueryLookupToMainPatterns("""
            FROM main_index
            | WHERE f IN (FROM (FROM cluster-a:logs), (FROM cluster-b:logs) | LOOKUP JOIN sub_lookup ON g)
            | LOOKUP JOIN main_lookup ON h
            """, Map.of("sub_lookup", Set.of("cluster-a:logs", "cluster-b:logs"), "main_lookup", Set.of("main_index")));

        assertInSubqueryLookupToMainPatterns("""
            FROM main_index, (FROM cluster-b:logs)
            | WHERE f IN (FROM (FROM cluster-a:logs), (FROM cluster-b:logs) | LOOKUP JOIN sub_lookup ON g)
            | LOOKUP JOIN main_lookup ON h
            """, Map.of("main_lookup", Set.of("main_index", "cluster-b:logs"), "sub_lookup", Set.of("cluster-a:logs", "cluster-b:logs")));

        assertInSubqueryLookupToMainPatterns(
            """
                FROM main_index, (FROM cluster-b:logs)
                | WHERE f IN (FROM
                                  (FROM cluster-a:logs | LOOKUP JOIN sub_lookup_a ON a),
                                  (FROM cluster-b:logs | LOOKUP JOIN sub_lookup_b ON b)
                              | LOOKUP JOIN sub_lookup ON g)
                | LOOKUP JOIN main_lookup ON h
                """,
            Map.of(
                "main_lookup",
                Set.of("main_index", "cluster-b:logs"),
                "sub_lookup",
                Set.of("cluster-a:logs", "cluster-b:logs"),
                "sub_lookup_a",
                Set.of("cluster-a:logs"),
                "sub_lookup_b",
                Set.of("cluster-b:logs")
            )
        );

        // The same lookup index used both in the main query and inside the IN subquery must exist on the clusters of both scopes, so it is
        // associated with the union of the main FROM and the IN subquery's main pattern.
        assertInSubqueryLookupToMainPatterns("""
            FROM main_index
            | WHERE f IN (FROM cluster-a:logs | LOOKUP JOIN shared_lookup ON g)
            | LOOKUP JOIN shared_lookup ON h
            """, Map.of("shared_lookup", Set.of("main_index", "cluster-a:logs")));
    }

    /**
     * Nested {@code IN}/{@code NOT IN} subqueries: each subquery is an independent scope whose data is never unioned into the scope that
     * filters on it. So a lookup is scoped to the main patterns of the {@code IN}/{@code NOT IN} subquery (or main query) that directly
     * contains it, and never to the clusters referenced only inside a more deeply nested {@code IN}/{@code NOT IN} subquery.
     */
    public void testNestedInSubqueryLookupsScopedIndependently() {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());

        // An IN subquery nested inside another IN subquery, each with its own LOOKUP JOIN, plus a main-query LOOKUP JOIN:
        // - inner_lookup sits in the innermost IN subquery and is scoped to cluster-b:logs only;
        // - outer_lookup sits in the outer IN subquery and is scoped to cluster-a:logs only (the inner IN subquery is not unioned in);
        // - main_lookup is in the main query and is scoped to main_index only (neither IN subquery is unioned in).
        assertInSubqueryLookupToMainPatterns(
            """
                FROM main_index
                | WHERE f IN (
                    FROM cluster-a:logs
                    | WHERE g IN (FROM cluster-b:logs | LOOKUP JOIN inner_lookup ON p)
                    | LOOKUP JOIN outer_lookup ON q
                  )
                | LOOKUP JOIN main_lookup ON h
                """,
            Map.of("inner_lookup", Set.of("cluster-b:logs"), "outer_lookup", Set.of("cluster-a:logs"), "main_lookup", Set.of("main_index"))
        );

        // Two sibling IN/NOT IN subqueries (two WHERE clauses), each with its own lookup: each lookup is scoped to its own subquery's
        // remote main pattern, and the main-query lookup remains scoped to the main FROM only.
        assertInSubqueryLookupToMainPatterns("""
            FROM main_index
            | WHERE f IN (FROM cluster-a:logs | LOOKUP JOIN lookup_a ON g)
            | WHERE k NOT IN (FROM cluster-b:logs | LOOKUP JOIN lookup_b ON m)
            | LOOKUP JOIN main_lookup ON h
            """, Map.of("lookup_a", Set.of("cluster-a:logs"), "lookup_b", Set.of("cluster-b:logs"), "main_lookup", Set.of("main_index")));

        // An IN subquery embedded in an OR (a MarkJoin) is scoped exactly like a SemiJoin/AntiJoin: the lookup inside it stays on the
        // subquery's clusters and the main-query lookup excludes them.
        assertInSubqueryLookupToMainPatterns("""
            FROM main_index
            | WHERE f IN (FROM cluster-a:logs | LOOKUP JOIN sub_lookup ON g) OR h > 1
            | LOOKUP JOIN main_lookup ON k
            """, Map.of("sub_lookup", Set.of("cluster-a:logs"), "main_lookup", Set.of("main_index")));
    }

    /**
     * A {@code ROW} command is a valid subquery source inside a {@code FROM}. A {@code ROW} subquery is sourceless - it produces no
     * {@link org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation} - so it contributes no main index patterns (and therefore no
     * clusters) to the scope it is unioned into. A main-query {@code LOOKUP JOIN} therefore gains nothing from a sibling {@code ROW}
     * subquery, whether the main FROM is local or remote, and a {@code LOOKUP JOIN} nested inside a {@code ROW} subquery is scoped to an
     * empty set of main patterns (treated as "all clusters" downstream, which the session resolves to the local coordinator).
     */
    public void testRowSubqueryInFromContributesNoMainPatterns() {
        assumeTrue("Requires ROW subquery support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());

        // A ROW subquery is a sourceless union member, so the main-query lookup spans only the top-level main FROM.
        assertLookupToMainPatterns(
            "FROM main_index, (ROW x = 1) | LOOKUP JOIN main_lookup ON f",
            Map.of("main_lookup", Set.of("main_index"))
        );
        // Same shape against a remote main FROM: the ROW still contributes no cluster, so the lookup spans only the remote main.
        assertLookupToMainPatterns(
            "FROM cluster-a:main_index, (ROW x = 1) | LOOKUP JOIN main_lookup ON f",
            Map.of("main_lookup", Set.of("cluster-a:main_index"))
        );

        // No top-level main FROM, only a ROW subquery: the whole query has no main patterns, so the main-query lookup maps to the empty
        // set (which downstream means "all clusters").
        assertLookupToMainPatterns("FROM (ROW x = 1) | LOOKUP JOIN main_lookup ON x", Map.of("main_lookup", Set.of()));

        // A LOOKUP JOIN inside the ROW subquery is scoped to the ROW subquery's (empty) main patterns; the main-query lookup still spans
        // only the top-level main FROM.
        assertLookupToMainPatterns(
            "FROM main_index, (ROW x = 1 | LOOKUP JOIN sub_lookup ON x) | LOOKUP JOIN main_lookup ON f",
            Map.of("sub_lookup", Set.of(), "main_lookup", Set.of("main_index"))
        );
        // Same shape against a remote main FROM: the nested ROW lookup never picks up that remote cluster (it stays empty), while the
        // main-query lookup spans only the remote main FROM.
        assertLookupToMainPatterns(
            "FROM cluster-a:main_index, (ROW x = 1 | LOOKUP JOIN sub_lookup ON x) | LOOKUP JOIN main_lookup ON f",
            Map.of("sub_lookup", Set.of(), "main_lookup", Set.of("cluster-a:main_index"))
        );
    }

    /**
     * A {@code ROW} command is also a valid {@code IN}/{@code NOT IN} subquery source. As with any {@code IN}/{@code NOT IN} subquery it is
     * an independent scope whose data only filters the enclosing rows and is never unioned in; being sourceless it also contributes no main
     * patterns (and no clusters) of its own. So a {@code LOOKUP JOIN} inside the {@code ROW} subquery is scoped to an empty set, and a
     * {@code LOOKUP JOIN} in the enclosing query stays scoped to the enclosing main FROM, whether that main is local or remote.
     */
    public void testRowSubqueryInWhereInScopedIndependently() {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
        assumeTrue("Requires ROW subquery support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());

        // IN subquery (SemiJoin) with a ROW body: the main-query lookup is scoped to the main FROM only.
        assertInSubqueryLookupToMainPatterns(
            "FROM main_index | WHERE f IN (ROW x = 1) | LOOKUP JOIN main_lookup ON h",
            Map.of("main_lookup", Set.of("main_index"))
        );
        // Same shape against a remote main FROM: scoped to the remote main only.
        assertInSubqueryLookupToMainPatterns(
            "FROM cluster-a:main_index | WHERE f IN (ROW x = 1) | LOOKUP JOIN main_lookup ON h",
            Map.of("main_lookup", Set.of("cluster-a:main_index"))
        );

        // NOT IN subquery (AntiJoin) with a ROW body: same scoping as IN, against both a local and a remote main FROM.
        assertInSubqueryLookupToMainPatterns(
            "FROM main_index | WHERE f NOT IN (ROW x = 1) | LOOKUP JOIN main_lookup ON h",
            Map.of("main_lookup", Set.of("main_index"))
        );
        assertInSubqueryLookupToMainPatterns(
            "FROM cluster-a:main_index | WHERE f NOT IN (ROW x = 1) | LOOKUP JOIN main_lookup ON h",
            Map.of("main_lookup", Set.of("cluster-a:main_index"))
        );

        // A LOOKUP JOIN inside the ROW IN subquery is scoped to the subquery's (empty) main patterns; the main-query lookup excludes it
        // and stays scoped to the enclosing main FROM - shown here against both a local and a remote main.
        assertInSubqueryLookupToMainPatterns(
            "FROM main_index | WHERE f IN (ROW x = 1 | LOOKUP JOIN sub_lookup ON x) | LOOKUP JOIN main_lookup ON h",
            Map.of("sub_lookup", Set.of(), "main_lookup", Set.of("main_index"))
        );
        assertInSubqueryLookupToMainPatterns(
            "FROM cluster-a:main_index | WHERE f IN (ROW x = 1 | LOOKUP JOIN sub_lookup ON x) | LOOKUP JOIN main_lookup ON h",
            Map.of("sub_lookup", Set.of(), "main_lookup", Set.of("cluster-a:main_index"))
        );
    }

    /**
     * A {@code TS} (time-series) command is a valid subquery source inside a {@code FROM}. Its index produces an
     * {@link org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation} just like a {@code FROM}, so a {@code TS} subquery contributes
     * its index pattern to the enclosing scope's main patterns (its output is unioned in). A main-query {@code LOOKUP JOIN} therefore spans
     * the {@code TS} subquery's index, and a {@code LOOKUP JOIN} nested inside the {@code TS} subquery is scoped to that subquery's index.
     */
    public void testTimeSeriesSubqueryInFromMapsToMainPattern() {
        assumeTrue("Requires TS subquery support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());

        // A TS subquery is unioned into the main query: the main-query lookup spans both the top-level main FROM and the TS index.
        assertLookupToMainPatterns(
            "FROM main_index, (TS metrics_index) | LOOKUP JOIN main_lookup ON f",
            Map.of("main_lookup", Set.of("main_index", "metrics_index"))
        );
        // Same shape across clusters: a remote main FROM and a TS subquery on a different remote cluster - the lookup spans both.
        assertLookupToMainPatterns(
            "FROM cluster-a:main_index, (TS remote-b:metrics) | LOOKUP JOIN main_lookup ON f",
            Map.of("main_lookup", Set.of("cluster-a:main_index", "remote-b:metrics"))
        );

        // No top-level main FROM, only a remote TS subquery: the main-query lookup spans the TS subquery's remote index.
        assertLookupToMainPatterns(
            "FROM (TS cluster-a:metrics) | LOOKUP JOIN main_lookup ON f",
            Map.of("main_lookup", Set.of("cluster-a:metrics"))
        );

        // A LOOKUP JOIN inside the TS subquery is scoped to that subquery's remote main pattern; the main-query lookup spans both the
        // local main FROM and the remote TS index.
        assertLookupToMainPatterns(
            "FROM main_index, (TS cluster-a:metrics | LOOKUP JOIN sub_lookup ON f) | LOOKUP JOIN main_lookup ON h",
            Map.of("sub_lookup", Set.of("cluster-a:metrics"), "main_lookup", Set.of("main_index", "cluster-a:metrics"))
        );
    }

    /**
     * A {@code TS} command is also a valid {@code IN}/{@code NOT IN} subquery source. As an {@code IN}/{@code NOT IN} subquery it is an
     * independent scope whose data only filters the enclosing rows and is never unioned in, so a {@code LOOKUP JOIN} inside the {@code TS}
     * subquery is scoped to the {@code TS} index alone while a {@code LOOKUP JOIN} in the enclosing query stays scoped to the enclosing
     * main FROM - it never picks up the (possibly remote) clusters referenced only inside the {@code TS} IN subquery.
     */
    public void testTimeSeriesSubqueryInWhereInScopedToSubquery() {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
        assumeTrue("Requires TS in IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());

        // IN subquery (SemiJoin) with a TS body and a nested LOOKUP JOIN: sub_lookup is scoped to the TS remote index, while the
        // main-query lookup is scoped to the main FROM only - it does not pick up the cluster referenced inside the IN subquery.
        assertInSubqueryLookupToMainPatterns(
            "FROM main_index | WHERE f IN (TS cluster-a:metrics | LOOKUP JOIN sub_lookup ON g) | LOOKUP JOIN main_lookup ON h",
            Map.of("sub_lookup", Set.of("cluster-a:metrics"), "main_lookup", Set.of("main_index"))
        );
        // Same shape against a remote enclosing main on a different cluster: the main-query lookup stays scoped to remote-b only and
        // never picks up cluster-a, which is referenced solely inside the IN subquery.
        assertInSubqueryLookupToMainPatterns(
            "FROM remote-b:main_index | WHERE f IN (TS cluster-a:metrics | LOOKUP JOIN sub_lookup ON g) | LOOKUP JOIN main_lookup ON h",
            Map.of("sub_lookup", Set.of("cluster-a:metrics"), "main_lookup", Set.of("remote-b:main_index"))
        );

        // NOT IN subquery (AntiJoin) with a TS body: same scoping as IN, against both a local and a remote enclosing main FROM.
        assertInSubqueryLookupToMainPatterns(
            "FROM main_index | WHERE f NOT IN (TS cluster-a:metrics | LOOKUP JOIN sub_lookup ON g) | LOOKUP JOIN main_lookup ON h",
            Map.of("sub_lookup", Set.of("cluster-a:metrics"), "main_lookup", Set.of("main_index"))
        );
        assertInSubqueryLookupToMainPatterns(
            "FROM remote-b:main_index | WHERE f NOT IN (TS cluster-a:metrics | LOOKUP JOIN sub_lookup ON g) | LOOKUP JOIN main_lookup ON h",
            Map.of("sub_lookup", Set.of("cluster-a:metrics"), "main_lookup", Set.of("remote-b:main_index"))
        );
    }

    public void testNoLookupIndexProducesEmptyMap() {
        var preAnalysis = new PreAnalyzer().preAnalyze(TEST_PARSER.parseQuery("FROM logs | WHERE f1 > 1"));
        assertThat(preAnalysis.lookupIndexPatternsToMainAndSubqueryIndexPatterns(), anEmptyMap());
    }

    private static void assertLookupToMainPatterns(String query, Map<String, Set<String>> expected) {
        assertLookupToMainPatterns(TEST_PARSER.parseQuery(query), expected);
    }

    /**
     * Resolves {@code IN}/{@code NOT IN} subqueries into {@link org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin}
     * nodes (as the session does before pre-analysis) and then asserts the lookup-to-main-pattern mapping. The parser only emits
     * {@code InSubquery} expressions, so the join nodes that {@link PreAnalyzer} reasons about only exist after this resolution step.
     */
    private static void assertInSubqueryLookupToMainPatterns(String query, Map<String, Set<String>> expected) {
        assertLookupToMainPatterns(InSubqueryResolver.resolve(TEST_PARSER.parseQuery(query)), expected);
    }

    private static void assertLookupToMainPatterns(LogicalPlan plan, Map<String, Set<String>> expected) {
        Map<IndexPattern, Set<IndexPattern>> actual = new PreAnalyzer().preAnalyze(plan)
            .lookupIndexPatternsToMainAndSubqueryIndexPatterns();

        assertThat(actual.keySet(), containsInAnyOrder(expected.keySet().stream().map(PreAnalyzerTests::pattern).toArray()));
        expected.forEach((lookup, mainPatterns) -> {
            Set<IndexPattern> actualMainPatterns = actual.get(pattern(lookup));
            assertThat(actualMainPatterns, containsInAnyOrder(mainPatterns.stream().map(PreAnalyzerTests::pattern).toArray()));
        });
    }

    private static IndexPattern pattern(String indexPattern) {
        return new IndexPattern(Source.EMPTY, indexPattern);
    }
}
