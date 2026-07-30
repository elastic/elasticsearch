/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.AstKeywordFieldRewriter.RewriteResult;
import org.elasticsearch.xpack.esql.AstKeywordFieldRewriter.ScopeResolver;
import org.elasticsearch.xpack.esql.AstKeywordFieldRewriter.SkipEvent;
import org.elasticsearch.xpack.esql.AstKeywordFieldRewriter.SkipSite;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

/**
 * Unit tests for {@link AstKeywordFieldRewriter}.
 * <p>
 * Each test exercises the pure
 * {@link AstKeywordFieldRewriter#rewrite(String, ScopeResolver, String, List)} entry point with an
 * in-memory {@link ScopeResolver}, so no cluster is required despite the enclosing source set: the
 * rewriter only parses the query (via {@code EsqlTestUtils.TEST_PARSER}) and splices text. The
 * wrapper sub-key is fixed to {@code "v"} so every wrap reads {@code field_extract(<field>, "v")}.
 */
public class AstKeywordFieldRewriterTests extends ESTestCase {

    private static final String SUBKEY = "v";

    /** Rewrites {@code query} with a constant scope and no expected column order. */
    private static RewriteResult rewrite(String query, Set<String> scope) {
        return AstKeywordFieldRewriter.rewrite(query, q -> scope, SUBKEY, List.of());
    }

    /** Rewrites {@code query} with a constant scope and the given expected column order. */
    private static RewriteResult rewrite(String query, Set<String> scope, List<String> expectedColumnOrder) {
        return AstKeywordFieldRewriter.rewrite(query, q -> scope, SUBKEY, expectedColumnOrder);
    }

    /** Number of non-overlapping occurrences of {@code needle} in {@code haystack}. */
    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        for (int idx = haystack.indexOf(needle); idx >= 0; idx = haystack.indexOf(needle, idx + needle.length())) {
            count++;
        }
        return count;
    }

    /**
     * Skips the calling test on release builds, where {@code WHERE ... IN (subquery)} is unavailable.
     * <p>
     * The {@code IN (subquery)} grammar alternative is guarded by a {@code {this.isDevVersion()}?}
     * predicate (see {@code Expression.g4} / {@code InExpression.g4}, tracked by
     * {@link EsqlCapabilities.Cap#WHERE_IN_SUBQUERY}), so {@code EsqlTestUtils.TEST_PARSER} can parse
     * such a query only in a snapshot build. In a release build the parse fails and the rewriter
     * returns the query unmodified by contract, so the {@code assertTrue(result.modified())} checks
     * below would fail for an environmental reason rather than a real rewrite regression.
     */
    private static void assumeWhereInSubquerySupported() {
        assumeTrue(
            "WHERE ... IN (subquery) is a snapshot-only grammar feature (EsqlCapabilities.Cap.WHERE_IN_SUBQUERY)",
            EsqlCapabilities.Cap.WHERE_IN_SUBQUERY.isEnabled()
        );
    }

    /** An empty resolved scope returns the original query with {@code modified == false}. */
    public void testEmptyInitialScopeReturnsUnmodifiedQuery() {
        String query = "FROM employees | KEEP first_name";
        RewriteResult result = rewrite(query, Set.of());
        assertFalse(result.modified());
        assertEquals(query, result.rewrittenQuery());
        assertTrue(result.rewrittenFieldNames().isEmpty());
        assertTrue(result.skipEvents().isEmpty());
    }

    /** A query the test parser cannot parse is returned unchanged rather than throwing. */
    public void testUnparseableQueryReturnsUnmodifiedQuery() {
        String query = "FROM employees | WHERE first_name > (";
        RewriteResult result = rewrite(query, Set.of("first_name"));
        assertFalse(result.modified());
        assertEquals(query, result.rewrittenQuery());
        assertTrue(result.rewrittenFieldNames().isEmpty());
    }

    /** A bare in-scope attribute in an expression position is wrapped in {@code field_extract(field, "v")}. */
    public void testInScopeAttributeWrappedInExpressionPosition() {
        RewriteResult result = rewrite("FROM employees | WHERE first_name == \"Georgi\" | STATS c = COUNT(*)", Set.of("first_name"));
        assertTrue(result.modified());
        assertThat(result.rewrittenQuery(), containsString("WHERE field_extract(first_name, \"v\") == \"Georgi\""));
        assertThat(result.rewrittenFieldNames(), hasItem("first_name"));
    }

    /** An attribute not in scope is left untouched even where wrapping would be syntactically legal. */
    public void testOutOfScopeAttributeLeftUntouched() {
        String query = "FROM employees | WHERE first_name == \"Georgi\" | STATS c = COUNT(*)";
        RewriteResult result = rewrite(query, Set.of("gender"));
        assertFalse(result.modified());
        assertThat(result.rewrittenQuery(), not(containsString("field_extract(first_name")));
    }

    /** {@code EVAL name = ...} wraps its right-hand side and then removes {@code name} from scope. */
    public void testEvalWrapsRhsAndRemovesAssignedNameFromScope() {
        RewriteResult result = rewrite(
            "FROM employees | EVAL first_name = TO_UPPER(first_name) | WHERE first_name == \"X\"",
            Set.of("first_name")
        );
        assertTrue(result.modified());
        // RHS reference is wrapped.
        assertThat(result.rewrittenQuery(), containsString("EVAL first_name = TO_UPPER(field_extract(first_name, \"v\"))"));
        // After the EVAL rebinds it, the later WHERE reference is no longer in scope and stays bare.
        assertThat(result.rewrittenQuery(), containsString("WHERE first_name == \"X\""));
        assertThat(result.rewrittenQuery(), not(containsString("field_extract(first_name, \"v\") == \"X\"")));
        assertThat(result.rewrittenFieldNames(), hasItem("first_name"));
    }

    /** {@code KEEP} narrows the scope to the intersection of kept names; dropped names are not recovered. */
    public void testKeepNarrowsScopeToKeptNames() {
        RewriteResult result = rewrite("FROM employees | KEEP first_name, gender", Set.of("first_name", "gender", "last_name"));
        assertTrue(result.modified());
        assertThat(
            result.rewrittenQuery(),
            containsString("\n| EVAL first_name = field_extract(first_name, \"v\"), gender = field_extract(gender, \"v\")")
        );
        assertThat(result.rewrittenQuery(), not(containsString("last_name")));
        assertEquals(Set.of("first_name", "gender"), result.rewrittenFieldNames());
    }

    /** {@code DROP} removes the dropped names from scope so only the survivors are recovered. */
    public void testDropRemovesNamesFromScope() {
        RewriteResult result = rewrite("FROM employees | DROP gender", Set.of("first_name", "gender"));
        assertTrue(result.modified());
        assertThat(result.rewrittenQuery(), containsString("\n| EVAL first_name = field_extract(first_name, \"v\")"));
        assertThat(result.rewrittenQuery(), not(containsString("field_extract(gender")));
        assertEquals(Set.of("first_name"), result.rewrittenFieldNames());
    }

    /** {@code RENAME old AS new} moves a flattened name's membership from {@code old} to {@code new}. */
    public void testRenameMovesScopeMembership() {
        RewriteResult result = rewrite("FROM employees | RENAME first_name AS fn | KEEP fn", Set.of("first_name"));
        assertTrue(result.modified());
        // Recovery targets the renamed column, proving the membership moved to "fn".
        assertThat(result.rewrittenQuery(), containsString("\n| EVAL fn = field_extract(fn, \"v\")"));
        assertThat(result.rewrittenFieldNames(), hasItem("fn"));
    }

    /** Plain {@code STATS} clears the scope and aliases a bare in-scope grouping key with a rename-back. */
    public void testStatsClearsScopeAndAliasesBareGroupingKey() {
        RewriteResult result = rewrite("FROM employees | STATS c = COUNT(*) BY gender", Set.of("gender"));
        assertTrue(result.modified());
        assertThat(result.rewrittenQuery(), containsString("BY __kw_gender = field_extract(gender, \"v\")"));
        assertThat(result.rewrittenQuery(), containsString("\n| RENAME __kw_gender AS gender"));
        // Plain STATS clears the scope: gender is handled in-segment, not by a tail EVAL.
        assertThat(result.rewrittenQuery(), not(containsString("\n| EVAL gender = field_extract(gender, \"v\")")));
        assertThat(result.rewrittenFieldNames(), hasItem("gender"));
    }

    /** {@code INLINE STATS} preserves the incoming scope minus the names it rebinds, with a drop+rename. */
    public void testInlineStatsPreservesScopeMinusReboundNames() {
        RewriteResult result = rewrite("FROM employees | INLINE STATS m = MAX(salary) BY gender", Set.of("gender", "first_name"));
        assertTrue(result.modified());
        assertThat(result.rewrittenQuery(), containsString("BY __kw_gender = field_extract(gender, \"v\")"));
        // INLINE STATS preserves the input row, so the original flattened gender is dropped before the rename-back.
        assertThat(result.rewrittenQuery(), containsString("\n| DROP gender"));
        assertThat(result.rewrittenQuery(), containsString("\n| RENAME __kw_gender AS gender"));
        // first_name was not rebound, so it stays in scope and is recovered by the tail EVAL.
        assertThat(result.rewrittenQuery(), containsString("\n| EVAL first_name = field_extract(first_name, \"v\")"));
        assertThat(result.rewrittenFieldNames(), hasItem("gender"));
        assertThat(result.rewrittenFieldNames(), hasItem("first_name"));
    }

    /** An implicitly-named aggregate over an in-scope field keeps its derived column name via a backtick alias. */
    public void testImplicitlyNamedStatsPiecePreservesColumnName() {
        RewriteResult result = rewrite("FROM employees | STATS MAX(LENGTH(first_name))", Set.of("first_name"));
        assertTrue(result.modified());
        assertThat(result.rewrittenQuery(), containsString("`MAX(LENGTH(first_name))` = MAX(LENGTH(field_extract(first_name, \"v\")))"));
        assertThat(result.rewrittenFieldNames(), hasItem("first_name"));
    }

    /** {@code DISSECT} wraps the input expression and drops the pattern-bound names from scope. */
    public void testRegexExtractWrapsInputAndDropsExtractedNames() {
        RewriteResult result = rewrite(
            "FROM employees | DISSECT first_name \"%{a} %{b}\" | WHERE b == \"x\" | STATS c = COUNT(*)",
            Set.of("first_name", "b")
        );
        assertTrue(result.modified());
        assertThat(result.rewrittenQuery(), containsString("DISSECT field_extract(first_name, \"v\") \"%{a} %{b}\""));
        // "b" is rebound by the DISSECT pattern, so the later WHERE reference is out of scope and stays bare.
        assertThat(result.rewrittenQuery(), containsString("WHERE b == \"x\""));
        assertThat(result.rewrittenQuery(), not(containsString("field_extract(b")));
        assertThat(result.rewrittenFieldNames(), hasItem("first_name"));
    }

    /** An in-scope {@code MV_EXPAND} target is hoisted into a preceding {@code EVAL} and then leaves scope. */
    public void testMvExpandTargetHoistedBeforeCommand() {
        RewriteResult result = rewrite("FROM employees | MV_EXPAND first_name | KEEP first_name", Set.of("first_name"));
        assertTrue(result.modified());
        // The hoist rebinds the field before the command; the MV_EXPAND argument itself stays a bare attribute.
        assertThat(result.rewrittenQuery(), containsString("EVAL first_name = field_extract(first_name, \"v\")\n| MV_EXPAND first_name"));
        assertThat(result.rewrittenFieldNames(), hasItem("first_name"));
    }

    /** A bare in-scope {@code IN (subquery)} left-hand side is hoisted into an {@code EVAL} before the {@code WHERE}. */
    public void testInSubqueryLeftHandSideHoistedBeforeWhere() {
        assumeWhereInSubquerySupported();
        RewriteResult result = rewrite(
            "FROM employees | WHERE first_name IN (FROM employees | KEEP first_name) | KEEP emp_no",
            Set.of("first_name")
        );
        assertTrue(result.modified());
        // The LHS is rebound to keyword by a hoisted EVAL, so the in-place IN reference stays a bare attribute.
        assertThat(result.rewrittenQuery(), containsString("EVAL first_name = field_extract(first_name, \"v\")\n| WHERE first_name IN ("));
        // The subquery's projected column is recovered to keyword by a tail EVAL before its closing paren.
        assertThat(result.rewrittenQuery(), containsString("KEEP first_name\n| EVAL first_name = field_extract(first_name, \"v\"))"));
        assertThat(result.rewrittenFieldNames(), hasItem("first_name"));
    }

    /** The body of an {@code IN (subquery)} is rewritten with a scope re-resolved from the subquery's own source. */
    public void testInSubqueryBodyRewrittenWithReResolvedScope() {
        assumeWhereInSubquerySupported();
        // The outer FROM employees resolves to {first_name}; the subquery FROM apps resolves to {id}. The resolver keys
        // on "employees" (present only in the outer text) so the subquery is rewritten with its own, different scope.
        ScopeResolver resolver = q -> q.contains("employees") ? Set.of("first_name") : Set.of("id");
        RewriteResult result = AstKeywordFieldRewriter.rewrite(
            "FROM employees | WHERE first_name IN (FROM apps | KEEP id) | KEEP emp_no",
            resolver,
            SUBKEY,
            List.of()
        );
        assertTrue(result.modified());
        // The subquery recovery uses "id" (its own scope), not the outer "first_name".
        assertThat(result.rewrittenQuery(), containsString("KEEP id\n| EVAL id = field_extract(id, \"v\"))"));
        assertThat(result.rewrittenFieldNames(), hasItem("id"));
        assertThat(result.rewrittenFieldNames(), hasItem("first_name"));
    }

    /** An in-scope reference inside an {@code ENRICH} body is recorded as an {@code ENRICH_BODY} skip event. */
    public void testEnrichBodyReferenceRecordedAsSkipEvent() {
        RewriteResult result = rewrite("FROM employees | ENRICH some_policy ON first_name | STATS c = COUNT(*)", Set.of("first_name"));
        assertThat(result.skipEvents(), hasItem(new SkipEvent(SkipSite.ENRICH_BODY, "first_name")));
    }

    /** An in-scope reference inside a {@code LOOKUP JOIN ... ON ...} body is recorded as a {@code LOOKUP_JOIN_ON} skip. */
    public void testLookupJoinOnReferenceRecordedAsSkipEvent() {
        RewriteResult result = rewrite(
            "FROM employees | LOOKUP JOIN languages_lookup ON language_code | STATS c = COUNT(*)",
            Set.of("language_code")
        );
        assertThat(result.skipEvents(), hasItem(new SkipEvent(SkipSite.LOOKUP_JOIN_ON, "language_code")));
    }

    /** An in-scope left-hand side of the {@code :} match operator is recorded as a {@code MATCH_OPERATOR_LHS} skip. */
    public void testMatchOperatorLeftHandSideRecordedAsSkipEvent() {
        RewriteResult result = rewrite("FROM employees | WHERE first_name : \"Georgi\" | STATS c = COUNT(*)", Set.of("first_name"));
        assertThat(result.skipEvents(), hasItem(new SkipEvent(SkipSite.MATCH_OPERATOR_LHS, "first_name")));
    }

    /** A {@code FORK} field flattened in every branch survives into the post-fork scope (the intersection). */
    public void testForkKeepsUniformlyFlattenedFieldInPostForkScope() {
        RewriteResult result = rewrite(
            "FROM employees | FORK (WHERE emp_no > 10) (WHERE emp_no < 5) | KEEP first_name",
            Set.of("first_name")
        );
        assertTrue(result.modified());
        // No branch is rewritten (the field is uniformly flattened, so there is no conflict to equalize).
        assertThat(result.rewrittenQuery(), startsWith("FROM employees | FORK (WHERE emp_no > 10) (WHERE emp_no < 5) | KEEP first_name"));
        // The field survives into the post-fork scope and is recovered by the top-level tail EVAL.
        assertThat(result.rewrittenQuery(), endsWith("\n| EVAL first_name = field_extract(first_name, \"v\")"));
        assertEquals(Set.of("first_name"), result.rewrittenFieldNames());
    }

    /** A {@code FORK} field flattened in some branches but keyword in others is recovered in the flattened branches. */
    public void testForkRecoversConflictingFieldPerBranch() {
        RewriteResult result = rewrite(
            "FROM employees | FORK (STATS c = COUNT(*) BY first_name) (WHERE emp_no > 10) | KEEP first_name",
            Set.of("first_name")
        );
        assertTrue(result.modified());
        // Branch 1 produces first_name as keyword via the STATS grouping alias + rename-back.
        assertThat(result.rewrittenQuery(), containsString("BY __kw_first_name = field_extract(first_name, \"v\")"));
        assertThat(result.rewrittenQuery(), containsString("RENAME __kw_first_name AS first_name"));
        // Branch 2 still has first_name flattened, so it is recovered there to match branch 1's keyword type.
        assertThat(result.rewrittenQuery(), containsString("WHERE emp_no > 10\n| EVAL first_name = field_extract(first_name, \"v\")"));
        assertThat(result.rewrittenFieldNames(), hasItem("first_name"));
    }

    /** {@code FROM a, (subquery)} ({@code UnionAll}) returns the union of branch scopes and emits no recovery EVAL. */
    public void testUnionAllReturnsUnionOfBranchScopesWithoutRecovery() {
        RewriteResult result = rewrite("FROM employees, (FROM apps | STATS c = COUNT(*)) | KEEP first_name", Set.of("first_name"));
        assertTrue(result.modified());
        // The union sources are untouched (no per-branch recovery EVAL is injected).
        assertThat(result.rewrittenQuery(), startsWith("FROM employees, (FROM apps | STATS c = COUNT(*)) | KEEP first_name"));
        // first_name is flattened in the employees branch (and cleared in the subquery), so the union scope keeps it:
        // it is recovered only by the top-level tail EVAL, proving union (not intersection) semantics.
        assertThat(result.rewrittenQuery(), endsWith("\n| EVAL first_name = field_extract(first_name, \"v\")"));
        assertEquals(Set.of("first_name"), result.rewrittenFieldNames());
    }

    /** {@code FROM a, (subquery)} still wraps an in-scope reference that appears inside a subquery branch. */
    public void testUnionAllWrapsInBranchSubqueryReference() {
        RewriteResult result = rewrite("FROM employees, (FROM apps | WHERE id == \"x\") | KEEP emp_no", Set.of("id"));
        assertTrue(result.modified());
        assertThat(result.rewrittenQuery(), containsString("WHERE field_extract(id, \"v\") == \"x\""));
        assertEquals(Set.of("id"), result.rewrittenFieldNames());
    }

    /** A field still flattened at end-of-pipeline gets a tail {@code EVAL name = field_extract(name, "v")}. */
    public void testTailRecoveryAppendsEvalForRemainingScope() {
        RewriteResult result = rewrite("FROM employees | SORT emp_no", Set.of("first_name"));
        assertTrue(result.modified());
        assertThat(result.rewrittenQuery(), endsWith("FROM employees | SORT emp_no\n| EVAL first_name = field_extract(first_name, \"v\")"));
        // With no expected column order there is no trailing KEEP.
        assertThat(result.rewrittenQuery(), not(containsString("\n| KEEP")));
        assertEquals(Set.of("first_name"), result.rewrittenFieldNames());
    }

    /** When an expected column order is supplied, tail recovery also appends a {@code KEEP} restoring positions. */
    public void testTailRecoveryAppendsKeepWhenColumnOrderSupplied() {
        RewriteResult result = rewrite("FROM employees | SORT emp_no", Set.of("first_name"), List.of("emp_no", "first_name"));
        assertTrue(result.modified());
        assertThat(result.rewrittenQuery(), containsString("\n| EVAL first_name = field_extract(first_name, \"v\")"));
        assertThat(result.rewrittenQuery(), endsWith("\n| KEEP emp_no, first_name"));
    }

    /** Edits in the shared pre-fork input pipeline are de-duplicated by span even though each branch re-walks it. */
    public void testSharedForkInputEditsDeduplicatedBySpan() {
        RewriteResult result = rewrite("FROM employees | WHERE first_name == \"x\" | FORK (LIMIT 1) (LIMIT 2)", Set.of("first_name"));
        assertTrue(result.modified());
        // The shared WHERE is walked once per branch; its wrap must appear exactly once in the output.
        assertEquals(1, countOccurrences(result.rewrittenQuery(), "WHERE field_extract(first_name, \"v\") == \"x\""));
        assertThat(result.rewrittenFieldNames(), hasItem("first_name"));
    }

    /** {@code rewrittenFieldNames} reports exactly the set of field names wrapped at least once. */
    public void testRewrittenFieldNamesReportsWrappedFields() {
        RewriteResult result = rewrite(
            "FROM employees | WHERE first_name == \"x\" AND gender == \"M\" | STATS c = COUNT(*)",
            Set.of("first_name", "gender", "last_name")
        );
        assertTrue(result.modified());
        // first_name and gender are referenced (and wrapped); last_name is in scope but never referenced.
        assertEquals(Set.of("first_name", "gender"), result.rewrittenFieldNames());
    }
}
