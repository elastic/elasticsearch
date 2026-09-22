/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.Operator;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.PartitionFilterHint;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.time.Instant;
import java.time.Period;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;

public class PartitionFilterHintExtractorTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;

    public void testEqualsHint() {
        LogicalPlan plan = filterAboveExternal(new Equals(SRC, unresolved("year"), intLiteral(2024)), "s3://bucket/data/*.parquet");

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(plan);

        assertEquals(1, hints.size());
        List<PartitionFilterHint> pathHints = hints.get("s3://bucket/data/*.parquet");
        assertNotNull(pathHints);
        assertEquals(1, pathHints.size());
        assertEquals("year", pathHints.get(0).columnName());
        assertEquals(Operator.EQUALS, pathHints.get(0).operator());
        assertEquals(List.of(2024), pathHints.get(0).values());
    }

    public void testNotEqualsHint() {
        LogicalPlan plan = filterAboveExternal(
            new NotEquals(SRC, unresolved("status"), keywordLiteral("deleted")),
            "s3://bucket/data/*.parquet"
        );

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(plan);

        assertEquals(1, hints.size());
        List<PartitionFilterHint> pathHints = hints.get("s3://bucket/data/*.parquet");
        assertEquals(1, pathHints.size());
        assertEquals(Operator.NOT_EQUALS, pathHints.get(0).operator());
        assertEquals("deleted", pathHints.get(0).values().get(0));
    }

    public void testGreaterThanHint() {
        LogicalPlan plan = filterAboveExternal(new GreaterThan(SRC, unresolved("year"), intLiteral(2020)), "s3://bucket/data/*.parquet");

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(plan);

        List<PartitionFilterHint> pathHints = hints.get("s3://bucket/data/*.parquet");
        assertEquals(1, pathHints.size());
        assertEquals(Operator.GREATER_THAN, pathHints.get(0).operator());
    }

    public void testGreaterThanOrEqualHint() {
        LogicalPlan plan = filterAboveExternal(
            new GreaterThanOrEqual(SRC, unresolved("year"), intLiteral(2020)),
            "s3://bucket/data/*.parquet"
        );

        List<PartitionFilterHint> pathHints = PartitionFilterHintExtractor.extract(plan).get("s3://bucket/data/*.parquet");
        assertEquals(Operator.GREATER_THAN_OR_EQUAL, pathHints.get(0).operator());
    }

    public void testLessThanHint() {
        LogicalPlan plan = filterAboveExternal(new LessThan(SRC, unresolved("year"), intLiteral(2025)), "s3://bucket/data/*.parquet");

        List<PartitionFilterHint> pathHints = PartitionFilterHintExtractor.extract(plan).get("s3://bucket/data/*.parquet");
        assertEquals(Operator.LESS_THAN, pathHints.get(0).operator());
    }

    public void testLessThanOrEqualHint() {
        LogicalPlan plan = filterAboveExternal(
            new LessThanOrEqual(SRC, unresolved("year"), intLiteral(2025)),
            "s3://bucket/data/*.parquet"
        );

        List<PartitionFilterHint> pathHints = PartitionFilterHintExtractor.extract(plan).get("s3://bucket/data/*.parquet");
        assertEquals(Operator.LESS_THAN_OR_EQUAL, pathHints.get(0).operator());
    }

    public void testReversedComparison() {
        LogicalPlan plan = filterAboveExternal(new LessThan(SRC, intLiteral(2020), unresolved("year")), "s3://bucket/data/*.parquet");

        List<PartitionFilterHint> pathHints = PartitionFilterHintExtractor.extract(plan).get("s3://bucket/data/*.parquet");
        assertEquals(1, pathHints.size());
        assertEquals("year", pathHints.get(0).columnName());
        assertEquals(Operator.GREATER_THAN, pathHints.get(0).operator());
    }

    public void testInHint() {
        LogicalPlan plan = filterAboveExternal(
            new In(SRC, unresolved("month"), List.of(intLiteral(1), intLiteral(2), intLiteral(3))),
            "s3://bucket/data/*.parquet"
        );

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(plan);

        List<PartitionFilterHint> pathHints = hints.get("s3://bucket/data/*.parquet");
        assertEquals(1, pathHints.size());
        assertEquals("month", pathHints.get(0).columnName());
        assertEquals(Operator.IN, pathHints.get(0).operator());
        assertEquals(List.of(1, 2, 3), pathHints.get(0).values());
    }

    public void testAndCombinedHints() {
        Expression condition = new And(
            SRC,
            new Equals(SRC, unresolved("year"), intLiteral(2024)),
            new In(SRC, unresolved("month"), List.of(intLiteral(1), intLiteral(2), intLiteral(3)))
        );

        LogicalPlan plan = filterAboveExternal(condition, "s3://bucket/data/*.parquet");

        List<PartitionFilterHint> pathHints = PartitionFilterHintExtractor.extract(plan).get("s3://bucket/data/*.parquet");
        assertEquals(2, pathHints.size());
        assertEquals("year", pathHints.get(0).columnName());
        assertEquals(Operator.EQUALS, pathHints.get(0).operator());
        assertEquals("month", pathHints.get(1).columnName());
        assertEquals(Operator.IN, pathHints.get(1).operator());
    }

    public void testNoFilterReturnsEmpty() {
        UnresolvedExternalRelation rel = new UnresolvedExternalRelation(SRC, Literal.keyword(SRC, "s3://bucket/data/*.parquet"), Map.of());

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(rel);
        assertTrue(hints.isEmpty());
    }

    /**
     * Field {@code DATE_EXTRACT} is not a Hive folder bind. C2 inverts it after analysis;
     * listing still sees an unresolved function and must not emit {@code year=}.
     */
    public void testDateExtractOnTimestampFieldIsNotHiveHint() {
        String path = "s3://bucket/data/year=*/*.parquet";
        Expression extract = new UnresolvedFunction(SRC, "DATE_EXTRACT", List.of(keywordLiteral("year"), unresolved("ts")));
        LogicalPlan plan = filterAboveExternal(new Equals(SRC, extract, intLiteral(2024)), path);

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(plan);
        assertTrue("DATE_EXTRACT on ts must not emit year=", hints.isEmpty());
        assertEquals(path, GlobExpander.rewriteGlobWithHints(path, hints.getOrDefault(path, List.of())));
    }

    public void testDateTruncOnTimestampFieldIsNotHiveHint() {
        String path = "s3://bucket/data/year=*/*.parquet";
        Expression trunc = new UnresolvedFunction(
            SRC,
            "DATE_TRUNC",
            List.of(new Literal(SRC, Period.ofYears(1), DataType.DATE_PERIOD), unresolved("ts"))
        );
        LogicalPlan plan = filterAboveExternal(new Equals(SRC, trunc, keywordLiteral("2024-01-01T00:00:00Z")), path);

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(plan);
        assertTrue("DATE_TRUNC on ts must not emit year=", hints.isEmpty());
        assertEquals(path, GlobExpander.rewriteGlobWithHints(path, hints.getOrDefault(path, List.of())));
    }

    public void testUnsupportedExpressionIgnored() {
        // func(year) = 2024 — function call on column side, should be ignored
        // We simulate this by using a Literal on both sides (neither is UnresolvedAttribute)
        Expression condition = new Equals(SRC, intLiteral(1), intLiteral(2024));

        LogicalPlan plan = filterAboveExternal(condition, "s3://bucket/data/*.parquet");

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(plan);
        assertTrue(hints.isEmpty());
    }

    public void testInWithNonLiteralListIgnored() {
        // IN where one list item is not a literal — should be ignored
        Expression condition = new In(SRC, unresolved("month"), List.of(intLiteral(1), unresolved("other")));

        LogicalPlan plan = filterAboveExternal(condition, "s3://bucket/data/*.parquet");

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(plan);
        assertTrue(hints.isEmpty());
    }

    public void testKeywordLiteralNormalizedToString() {
        LogicalPlan plan = filterAboveExternal(new Equals(SRC, unresolved("country"), keywordLiteral("US")), "s3://bucket/data/*.parquet");

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(plan);

        List<PartitionFilterHint> pathHints = hints.get("s3://bucket/data/*.parquet");
        assertEquals(1, pathHints.size());
        assertEquals("country", pathHints.get(0).columnName());
        assertEquals(Operator.EQUALS, pathHints.get(0).operator());
        Object value = pathHints.get(0).values().get(0);
        assertFalse("hint value should not be a BytesRef", value instanceof BytesRef);
        assertEquals("US", value);
    }

    public void testInWithKeywordLiteralsNormalizedToString() {
        LogicalPlan plan = filterAboveExternal(
            new In(SRC, unresolved("country"), List.of(keywordLiteral("US"), keywordLiteral("DE"))),
            "s3://bucket/data/*.parquet"
        );

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(plan);

        List<PartitionFilterHint> pathHints = hints.get("s3://bucket/data/*.parquet");
        assertEquals(1, pathHints.size());
        assertEquals(Operator.IN, pathHints.get(0).operator());
        for (Object val : pathHints.get(0).values()) {
            assertFalse("hint value should not be a BytesRef", val instanceof BytesRef);
        }
        assertEquals(List.of("US", "DE"), pathHints.get(0).values());
    }

    public void testMultipleExternalRelations() {
        UnresolvedExternalRelation rel1 = new UnresolvedExternalRelation(SRC, Literal.keyword(SRC, "s3://bucket/a/*.parquet"), Map.of());
        UnresolvedExternalRelation rel2 = new UnresolvedExternalRelation(SRC, Literal.keyword(SRC, "s3://bucket/b/*.parquet"), Map.of());

        // Filter -> rel1 (only rel1 gets the hint)
        Filter filterPlan = new Filter(SRC, rel1, new Equals(SRC, unresolved("year"), intLiteral(2024)));

        Map<String, List<PartitionFilterHint>> hints = PartitionFilterHintExtractor.extract(filterPlan);
        assertEquals(1, hints.size());
        assertNotNull(hints.get("s3://bucket/a/*.parquet"));
        assertNull(hints.get("s3://bucket/b/*.parquet"));
    }

    // -- A hint rewrites the glob, so folders it excludes are never listed. These pin when it may not be trusted. --

    /**
     * {@code SORT id | LIMIT 4 | WHERE year == 2025} takes four rows and filters them <em>afterwards</em>. Narrowing
     * the listing to {@code year=2025/} first would refill the {@code LIMIT} window from the surviving folders and
     * return rows the query never asked for. No hint may cross a {@code LIMIT}.
     */
    public void testLimitBarrierDropsHints() {
        UnresolvedExternalRelation rel = externalRelation(PATH);
        LogicalPlan limit = new Limit(SRC, intLiteral(4), rel);
        LogicalPlan plan = new Filter(SRC, limit, new Equals(SRC, unresolved("year"), intLiteral(2025)));

        assertTrue("a filter above a LIMIT must not narrow the listing", PartitionFilterHintExtractor.extract(plan).isEmpty());
    }

    /** Same rule, a node that collapses rows rather than truncating them. */
    public void testAggregateBarrierDropsHints() {
        UnresolvedExternalRelation rel = externalRelation(PATH);
        LogicalPlan stats = new Aggregate(SRC, rel, List.of(), List.of(unresolved("id")));
        LogicalPlan plan = new Filter(SRC, stats, new Equals(SRC, unresolved("year"), intLiteral(2025)));

        assertTrue("a filter above STATS must not narrow the listing", PartitionFilterHintExtractor.extract(plan).isEmpty());
    }

    /**
     * {@code EVAL year = ...} redefines {@code year} as a row value that has nothing to do with the {@code year=2024/}
     * folder the row came from. Rewriting the glob to {@code year=615/} would list nothing at all.
     */
    public void testEvalShadowDropsMatchingHint() {
        UnresolvedExternalRelation rel = externalRelation(PATH);
        LogicalPlan eval = new Eval(SRC, rel, List.of(new Alias(SRC, "year", unresolved("id"))));
        LogicalPlan plan = new Filter(SRC, eval, new Equals(SRC, unresolved("year"), intLiteral(615)));

        assertTrue(
            "a hint on a column an EVAL redefined must not narrow the listing",
            PartitionFilterHintExtractor.extract(plan).isEmpty()
        );
    }

    /** Only the shadowed conjunct is dropped — a genuine partition predicate alongside it still prunes. */
    public void testEvalShadowKeepsUnrelatedHint() {
        UnresolvedExternalRelation rel = externalRelation(PATH);
        LogicalPlan eval = new Eval(SRC, rel, List.of(new Alias(SRC, "year", unresolved("id"))));
        Expression condition = new And(
            SRC,
            new Equals(SRC, unresolved("year"), intLiteral(615)),
            new Equals(SRC, unresolved("month"), intLiteral(6))
        );
        LogicalPlan plan = new Filter(SRC, eval, condition);

        List<PartitionFilterHint> hints = PartitionFilterHintExtractor.extract(plan).get(PATH);
        assertNotNull("the untouched `month` predicate must survive", hints);
        assertEquals(1, hints.size());
        assertEquals("month", hints.get(0).columnName());
    }

    /**
     * Direction check. Here the {@code WHERE} reads the real partition column and the {@code EVAL} only affects rows
     * downstream of it, so the hint is still good. Applying shadowing at the relation rather than at the moment the
     * walk passes the {@code EVAL} would wrongly drop it.
     */
    public void testFilterBelowEvalKeepsHint() {
        UnresolvedExternalRelation rel = externalRelation(PATH);
        LogicalPlan filter = new Filter(SRC, rel, new Equals(SRC, unresolved("year"), intLiteral(2025)));
        LogicalPlan plan = new Eval(SRC, filter, List.of(new Alias(SRC, "year", intLiteral(9))));

        List<PartitionFilterHint> hints = PartitionFilterHintExtractor.extract(plan).get(PATH);
        assertNotNull("a filter BELOW the EVAL reads the partition column and must still prune", hints);
        assertEquals("year", hints.get(0).columnName());
    }

    /** {@code RENAME id AS year} makes `year` row-derived just as an EVAL would; the new name is the dangerous one. */
    public void testRenameShadowDropsHint() {
        UnresolvedExternalRelation rel = externalRelation(PATH);
        LogicalPlan rename = new Rename(SRC, rel, List.of(new Alias(SRC, "year", unresolved("id"))));
        LogicalPlan plan = new Filter(SRC, rename, new Equals(SRC, unresolved("year"), intLiteral(615)));

        assertTrue("a hint on a RENAMEd column must not narrow the listing", PartitionFilterHintExtractor.extract(plan).isEmpty());
    }

    /**
     * {@code ENRICH} is the reason the listing layer cannot reuse the {@code Streaming} marker: it is row-preserving,
     * but until the analyzer loads the policy its generated fields are unknown, so a policy that contributes a
     * {@code year} column would shadow the partition column invisibly. It must stop the hint.
     */
    public void testEnrichBarrierDropsHints() {
        UnresolvedExternalRelation rel = externalRelation(PATH);
        LogicalPlan enrich = new Enrich(
            SRC,
            rel,
            Enrich.Mode.ANY,
            Literal.keyword(SRC, "policy"),
            unresolved("id"),
            null,
            Map.of(),
            List.of()
        );
        LogicalPlan plan = new Filter(SRC, enrich, new Equals(SRC, unresolved("year"), intLiteral(2025)));

        assertTrue(
            "ENRICH may introduce columns not knowable pre-resolution, so no hint may cross it",
            PartitionFilterHintExtractor.extract(plan).isEmpty()
        );
    }

    /** KEEP/DROP only remove columns; they never redefine one, so the common shapes must keep pruning. */
    public void testKeepAndDropAreTransparent() {
        UnresolvedExternalRelation rel = externalRelation(PATH);
        LogicalPlan drop = new Drop(SRC, rel, List.of(unresolved("unused")));
        LogicalPlan plan = new Filter(SRC, drop, new Equals(SRC, unresolved("year"), intLiteral(2025)));

        List<PartitionFilterHint> hints = PartitionFilterHintExtractor.extract(plan).get(PATH);
        assertNotNull("DROP must not stop a partition hint", hints);
        assertEquals("year", hints.get(0).columnName());
    }

    // -- One listing serves every occurrence of a path, so a folder may be skipped only if EVERY occurrence excludes it --

    /**
     * {@code FORK} reaches the same relation from two branches, and both are served by one listing. A folder excluded
     * by one branch's filter may still be needed by the other, so an unfiltered branch has to veto the rewrite
     * outright — otherwise it silently loses the folders the other branch pruned away.
     */
    public void testForkUnfilteredBranchVetoesTheRewrite() {
        UnresolvedExternalRelation rel = externalRelation(PATH);
        LogicalPlan guarded = new Filter(SRC, rel, new Equals(SRC, unresolved("year"), intLiteral(2025)));
        LogicalPlan fork = new Fork(SRC, List.of(guarded, rel), List.of());

        assertTrue(
            "one branch's filter must not narrow a listing the other branch reads unfiltered",
            PartitionFilterHintExtractor.extract(fork).isEmpty()
        );
    }

    /** Both branches exclude the same folders, so the listing may safely skip them. */
    public void testForkAgreeingBranchesKeepTheCommonHint() {
        UnresolvedExternalRelation rel = externalRelation(PATH);
        LogicalPlan left = new Filter(SRC, rel, new Equals(SRC, unresolved("year"), intLiteral(2025)));
        LogicalPlan right = new Filter(SRC, rel, new Equals(SRC, unresolved("year"), intLiteral(2025)));
        LogicalPlan fork = new Fork(SRC, List.of(left, right), List.of());

        List<PartitionFilterHint> hints = PartitionFilterHintExtractor.extract(fork).get(PATH);
        assertNotNull("both branches want only year=2025, so the rest may be skipped", hints);
        assertEquals(1, hints.size());
        assertEquals("year", hints.get(0).columnName());
    }

    /** Branches wanting different years between them need every folder, so nothing may be skipped. */
    public void testForkDisagreeingBranchesKeepNoHint() {
        UnresolvedExternalRelation rel = externalRelation(PATH);
        LogicalPlan left = new Filter(SRC, rel, new Equals(SRC, unresolved("year"), intLiteral(2025)));
        LogicalPlan right = new Filter(SRC, rel, new Equals(SRC, unresolved("year"), intLiteral(2024)));
        LogicalPlan fork = new Fork(SRC, List.of(left, right), List.of());

        assertTrue(
            "the branches need different folders, so neither may narrow the shared listing",
            PartitionFilterHintExtractor.extract(fork).isEmpty()
        );
    }

    /**
     * Extractor stays attr-vs-literal: a raw {@code DATE_EXTRACT} call is not a hint even when
     * every argument is a literal. Listing must run the preprocessor first.
     */
    public void testExtractorIgnoresRawDateExtract() {
        Expression condition = new Equals(SRC, unresolved("year"), dateExtract("YEAR", datetimeLiteral(DASHBOARD_TS)));
        LogicalPlan plan = filterAboveExternal(condition, PATH);

        assertTrue("extractor must stay dumb about UnresolvedFunction", PartitionFilterHintExtractor.extract(plan).isEmpty());
    }

    public void testPreprocessorFoldsDashboardDateExtracts() {
        Literal ts = datetimeLiteral(DASHBOARD_TS);
        Expression condition = new And(
            SRC,
            new And(
                SRC,
                new Equals(SRC, unresolved("year"), dateExtract("YEAR", ts)),
                new Equals(SRC, unresolved("month"), dateExtract("MONTH_OF_YEAR", ts))
            ),
            new Equals(SRC, unresolved("day"), dateExtract("DAY_OF_MONTH", ts))
        );
        LogicalPlan plan = filterAboveExternal(condition, PATH);

        List<PartitionFilterHint> hints = extractFolded(plan).get(PATH);
        assertNotSame("folding must copy the Filter, not mutate the session plan", plan, foldForListing(plan));
        assertNotNull(hints);
        assertEquals(3, hints.size());
        assertEquals(new PartitionFilterHint("year", Operator.EQUALS, List.of(2026L)), hints.get(0));
        assertEquals(new PartitionFilterHint("month", Operator.EQUALS, List.of(7L)), hints.get(1));
        assertEquals(new PartitionFilterHint("day", Operator.EQUALS, List.of(13L)), hints.get(2));

        assertTrue(
            "session plan must keep the unresolved DATE_EXTRACT calls",
            ((Filter) plan).condition().anyMatch(e -> e instanceof UnresolvedFunction)
        );
    }

    public void testPreprocessorFoldsDateExtractOnLeft() {
        Expression condition = new Equals(SRC, dateExtract("YEAR", datetimeLiteral(DASHBOARD_TS)), unresolved("year"));
        LogicalPlan plan = filterAboveExternal(condition, PATH);

        List<PartitionFilterHint> hints = extractFolded(plan).get(PATH);
        assertNotNull(hints);
        assertEquals(1, hints.size());
        assertEquals(new PartitionFilterHint("year", Operator.EQUALS, List.of(2026L)), hints.get(0));
    }

    public void testPreprocessorDoesNotFoldFieldDateExtract() {
        Expression condition = new Equals(SRC, unresolved("year"), dateExtract("YEAR", unresolved("start")));
        LogicalPlan plan = filterAboveExternal(condition, PATH);

        assertTrue("DATE_EXTRACT over an unresolved field cannot become a partition hint", extractFolded(plan).isEmpty());
        assertSame("nothing folded, so the listing copy is the original plan", plan, foldForListing(plan));
    }

    /**
     * Fold + hint are real. {@link Operator#canRewriteGlob} is EQUALS/IN only, so this
     * GREATER_THAN is the same as {@code year > 2024}: list every prefix, drop files at
     * split-discovery. Do not teach the glob ranges here.
     */
    public void testPreprocessorFoldsYearGreaterThanLiteral() {
        Expression condition = new GreaterThan(SRC, unresolved("year"), yearFn(keywordLiteral("2024-01-01")));
        LogicalPlan plan = filterAboveExternal(condition, PATH);

        List<PartitionFilterHint> hints = extractFolded(plan).get(PATH);
        assertEquals(List.of(new PartitionFilterHint("year", Operator.GREATER_THAN, List.of(2024L))), hints);
    }

    public void testPreprocessorFoldsYearEqualsParam() {
        Expression condition = new Equals(SRC, yearFn(datetimeLiteral(DASHBOARD_TS)), unresolved("year"));
        LogicalPlan plan = filterAboveExternal(condition, PATH);

        List<PartitionFilterHint> hints = extractFolded(plan).get(PATH);
        assertEquals(List.of(new PartitionFilterHint("year", Operator.EQUALS, List.of(2026L))), hints);
    }

    public void testPreprocessorFoldsMonthEqualsLiteral() {
        Expression condition = new Equals(SRC, unresolved("month"), monthFn(keywordLiteral("2024-06-15")));
        LogicalPlan plan = filterAboveExternal(condition, PATH);

        List<PartitionFilterHint> hints = extractFolded(plan).get(PATH);
        assertEquals(List.of(new PartitionFilterHint("month", Operator.EQUALS, List.of(6L))), hints);
    }

    public void testPreprocessorFoldsNestedDateExtractOverDateTrunc() {
        UnresolvedFunction trunc = new UnresolvedFunction(
            SRC,
            "DATE_TRUNC",
            List.of(new Literal(SRC, Period.ofDays(1), DataType.DATE_PERIOD), datetimeLiteral(DASHBOARD_TS))
        );
        LogicalPlan plan = filterAboveExternal(new Equals(SRC, unresolved("year"), dateExtract("YEAR", trunc)), PATH);

        List<PartitionFilterHint> hints = extractFolded(plan).get(PATH);
        assertEquals(List.of(new PartitionFilterHint("year", Operator.EQUALS, List.of(2026L))), hints);
    }

    private static final String PATH = "s3://bucket/data/*.parquet";
    private static final long DASHBOARD_TS = Instant.parse("2026-07-13T00:00:00Z").toEpochMilli();
    private static final EsqlFunctionRegistry FUNCTION_REGISTRY = new EsqlFunctionRegistry();

    private static UnresolvedExternalRelation externalRelation(String path) {
        return new UnresolvedExternalRelation(SRC, Literal.keyword(SRC, path), Map.of());
    }

    private static LogicalPlan filterAboveExternal(Expression condition, String path) {
        UnresolvedExternalRelation rel = new UnresolvedExternalRelation(SRC, Literal.keyword(SRC, path), Map.of());
        return new Filter(SRC, rel, condition);
    }

    private static UnresolvedAttribute unresolved(String name) {
        return new UnresolvedAttribute(SRC, name);
    }

    private static Literal intLiteral(int value) {
        return new Literal(SRC, value, DataType.INTEGER);
    }

    private static Literal keywordLiteral(String value) {
        return Literal.keyword(SRC, value);
    }

    private static Literal datetimeLiteral(long millis) {
        return new Literal(SRC, millis, DataType.DATETIME);
    }

    private static UnresolvedFunction dateExtract(String chrono, Expression date) {
        return new UnresolvedFunction(SRC, "DATE_EXTRACT", List.of(Literal.keyword(SRC, chrono), date));
    }

    private static UnresolvedFunction yearFn(Expression date) {
        return new UnresolvedFunction(SRC, "YEAR", List.of(date));
    }

    private static UnresolvedFunction monthFn(Expression date) {
        return new UnresolvedFunction(SRC, "MONTH", List.of(date));
    }

    private static LogicalPlan foldForListing(LogicalPlan plan) {
        return FoldDateFunctionFiltersForListing.fold(plan, TEST_CFG, FUNCTION_REGISTRY);
    }

    private static Map<String, List<PartitionFilterHint>> extractFolded(LogicalPlan plan) {
        return PartitionFilterHintExtractor.extract(foldForListing(plan));
    }
}
