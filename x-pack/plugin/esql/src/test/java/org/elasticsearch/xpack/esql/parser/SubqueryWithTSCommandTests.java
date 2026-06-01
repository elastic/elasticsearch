/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.randomIndexPatterns;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.unquoteIndexPattern;
import static org.hamcrest.Matchers.containsString;

/**
 * Parser tests for subqueries with {@code TS} as source command.
 *
 * Subquery with {@code TS} source command is supported in the FROM command and WHERE IN subquery in grammar and parser.
 * WHERE IN subquery with {@code TS} as source command is not fully supported by downstream component yet.
 */
public class SubqueryWithTSCommandTests extends AbstractStatementParserTests {

    @Before
    public void checkSubqueryWithTSCommand() {
        assumeTrue("Requires subquery with TS as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
    }

    private static void requireSubqueryInFromCommand() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
    }

    private static void requireWhereInSubquery() {
        assumeTrue("Requires WHERE IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY.isEnabled());
    }

    private static void requireSubqueryWithRow() {
        assumeTrue("Requires subquery with ROW as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
    }

    // subquery in the from command

    /**
     * Single TS subquery alongside an index pattern in the main FROM.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_UnresolvedRelation[, TIME_SERIES]
     */
    public void testIndexPatternWithTSSubquery() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (TS {})
            """, mainQueryIndexPattern, tsSubqueryIndexPattern);

        LogicalPlan plan = query(query);

        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());
        // branch 1
        assertStandardRelation(children.get(0), mainQueryIndexPattern);
        // branch 2
        Subquery subquery = as(children.get(1), Subquery.class);
        assertTSRelation(subquery.plan(), tsSubqueryIndexPattern);
    }

    /**
     * Mix of an index pattern, a TS subquery and a FROM subquery — the user-facing example.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * |_Subquery[]
     * | \_Filter[?x &gt; 1[INTEGER]]
     * |   \_UnresolvedRelation[, TIME_SERIES]
     * \_Subquery[]
     *   \_Filter[?x &gt; 1[INTEGER]]
     *     \_UnresolvedRelation[]
     */
    public void testIndexPatternWithTSAndFromSubqueries() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        var fromSubqueryIndexPattern = randomIndexPatterns();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (TS {} | WHERE x > 1), (FROM {} | WHERE x > 1)
            """, mainQueryIndexPattern, tsSubqueryIndexPattern, fromSubqueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(3, children.size());
        // branch 1
        assertStandardRelation(children.get(0), mainQueryIndexPattern);
        // branch 2
        Subquery tsSubquery = as(children.get(1), Subquery.class);
        Filter tsFilter = as(tsSubquery.plan(), Filter.class);
        GreaterThan tsFilterCondition = as(tsFilter.condition(), GreaterThan.class);
        assertEquals("x", as(tsFilterCondition.left(), Attribute.class).name());
        assertTSRelation(tsFilter.child(), tsSubqueryIndexPattern);
        // branch 3
        Subquery fromSubquery = as(children.get(2), Subquery.class);
        Filter fromFilter = as(fromSubquery.plan(), Filter.class);
        assertStandardRelation(fromFilter.child(), fromSubqueryIndexPattern);
    }

    /**
     * A TS subquery with several processing commands inside.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_Limit[5[INTEGER],false,false]
     *     \_Eval[[?x + 1[INTEGER] AS y]]
     *       \_Filter[?x &gt; 0[INTEGER]]
     *         \_UnresolvedRelation[, TIME_SERIES]
     */
    public void testTSSubqueryWithProcessingCommandsInSubquery() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (TS {}
                      | WHERE x > 0
                      | EVAL y = x + 1
                      | LIMIT 5)
            """, mainQueryIndexPattern, tsSubqueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());
        // branch 1
        assertStandardRelation(children.get(0), mainQueryIndexPattern);
        // branch 2
        Subquery subquery = as(children.get(1), Subquery.class);
        Limit limit = as(subquery.plan(), Limit.class);
        Eval eval = as(limit.child(), Eval.class);
        Filter filter = as(eval.child(), Filter.class);
        assertTSRelation(filter.child(), tsSubqueryIndexPattern);
    }

    /**
     * TS subquery combined with processing commands in the main query.
     *
     * Limit[10[INTEGER],false,false]
     * \_Filter[?x &gt; 5[INTEGER]]
     *   \_UnionAll[[]]
     *     |_UnresolvedRelation[]
     *     \_Subquery[]
     *       \_UnresolvedRelation[, TIME_SERIES]
     */
    public void testTSSubqueryWithProcessingCommandsInMainQuery() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (TS {})
            | WHERE x > 5
            | LIMIT 10
            """, mainQueryIndexPattern, tsSubqueryIndexPattern);

        LogicalPlan plan = query(query);
        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());
        // branch 1
        assertStandardRelation(children.get(0), mainQueryIndexPattern);
        // branch 2
        Subquery subquery = as(children.get(1), Subquery.class);
        assertTSRelation(subquery.plan(), tsSubqueryIndexPattern);
    }

    /**
     * Processing commands in both the TS subquery and the main query.
     *
     * Limit[10[INTEGER],false,false]
     * \_Filter[?y &gt; 0[INTEGER]]
     *   \_UnionAll[[]]
     *     |_UnresolvedRelation[]
     *     \_Subquery[]
     *       \_Eval[[?x + 1[INTEGER] AS y]]
     *         \_UnresolvedRelation[, TIME_SERIES]
     */
    public void testTSSubqueryWithProcessingCommandsInSubqueryAndMainQuery() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (TS {} | EVAL y = x + 1)
            | WHERE y > 0
            | LIMIT 10
            """, mainQueryIndexPattern, tsSubqueryIndexPattern);

        LogicalPlan plan = query(query);
        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());
        // branch 1
        assertStandardRelation(children.get(0), mainQueryIndexPattern);
        // branch 2
        Subquery subquery = as(children.get(1), Subquery.class);
        Eval eval = as(subquery.plan(), Eval.class);
        assertTSRelation(eval.child(), tsSubqueryIndexPattern);
    }

    /**
     * If the only child of FROM is a TS subquery, the {@code UnionAll} is collapsed and the TS
     * {@link UnresolvedRelation} (with its trailing processing commands) is returned directly,
     * mirroring the behaviour for a single FROM subquery in {@link SubqueryTests#testSubqueryOnly()}.
     *
     * Filter[?x &gt; 5[INTEGER]]
     * \_UnresolvedRelation[, TIME_SERIES]
     */
    public void testTSSubqueryOnly() {
        requireSubqueryInFromCommand();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM (TS {} | WHERE x > 5)
            """, tsSubqueryIndexPattern);

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        GreaterThan gt = as(filter.condition(), GreaterThan.class);
        assertEquals("x", as(gt.left(), Attribute.class).name());
        assertTSRelation(filter.child(), tsSubqueryIndexPattern);
    }

    /**
     * Multiple TS subqueries with no main index pattern produce a {@code UnionAll} of {@code Subquery}
     * over the TS {@link UnresolvedRelation}s.
     *
     * UnionAll[[]]
     * |_Subquery[]
     * | \_Filter[?x &gt; 5[INTEGER]]
     * |   \_UnresolvedRelation[, TIME_SERIES]
     * |_Subquery[]
     * | \_TimeSeriesAggregate[[],[?count[*] AS count(*)],null,null,?@timestamp,false]
     * |   \_UnresolvedRelation[, TIME_SERIES]
     * \_Subquery[]
     *   \_OrderBy[[Order[?y,ASC,LAST]]]
     *     \_UnresolvedRelation[, TIME_SERIES]
     */
    public void testMultipleTSSubqueriesOnly() {
        requireSubqueryInFromCommand();
        var tsSubqueryIndexPattern1 = randomIndexPatterns();
        var tsSubqueryIndexPattern2 = randomIndexPatterns();
        var tsSubqueryIndexPattern3 = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM (TS {} | WHERE x > 5), (TS {} | STATS count(*)), (TS {} | SORT y)
            """, tsSubqueryIndexPattern1, tsSubqueryIndexPattern2, tsSubqueryIndexPattern3);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(3, children.size());
        // branch 1
        Subquery subquery1 = as(children.get(0), Subquery.class);
        Filter filter1 = as(subquery1.plan(), Filter.class);
        as(filter1.condition(), GreaterThan.class);
        assertTSRelation(filter1.child(), tsSubqueryIndexPattern1);
        // branch 2
        Subquery subquery2 = as(children.get(1), Subquery.class);
        Aggregate aggregate = as(subquery2.plan(), TimeSeriesAggregate.class);
        assertTSRelation(aggregate.child(), tsSubqueryIndexPattern2);
        // branch 3
        Subquery subquery3 = as(children.get(2), Subquery.class);
        OrderBy orderBy = as(subquery3.plan(), OrderBy.class);
        assertTSRelation(orderBy.child(), tsSubqueryIndexPattern3);
    }

    /**
     * A TS subquery and a FROM subquery without a main index pattern.
     *
     * UnionAll[[]]
     * |_Subquery[]
     * | \_UnresolvedRelation[, TIME_SERIES]
     * \_Subquery[]
     *   \_UnresolvedRelation[]
     */
    public void testTSAndFromSubqueriesOnly() {
        requireSubqueryInFromCommand();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        var fromSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM (TS {}), (FROM {})
            """, tsSubqueryIndexPattern, fromSubqueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());
        // branch 1
        Subquery tsSubquery = as(children.get(0), Subquery.class);
        assertTSRelation(tsSubquery.plan(), tsSubqueryIndexPattern);
        // branch 2
        Subquery fromSubquery = as(children.get(1), Subquery.class);
        assertStandardRelation(fromSubquery.plan(), fromSubqueryIndexPattern);
    }

    /**
     * A TS subquery nested inside a FROM subquery.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_UnionAll[[]]
     *     |_UnresolvedRelation[]
     *     \_Subquery[]
     *       \_UnresolvedRelation[, TIME_SERIES]
     */
    public void testTSSubqueryNestedInsideFromSubquery() {
        requireSubqueryInFromCommand();
        var outerIndexPattern = randomIndexPatterns();
        var innerIndexPattern = randomIndexPatterns();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (FROM {}, (TS {}))
            """, outerIndexPattern, innerIndexPattern, tsSubqueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll outerUnion = as(plan, UnionAll.class);
        List<LogicalPlan> outerChildren = outerUnion.children();
        assertEquals(2, outerChildren.size());
        // branch 1
        assertStandardRelation(outerChildren.get(0), outerIndexPattern);
        // branch 2
        Subquery outerSubquery = as(outerChildren.get(1), Subquery.class);
        UnionAll innerUnion = as(outerSubquery.plan(), UnionAll.class);
        List<LogicalPlan> innerChildren = innerUnion.children();
        assertEquals(2, innerChildren.size());
        // branch 2.1
        assertStandardRelation(innerChildren.get(0), innerIndexPattern);
        // branch 2.2
        Subquery innerSubquery = as(innerChildren.get(1), Subquery.class);
        assertTSRelation(innerSubquery.plan(), tsSubqueryIndexPattern);
    }

    /**
     * Verifies the parser accepts a TS subquery whose trailing processing command sits in each of the
     * different ANTLR lexer modes the {@code processingCommand} rule can transition into. The shape of the
     * tree is asserted only at a high level since the goal is to ensure no parse errors occur.
     */
    public void testTSSubqueryEndsWithProcessingCommandsInDifferentMode() {
        requireSubqueryInFromCommand();
        List<String> processingCommandInDifferentMode = List.of(
            "INLINE STATS max_x = MAX(x) BY x",
            "DISSECT y \"%{a} %{b}\"",
            "ENRICH clientip_policy ON x WITH env",
            "CHANGE_POINT x ON x AS type, pvalue",
            "FORK (WHERE x < 100) (WHERE x > 200)",
            "MV_EXPAND x",
            "RENAME x AS z",
            "DROP x"
        );
        var mainQueryIndexPattern = randomIndexPatterns();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        for (String processingCommand : processingCommandInDifferentMode) {
            String query = LoggerMessageFormat.format(null, """
                FROM {}, (TS {} | {})
                | WHERE x > 0
                """, mainQueryIndexPattern, tsSubqueryIndexPattern, processingCommand);

            LogicalPlan plan = query(query);
            Filter filter = as(plan, Filter.class);
            UnionAll unionAll = as(filter.child(), UnionAll.class);
            List<LogicalPlan> children = unionAll.children();
            assertEquals(2, children.size());
            // branch 1
            assertStandardRelation(children.get(0), mainQueryIndexPattern);
            // branch 2
            as(children.get(1), Subquery.class);
        }
    }

    // WHERE (NOT) IN (TS ...)

    /**
     * A basic IN subquery whose source command is TS:
     * {@code FROM main | WHERE x IN (TS ts_idx)}.
     *
     * Filter[InSubquery[?x, UnresolvedRelation[, TIME_SERIES]]]
     * \_UnresolvedRelation[main]
     */
    public void testWhereInTSSubqueryBasic() {
        requireWhereInSubquery();
        boolean fromContext = randomBoolean();
        var mainQueryIndexPattern = randomIndexPatterns();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            {} {}
            | WHERE x IN (TS {})
            """, fromOrTS(fromContext), mainQueryIndexPattern, tsSubqueryIndexPattern);

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);
        UnresolvedAttribute value = as(inSubquery.value(), UnresolvedAttribute.class);
        assertEquals("x", value.name());
        assertTSRelation(inSubquery.subquery(), tsSubqueryIndexPattern);
        validateRelation(filter.child(), fromContext, mainQueryIndexPattern);
    }

    /**
     * A NOT IN subquery whose source command is TS:
     * {@code FROM main | WHERE x NOT IN (TS ts_idx)}.
     *
     * Filter[NOT(InSubquery[?x, UnresolvedRelation[, TIME_SERIES]])]
     * \_UnresolvedRelation[main]
     */
    public void testWhereNotInTSSubquery() {
        requireWhereInSubquery();
        boolean fromContext = randomBoolean();
        var mainQueryIndexPattern = randomIndexPatterns();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            {} {}
            | WHERE x NOT IN (TS {})
            """, fromOrTS(fromContext), mainQueryIndexPattern, tsSubqueryIndexPattern);

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Not not = as(filter.condition(), Not.class);
        InSubquery inSubquery = as(not.field(), InSubquery.class);
        UnresolvedAttribute value = as(inSubquery.value(), UnresolvedAttribute.class);
        assertEquals("x", value.name());
        assertTSRelation(inSubquery.subquery(), tsSubqueryIndexPattern);
        validateRelation(filter.child(), fromContext, mainQueryIndexPattern);
    }

    /**
     * IN TS subquery with processing commands inside the subquery:
     * {@code FROM main | WHERE x (NOT)? IN (TS ts_idx | WHERE a > 0 | EVAL b = a + 1 | KEEP a | LIMIT 5)}.
     *
     * Filter[(NOT) InSubquery[?x, Limit[5[INTEGER],false,false]]]
     * \_UnresolvedRelation[main]
     *
     * Where the IN subquery's inner plan is Limit -&gt; Keep -&gt; Eval -&gt; Filter -&gt; TS UnresolvedRelation.
     */
    public void testWhereInTSSubqueryWithProcessingCommands() {
        requireWhereInSubquery();
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        boolean fromContext = randomBoolean();
        var mainQueryIndexPattern = randomIndexPatterns();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            {} {}
            | WHERE x {}IN (TS {}
                            | WHERE a > 0
                            | EVAL b = a + 1
                            | KEEP a
                            | LIMIT 5)
            """, fromOrTS(fromContext), mainQueryIndexPattern, notClause, tsSubqueryIndexPattern);

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery;
        if (negated) {
            Not not = as(filter.condition(), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(filter.condition(), InSubquery.class);
        }
        assertEquals("x", as(inSubquery.value(), UnresolvedAttribute.class).name());

        Limit limit = as(inSubquery.subquery(), Limit.class);
        Keep keep = as(limit.child(), Keep.class);
        Eval eval = as(keep.child(), Eval.class);
        Filter subqueryFilter = as(eval.child(), Filter.class);
        GreaterThan gt = as(subqueryFilter.condition(), GreaterThan.class);
        assertEquals("a", as(gt.left(), Attribute.class).name());
        assertTSRelation(subqueryFilter.child(), tsSubqueryIndexPattern);
        validateRelation(filter.child(), fromContext, mainQueryIndexPattern);
    }

    /**
     * IN TS subquery combined with another boolean condition in the WHERE clause:
     * {@code FROM main | WHERE x > 5 AND y IN (TS ts_idx | ...)}.
     *
     * Filter[?x &gt; 5[INTEGER] AND InSubquery[?y, Limit[5[INTEGER],false,false]]]
     * \_UnresolvedRelation[main]
     *
     * Where the IN subquery's inner plan is Limit -&gt; Keep -&gt; Eval -&gt; Filter -&gt; TS UnresolvedRelation.
     */
    public void testWhereInTSSubqueryWithOtherConditions() {
        requireWhereInSubquery();
        boolean fromContext = randomBoolean();
        var mainQueryIndexPattern = randomIndexPatterns();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            {} {}
            | WHERE x > 5
              AND y IN (TS {}
                        | WHERE a > 0
                        | EVAL b = a + 1
                        | KEEP a
                        | LIMIT 5)
            """, fromOrTS(fromContext), mainQueryIndexPattern, tsSubqueryIndexPattern);

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        And and = as(filter.condition(), And.class);
        as(and.left(), GreaterThan.class);

        InSubquery inSubquery = as(and.right(), InSubquery.class);
        assertEquals("y", as(inSubquery.value(), UnresolvedAttribute.class).name());

        Limit limit = as(inSubquery.subquery(), Limit.class);
        Keep keep = as(limit.child(), Keep.class);
        Eval eval = as(keep.child(), Eval.class);
        Filter subqueryFilter = as(eval.child(), Filter.class);
        as(subqueryFilter.condition(), GreaterThan.class);
        assertTSRelation(subqueryFilter.child(), tsSubqueryIndexPattern);

        validateRelation(filter.child(), fromContext, mainQueryIndexPattern);
    }

    /**
     * Nested IN TS subquery — an outer IN subquery whose own WHERE contains another IN TS subquery:
     * {@code FROM main | WHERE x IN (TS ts1 | WHERE y IN (TS ts2) | KEEP y)}.
     *
     * Filter[InSubquery[?x, Keep[[?y]]]]
     * \_UnresolvedRelation[main]
     *
     * Where the outer IN subquery's inner plan is Keep -&gt; Filter[InSubquery[?y, TS ts2]] -&gt; TS ts1.
     */
    public void testWhereInSubqueryWithNestedInTSSubquery() {
        requireWhereInSubquery();
        boolean fromContext = randomBoolean();
        var mainQueryIndexPattern = randomIndexPatterns();
        var tsSubqueryIndexPattern1 = randomIndexPatterns();
        var tsSubqueryIndexPattern2 = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            {} {}
            | WHERE x IN (TS {} | WHERE y IN (TS {}) | KEEP y)
            """, fromOrTS(fromContext), mainQueryIndexPattern, tsSubqueryIndexPattern1, tsSubqueryIndexPattern2);

        LogicalPlan plan = query(query);
        Filter outerFilter = as(plan, Filter.class);
        InSubquery outerIn = as(outerFilter.condition(), InSubquery.class);
        Keep keep = as(outerIn.subquery(), Keep.class);
        Filter innerFilter = as(keep.child(), Filter.class);
        InSubquery innerIn = as(innerFilter.condition(), InSubquery.class);
        assertEquals("y", as(innerIn.value(), UnresolvedAttribute.class).name());
        assertTSRelation(innerIn.subquery(), tsSubqueryIndexPattern2);
        assertTSRelation(innerFilter.child(), tsSubqueryIndexPattern1);
        validateRelation(outerFilter.child(), fromContext, mainQueryIndexPattern);
    }

    // mixed subquery in where command and where in subquery

    /**
     * IN subquery whose FROM has a sibling TS subquery — the FROM-subquery becomes a UnionAll of an
     * index pattern and a {@link Subquery} wrapping the TS {@link UnresolvedRelation}:
     * {@code FROM main | WHERE x (NOT)? IN (FROM sub, (TS ts_idx))}.
     *
     * Filter[(NOT) InSubquery[?x, UnionAll[[]]]]
     * \_UnresolvedRelation[main]
     *
     * Where the IN subquery's UnionAll has two children: UnresolvedRelation[sub] and
     * Subquery[UnresolvedRelation[, TIME_SERIES]].
     */
    public void testWhereInSubqueryWithTSInsideFromSubquery() {
        requireWhereInSubquery();
        requireSubqueryInFromCommand();
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        boolean fromContext = randomBoolean();
        var mainQueryIndexPattern = randomIndexPatterns();
        var fromSubqueryIndexPattern = randomIndexPatterns();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            {} {}
            | WHERE x {}IN (FROM {}, (TS {}))
            """, fromOrTS(fromContext), mainQueryIndexPattern, notClause, fromSubqueryIndexPattern, tsSubqueryIndexPattern);

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery;
        if (negated) {
            Not not = as(filter.condition(), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(filter.condition(), InSubquery.class);
        }
        assertEquals("x", as(inSubquery.value(), UnresolvedAttribute.class).name());

        UnionAll unionAll = as(inSubquery.subquery(), UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());
        // branch 1
        assertStandardRelation(children.get(0), fromSubqueryIndexPattern);
        // branch 2
        Subquery tsSubquery = as(children.get(1), Subquery.class);
        assertTSRelation(tsSubquery.plan(), tsSubqueryIndexPattern);
        // main
        validateRelation(filter.child(), fromContext, mainQueryIndexPattern);
    }

    /**
     * Same shape as {@link #testWhereInSubqueryWithTSInsideFromSubquery()} but with processing commands
     * stacked on both the inner TS subquery and on the IN-subquery's FROM source:
     * {@code FROM main | WHERE x IN (FROM sub, (TS ts_idx | WHERE a > 0 | EVAL b = a + 1) | KEEP b)}.
     *
     * Filter[InSubquery[?x, Keep[[?b]]]]
     * \_UnresolvedRelation[main]
     *
     * Where the IN subquery's inner plan is Keep -&gt; UnionAll{ UnresolvedRelation[sub],
     * Subquery[Eval -&gt; Filter -&gt; TS UnresolvedRelation] }.
     */
    public void testWhereInSubqueryWithTSAndProcessingCommandsInsideFromSubquery() {
        requireWhereInSubquery();
        requireSubqueryInFromCommand();
        boolean fromContext = randomBoolean();
        var mainQueryIndexPattern = randomIndexPatterns();
        var fromSubqueryIndexPattern = randomIndexPatterns();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            {} {}
            | WHERE x IN (FROM {},
                          (TS {} | WHERE a > 0 | EVAL b = a + 1)
                          | KEEP b)
            """, fromOrTS(fromContext), mainQueryIndexPattern, fromSubqueryIndexPattern, tsSubqueryIndexPattern);

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), UnresolvedAttribute.class).name());

        Keep keep = as(inSubquery.subquery(), Keep.class);
        UnionAll unionAll = as(keep.child(), UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        assertStandardRelation(children.get(0), fromSubqueryIndexPattern);

        Subquery tsSubquery = as(children.get(1), Subquery.class);
        Eval eval = as(tsSubquery.plan(), Eval.class);
        Filter subqueryFilter = as(eval.child(), Filter.class);
        GreaterThan gt = as(subqueryFilter.condition(), GreaterThan.class);
        assertEquals("a", as(gt.left(), Attribute.class).name());
        assertTSRelation(subqueryFilter.child(), tsSubqueryIndexPattern);

        validateRelation(filter.child(), fromContext, mainQueryIndexPattern);
    }

    /**
     * IN subquery whose FROM only wraps a single TS subquery — the {@code UnionAll} collapses and the
     * TS {@link UnresolvedRelation} is hung directly under the {@link InSubquery}, mirroring
     * {@link #testTSSubqueryOnly()}:
     * {@code FROM main | WHERE x IN (FROM (TS ts_idx))}.
     *
     * Filter[InSubquery[?x, UnresolvedRelation[, TIME_SERIES]]]
     * \_UnresolvedRelation[main]
     */
    public void testWhereInSubqueryWithSingleTSSubquery() {
        requireWhereInSubquery();
        requireSubqueryInFromCommand();
        boolean fromContext = randomBoolean();
        var mainQueryIndexPattern = randomIndexPatterns();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            {} {}
            | WHERE x IN (FROM (TS {}))
            """, fromOrTS(fromContext), mainQueryIndexPattern, tsSubqueryIndexPattern);

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), UnresolvedAttribute.class).name());
        assertTSRelation(inSubquery.subquery(), tsSubqueryIndexPattern);
        validateRelation(filter.child(), fromContext, mainQueryIndexPattern);
    }

    /**
     * IN subquery whose FROM stitches together two TS subqueries — produces a {@code UnionAll} of
     * {@link Subquery}-wrapped TS {@link UnresolvedRelation}s (no index pattern):
     * {@code FROM main | WHERE x IN (FROM (TS ts1), (TS ts2))}.
     *
     * Filter[InSubquery[?x, UnionAll[[]]]]
     * \_UnresolvedRelation[main]
     *
     * Where the UnionAll has two children: Subquery[TS UnresolvedRelation] and
     * Subquery[TS UnresolvedRelation].
     */
    public void testWhereInSubqueryWithMultipleTSSubqueries() {
        requireWhereInSubquery();
        requireSubqueryInFromCommand();
        boolean fromContext = randomBoolean();
        var mainQueryIndexPattern = randomIndexPatterns();
        var tsSubqueryIndexPattern1 = randomIndexPatterns();
        var tsSubqueryIndexPattern2 = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            {} {}
            | WHERE x IN (FROM (TS {}), (TS {}))
            """, fromOrTS(fromContext), mainQueryIndexPattern, tsSubqueryIndexPattern1, tsSubqueryIndexPattern2);

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), UnresolvedAttribute.class).name());

        UnionAll unionAll = as(inSubquery.subquery(), UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());
        // branch 1
        Subquery first = as(children.get(0), Subquery.class);
        assertTSRelation(first.plan(), tsSubqueryIndexPattern1);
        // branch 2
        Subquery second = as(children.get(1), Subquery.class);
        assertTSRelation(second.plan(), tsSubqueryIndexPattern2);
        // main
        validateRelation(filter.child(), fromContext, mainQueryIndexPattern);
    }

    /**
     * The outer FROM exposes a TS subquery as one of its branches, and that branch internally references
     * an IN subquery — i.e. TS-source subquery on the outside hosts the WHERE IN subquery:
     * {@code FROM main, (TS ts_idx | WHERE x IN (FROM sub))}.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[main]
     * \_Subquery[]
     *   \_Filter[InSubquery[?x, UnresolvedRelation[sub]]]
     *     \_UnresolvedRelation[, TIME_SERIES]
     */
    public void testFromSubqueryWithWhereInFromSubquery() {
        requireWhereInSubquery();
        requireSubqueryInFromCommand();
        boolean fromContext = randomBoolean();
        var mainQueryIndexPattern = randomIndexPatterns();
        var subqueryIndexPattern = randomIndexPatterns();
        var fromSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, ({} {} | WHERE x IN (FROM {}))
            """, mainQueryIndexPattern, fromOrTS(fromContext), subqueryIndexPattern, fromSubqueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());
        // branch 1
        assertStandardRelation(children.get(0), mainQueryIndexPattern);
        // branch 2
        Subquery tsSubquery = as(children.get(1), Subquery.class);
        Filter filter = as(tsSubquery.plan(), Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), UnresolvedAttribute.class).name());
        assertStandardRelation(inSubquery.subquery(), fromSubqueryIndexPattern);
        validateRelation(filter.child(), fromContext, subqueryIndexPattern);
    }

    /**
     * The outer FROM exposes a FROM-subquery branch which uses an IN-subquery whose source is TS —
     * combines all three constructs: outer FROM-subquery, WHERE IN subquery, and TS as the IN source:
     * {@code FROM main, (FROM sub | WHERE x IN (TS ts_idx))}.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[main]
     * \_Subquery[]
     *   \_Filter[InSubquery[?x, UnresolvedRelation[, TIME_SERIES]]]
     *     \_UnresolvedRelation[sub]
     */
    public void testFromSubqueryWithWhereInTSSubquery() {
        requireWhereInSubquery();
        requireSubqueryInFromCommand();
        boolean fromContext = randomBoolean();
        var mainQueryIndexPattern = randomIndexPatterns();
        var subqueryIndexPattern = randomIndexPatterns();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, ({} {} | WHERE x IN (TS {}))
            """, mainQueryIndexPattern, fromOrTS(fromContext), subqueryIndexPattern, tsSubqueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());
        // branch 1
        assertStandardRelation(children.get(0), mainQueryIndexPattern);
        // branch 2
        Subquery fromSubquery = as(children.get(1), Subquery.class);
        Filter filter = as(fromSubquery.plan(), Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), UnresolvedAttribute.class).name());
        assertTSRelation(inSubquery.subquery(), tsSubqueryIndexPattern);
        validateRelation(filter.child(), fromContext, subqueryIndexPattern);
    }

    private static void validateRelation(LogicalPlan plan, boolean standardMode, String indexPattern) {
        UnresolvedRelation relation = as(plan, UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(indexPattern), relation.indexPattern().indexPattern());
        assertEquals(standardMode ? IndexMode.STANDARD : IndexMode.TIME_SERIES, relation.indexMode());
    }

    private static void assertTSRelation(LogicalPlan plan, String tsIndexPattern) {
        UnresolvedRelation relation = as(plan, UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(tsIndexPattern), relation.indexPattern().indexPattern());
        assertEquals(IndexMode.TIME_SERIES, relation.indexMode());
    }

    private static void assertStandardRelation(LogicalPlan plan, String standardIndexPattern) {
        UnresolvedRelation relation = as(plan, UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(standardIndexPattern), relation.indexPattern().indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());
    }

    private static void assertRowField(LogicalPlan plan, String aliasName, int aliasValue) {
        Row row = as(plan, Row.class);
        assertEquals(1, row.fields().size());
        Alias alias = row.fields().get(0);
        assertEquals(aliasName, alias.name());
        Literal literal = as(alias.child(), Literal.class);
        assertEquals(aliasValue, literal.value());
    }

    private static String fromOrTS(boolean fromContext) {
        return fromContext ? "FROM" : "TS";
    }

    // mixed FROM, ROW and TS subqueries

    /**
     * An index pattern alongside a TS subquery and a ROW subquery. Each subquery branch keeps its
     * own source-command flavour while sharing the outer {@link UnionAll}.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * |_Subquery[]
     * | \_UnresolvedRelation[, TIME_SERIES]
     * \_Subquery[]
     *   \_Row[[1[INTEGER] AS x]]
     */
    public void testIndexPatternWithTSAndRowSubqueries() {
        requireSubqueryInFromCommand();
        requireSubqueryWithRow();
        var mainQueryIndexPattern = randomIndexPatterns();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (TS {}), (ROW x = 1)
            """, mainQueryIndexPattern, tsSubqueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(3, children.size());
        // branch 1
        assertStandardRelation(children.get(0), mainQueryIndexPattern);
        // branch 2
        Subquery tsSubquery = as(children.get(1), Subquery.class);
        assertTSRelation(tsSubquery.plan(), tsSubqueryIndexPattern);
        // branch 3
        Subquery rowSubquery = as(children.get(2), Subquery.class);
        assertRowField(rowSubquery.plan(), "x", 1);
    }

    /**
     * Mix of all three subquery flavours alongside a main index pattern. Each branch carries its
     * own trailing {@code WHERE} to verify processing commands are kept independently per branch.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * |_Subquery[]
     * | \_Filter[?x &gt; 1[INTEGER]]
     * |   \_UnresolvedRelation[, TIME_SERIES]
     * |_Subquery[]
     * | \_Filter[?x &gt; 1[INTEGER]]
     * |   \_UnresolvedRelation[]
     * \_Subquery[]
     *   \_Filter[?x &gt; 0[INTEGER]]
     *     \_Row[[1[INTEGER] AS x]]
     */
    public void testIndexPatternWithTSFromAndRowSubqueries() {
        requireSubqueryInFromCommand();
        requireSubqueryWithRow();
        var mainQueryIndexPattern = randomIndexPatterns();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        var fromSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (TS {} | WHERE x > 1), (FROM {} | WHERE x > 1), (ROW x = 1 | WHERE x > 0)
            """, mainQueryIndexPattern, tsSubqueryIndexPattern, fromSubqueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(4, children.size());
        // branch 1
        assertStandardRelation(children.get(0), mainQueryIndexPattern);
        // branch 2: TS subquery
        Subquery tsSubquery = as(children.get(1), Subquery.class);
        Filter tsFilter = as(tsSubquery.plan(), Filter.class);
        assertEquals("x", as(as(tsFilter.condition(), GreaterThan.class).left(), Attribute.class).name());
        assertTSRelation(tsFilter.child(), tsSubqueryIndexPattern);
        // branch 3: FROM subquery
        Subquery fromSubquery = as(children.get(2), Subquery.class);
        Filter fromFilter = as(fromSubquery.plan(), Filter.class);
        assertStandardRelation(fromFilter.child(), fromSubqueryIndexPattern);
        // branch 4: ROW subquery
        Subquery rowSubquery = as(children.get(3), Subquery.class);
        Filter rowFilter = as(rowSubquery.plan(), Filter.class);
        assertEquals("x", as(as(rowFilter.condition(), GreaterThan.class).left(), Attribute.class).name());
        assertRowField(rowFilter.child(), "x", 1);
    }

    /**
     * Mix of all three subquery flavours with no main index pattern. The outer {@link UnionAll}
     * has three {@link Subquery} children — one per source command.
     *
     * UnionAll[[]]
     * |_Subquery[]
     * | \_UnresolvedRelation[, TIME_SERIES]
     * |_Subquery[]
     * | \_Row[[1[INTEGER] AS x]]
     * \_Subquery[]
     *   \_UnresolvedRelation[]
     */
    public void testTSRowAndFromSubqueriesOnly() {
        requireSubqueryInFromCommand();
        requireSubqueryWithRow();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        var fromSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM (TS {}), (ROW x = 1), (FROM {})
            """, tsSubqueryIndexPattern, fromSubqueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(3, children.size());
        // branch 1: TS subquery
        Subquery tsSubquery = as(children.get(0), Subquery.class);
        assertTSRelation(tsSubquery.plan(), tsSubqueryIndexPattern);
        // branch 2: ROW subquery
        Subquery rowSubquery = as(children.get(1), Subquery.class);
        assertRowField(rowSubquery.plan(), "x", 1);
        // branch 3: FROM subquery
        Subquery fromSubquery = as(children.get(2), Subquery.class);
        assertStandardRelation(fromSubquery.plan(), fromSubqueryIndexPattern);
    }

    /**
     * A TS subquery and a ROW subquery both nested inside an outer FROM subquery. The outer
     * {@link UnionAll} has the main index pattern and a {@link Subquery} wrapping an inner
     * {@link UnionAll} that holds the inner FROM source plus the TS and ROW subqueries.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_UnionAll[[]]
     *     |_UnresolvedRelation[]
     *     |_Subquery[]
     *     | \_UnresolvedRelation[, TIME_SERIES]
     *     \_Subquery[]
     *       \_Row[[1[INTEGER] AS x]]
     */
    public void testTSAndRowSubqueriesNestedInsideFromSubquery() {
        requireSubqueryInFromCommand();
        requireSubqueryWithRow();
        var outerIndexPattern = randomIndexPatterns();
        var innerIndexPattern = randomIndexPatterns();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (FROM {}, (TS {}), (ROW x = 1))
            """, outerIndexPattern, innerIndexPattern, tsSubqueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll outerUnion = as(plan, UnionAll.class);
        List<LogicalPlan> outerChildren = outerUnion.children();
        assertEquals(2, outerChildren.size());
        // outer branch 1
        assertStandardRelation(outerChildren.get(0), outerIndexPattern);
        // outer branch 2: FROM subquery wrapping the inner UnionAll
        Subquery outerSubquery = as(outerChildren.get(1), Subquery.class);
        UnionAll innerUnion = as(outerSubquery.plan(), UnionAll.class);
        List<LogicalPlan> innerChildren = innerUnion.children();
        assertEquals(3, innerChildren.size());
        // inner branch 1: index pattern
        assertStandardRelation(innerChildren.get(0), innerIndexPattern);
        // inner branch 2: TS subquery
        Subquery innerTSSubquery = as(innerChildren.get(1), Subquery.class);
        assertTSRelation(innerTSSubquery.plan(), tsSubqueryIndexPattern);
        // inner branch 3: ROW subquery
        Subquery innerRowSubquery = as(innerChildren.get(2), Subquery.class);
        assertRowField(innerRowSubquery.plan(), "x", 1);
    }

    /**
     * IN-subquery whose FROM body mixes all three source flavours: an index pattern, a TS subquery
     * and a ROW subquery. The mix produces a {@link UnionAll} with the index pattern as the bare
     * leaf followed by a {@link Subquery}-wrapped TS {@link UnresolvedRelation} and a
     * {@link Subquery}-wrapped {@link Row}.
     *
     * Filter[InSubquery[?x, UnionAll[[]]]]
     * \_UnresolvedRelation[]
     *
     * Where the IN subquery's UnionAll has three children:
     * UnresolvedRelation[sub], Subquery[UnresolvedRelation[, TIME_SERIES]] and Subquery[Row[x=1]].
     */
    public void testWhereInSubqueryWithMixedTSRowAndFromSources() {
        requireWhereInSubquery();
        requireSubqueryInFromCommand();
        requireSubqueryWithRow();
        boolean fromContext = randomBoolean();
        var mainQueryIndexPattern = randomIndexPatterns();
        var fromSubqueryIndexPattern = randomIndexPatterns();
        var tsSubqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            {} {}
            | WHERE x IN (FROM {}, (TS {}), (ROW x = 1))
            """, fromOrTS(fromContext), mainQueryIndexPattern, fromSubqueryIndexPattern, tsSubqueryIndexPattern);

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), UnresolvedAttribute.class).name());

        UnionAll unionAll = as(inSubquery.subquery(), UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(3, children.size());
        // branch 1: index pattern
        assertStandardRelation(children.get(0), fromSubqueryIndexPattern);
        // branch 2: TS subquery
        Subquery tsSubquery = as(children.get(1), Subquery.class);
        assertTSRelation(tsSubquery.plan(), tsSubqueryIndexPattern);
        // branch 3: ROW subquery
        Subquery rowSubquery = as(children.get(2), Subquery.class);
        assertRowField(rowSubquery.plan(), "x", 1);
        // main
        validateRelation(filter.child(), fromContext, mainQueryIndexPattern);
    }

    // negative tests

    /**
     * The TS source command does not allow subqueries as source, regardless of whether the subquery uses FROM or TS.
     */
    public void testSubqueryWithinTimeSeriesCommand() {
        requireSubqueryInFromCommand();
        for (String query : List.of(
            "TS mainIndex, (FROM subIndex)",
            "TS mainIndex, (TS subIndex)",
            "FROM mainIndex | WHERE x IN (TS subIndex1, (FROM subIndex2))",
            "FROM mainIndex | WHERE x IN (TS subIndex1, (TS subIndex2))"
        )) {
            expectThrows(ParsingException.class, containsString("Subqueries are not supported in TS command"), () -> query(query));
        }
    }
}
