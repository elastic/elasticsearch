/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.ChangePoint;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;

public class InSubqueryTests extends AbstractStatementParserTests {

    @Before
    public void checkSubqueryInFromCommandSupport() {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY.isEnabled());
    }

    /**
     * Basic WHERE x IN (FROM subquery_index) test.
     *
     * Filter[InSubquery[?x, subquery_plan]]
     *   \_UnresolvedRelation[]
     */
    public void testWhereInSubqueryBasic() {
        String query = "FROM main_index | WHERE x IN (FROM sub_index)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);
        Attribute value = as(inSubquery.value(), Attribute.class);
        assertEquals("x", value.name());

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * WHERE x NOT IN (FROM subquery_index) test.
     *
     * Filter[Not[InSubquery[?x, subquery_plan]]]
     *   \_UnresolvedRelation[]
     */
    public void testWhereNotInSubquery() {
        String query = "FROM main_index | WHERE x NOT IN (FROM sub_index)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Not not = as(filter.condition(), Not.class);
        InSubquery inSubquery = as(not.field(), InSubquery.class);
        Attribute value = as(inSubquery.value(), Attribute.class);
        assertEquals("x", value.name());

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());
    }

    /**
     * (NOT) IN subquery with multiple processing commands in the subquery.
     */
    public void testWhereInSubqueryMultipleProcessingCommands() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT" : "";
        String query = LoggerMessageFormat.format(null, """
            FROM main_index
            | WHERE x {} IN (FROM sub_index
                          | WHERE a > 1
                          | EVAL b = a * 2
                          | FORK (WHERE c < 100) (WHERE d > 200)
                          | STATS cnt = COUNT(*) BY e
                          | INLINE STATS max_e = MAX(e) BY f
                          | DISSECT g "%{b} %{c}"
                          | GROK h "%{WORD:word} %{NUMBER:number}"
                          | SORT cnt desc
                          | LIMIT 10
                          | DROP i
                          | KEEP j
                          | RENAME k AS l
                          | MV_EXPAND m
                          | LOOKUP JOIN lookup_index ON n
                          | ENRICH clientip_policy ON client_ip WITH env
                          | CHANGE_POINT count ON @timestamp AS type, pvalue)
            """, notClause);

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery;
        if (negated) {
            Not not = as(filter.condition(), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(filter.condition(), InSubquery.class);
        }

        ChangePoint changePoint = as(inSubquery.subquery(), ChangePoint.class);
        Enrich enrich = as(changePoint.child(), Enrich.class);
        LookupJoin lookupJoin = as(enrich.child(), LookupJoin.class);
        UnresolvedRelation joinRelation = as(lookupJoin.right(), UnresolvedRelation.class);
        assertEquals("lookup_index", joinRelation.indexPattern().indexPattern());
        MvExpand mvExpand = as(lookupJoin.left(), MvExpand.class);
        Rename rename = as(mvExpand.child(), Rename.class);
        Keep keep = as(rename.child(), Keep.class);
        Drop drop = as(keep.child(), Drop.class);
        Limit limit = as(drop.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Grok grok = as(orderBy.child(), Grok.class);
        Dissect dissect = as(grok.child(), Dissect.class);
        InlineStats inlineStats = as(dissect.child(), InlineStats.class);
        Aggregate aggregate = as(inlineStats.child(), Aggregate.class);
        aggregate = as(aggregate.child(), Aggregate.class);
        Fork fork = as(aggregate.child(), Fork.class);
        assertEquals(2, fork.children().size());
        // Each fork branch wraps the preceding pipeline: Eval(fork) -> Filter(fork) -> Eval -> Filter -> UnresolvedRelation
        for (LogicalPlan branch : fork.children()) {
            Eval forkEval = as(branch, Eval.class);
            Filter forkFilter = as(forkEval.child(), Filter.class);
            Eval eval = as(forkFilter.child(), Eval.class);
            Filter subqueryFilter = as(eval.child(), Filter.class);
            UnresolvedRelation subqueryRelation = as(subqueryFilter.child(), UnresolvedRelation.class);
            assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());
        }
        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * WHERE (NOT) IN subquery ends with different modes to verify lexer mode transitions.
     */
    public void testWhereInSubqueryEndsWithDifferentModes() {
        List<String> processingCommands = List.of(
            "WHERE a > 10",
            "EVAL b = a * 2",
            "KEEP x",
            "DROP y",
            "SORT a",
            "LIMIT 10",
            "STATS cnt = COUNT(*) BY a",
            "RENAME a AS b",
            "MV_EXPAND m",
            "CHANGE_POINT a ON b",
            "ENRICH my_policy ON x",
            "FORK (WHERE a > 1)(WHERE a < 10)",
            "INLINE STATS cnt = COUNT(*) BY a",
            "LOOKUP JOIN lookup_index ON x"
        );
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT" : "";
        for (String processingCommand : processingCommands) {
            String query = LoggerMessageFormat.format(null, """
                FROM main_index | WHERE x {} IN (FROM sub_index | {})
                """, notClause, processingCommand);

            LogicalPlan plan = query(query);
            Filter filter = as(plan, Filter.class);
            InSubquery inSubquery;
            if (negated) {
                Not not = as(filter.condition(), Not.class);
                inSubquery = as(not.field(), InSubquery.class);
            } else {
                inSubquery = as(filter.condition(), InSubquery.class);
            }
            UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
            assertEquals("main_index", mainRelation.indexPattern().indexPattern());
        }
    }

    /**
     * WHERE IN subquery combined with other boolean expressions.
     *
     * Filter[And[GreaterThan[?a, 5], InSubquery[?x, ...]]]
     *   \_UnresolvedRelation[]
     */
    public void testWhereInSubqueryWithOtherConditions() {
        String query = "FROM main_index | WHERE a > 5 AND x IN (FROM sub_index | KEEP y)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        And and = as(filter.condition(), And.class);
        as(and.left(), GreaterThan.class);
        InSubquery inSubquery = as(and.right(), InSubquery.class);
        Attribute value = as(inSubquery.value(), Attribute.class);
        assertEquals("x", value.name());
    }

    /**
     * Existing value list IN still works after the grammar changes.
     */
    public void testWhereInValueListStillWorks() {
        String query = "FROM main_index | WHERE x IN (1, 2, 3)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        In in = as(filter.condition(), In.class);
        Attribute value = as(in.value(), Attribute.class);
        assertEquals("x", value.name());
        assertEquals(3, in.list().size());
    }

    /**
     * IN value-list with a mix of constants and field references:
     * {@code WHERE x IN (1, y, "hello", z)}
     */
    public void testWhereInListMixedConstantsAndFields() {
        String query = "FROM main_index | WHERE x IN (1, y, \"hello\", z)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        In in = as(filter.condition(), In.class);

        UnresolvedAttribute value = as(in.value(), UnresolvedAttribute.class);
        assertEquals("x", value.name());

        List<Expression> list = in.list();
        assertEquals(4, list.size());
        Literal lit1 = as(list.get(0), Literal.class);
        assertEquals(1, lit1.value());
        UnresolvedAttribute fieldY = as(list.get(1), UnresolvedAttribute.class);
        assertEquals("y", fieldY.name());
        Literal litHello = as(list.get(2), Literal.class);
        assertEquals(new BytesRef("hello"), litHello.value());
        UnresolvedAttribute fieldZ = as(list.get(3), UnresolvedAttribute.class);
        assertEquals("z", fieldZ.name());
    }

    /**
     * Multiple IN and/or NOT IN subqueries in the same WHERE clause, combined with AND or OR.
     */
    public void testMultipleInSubqueries() {
        boolean firstNegated = randomBoolean();
        boolean secondNegated = randomBoolean();
        boolean useAnd = randomBoolean();
        String first = firstNegated ? "NOT IN" : "IN";
        String second = secondNegated ? "NOT IN" : "IN";
        String op = useAnd ? "AND" : "OR";
        String query = LoggerMessageFormat.format(null, """
            FROM main_index
            | WHERE x {} (FROM sub1 | KEEP a) {} y {} (FROM sub2 | KEEP b)
            """, first, op, second);

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Expression condition = filter.condition();

        Expression left;
        Expression right;
        if (useAnd) {
            And and = as(condition, And.class);
            left = and.left();
            right = and.right();
        } else {
            Or or = as(condition, Or.class);
            left = or.left();
            right = or.right();
        }

        // Verify first IN/NOT IN subquery
        InSubquery firstIn;
        if (firstNegated) {
            firstIn = as(as(left, Not.class).field(), InSubquery.class);
        } else {
            firstIn = as(left, InSubquery.class);
        }
        Keep firstKeep = as(firstIn.subquery(), Keep.class);
        UnresolvedRelation firstRelation = as(firstKeep.child(), UnresolvedRelation.class);
        assertEquals("sub1", firstRelation.indexPattern().indexPattern());

        // Verify second IN/NOT IN subquery
        InSubquery secondIn;
        if (secondNegated) {
            secondIn = as(as(right, Not.class).field(), InSubquery.class);
        } else {
            secondIn = as(right, InSubquery.class);
        }
        Keep secondKeep = as(secondIn.subquery(), Keep.class);
        UnresolvedRelation secondRelation = as(secondKeep.child(), UnresolvedRelation.class);
        assertEquals("sub2", secondRelation.indexPattern().indexPattern());

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * Two IN predicates in the same WHERE clause, each randomly an IN value-list or IN subquery, with random NOT.
     */
    public void testWhereInSubqueryMixedWithInList() {
        boolean leftIsSubquery = randomBoolean();
        boolean rightIsSubquery = randomBoolean();
        boolean leftNegated = randomBoolean();
        boolean rightNegated = randomBoolean();

        String leftPart = "x " + (leftNegated ? "NOT " : "") + (leftIsSubquery ? "IN (FROM sub1 | KEEP a)" : "IN (1, 2, 3)");
        String rightPart = "y " + (rightNegated ? "NOT " : "") + (rightIsSubquery ? "IN (FROM sub2 | KEEP b)" : "IN (4, 5, 6)");
        String query = LoggerMessageFormat.format(null, """
            FROM main_index | WHERE {} AND {}
            """, leftPart, rightPart);

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        And and = as(filter.condition(), And.class);

        assertInPredicate(and.left(), leftNegated, leftIsSubquery, "sub1");
        assertInPredicate(and.right(), rightNegated, rightIsSubquery, "sub2");

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * IN subquery where the subquery's FROM command includes METADATA fields.
     */
    public void testWhereInSubqueryWithMetadata() {
        String query = """
            FROM main_index
            | WHERE x IN (FROM sub_index METADATA _id, _index | KEEP _id)
            """;

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);

        Keep keep = as(inSubquery.subquery(), Keep.class);
        UnresolvedRelation subRelation = as(keep.child(), UnresolvedRelation.class);
        assertEquals("sub_index", subRelation.indexPattern().indexPattern());
        List<String> metadataFieldNames = subRelation.metadataFields().stream().map(NamedExpression::name).toList();
        assertEquals(List.of("_id", "_index"), metadataFieldNames);
    }

    /**
     * IN subquery where the subquery's FROM command references a remote cluster index.
     */
    public void testWhereInSubqueryWithRemoteCluster() {
        String query = """
            FROM main_index
            | WHERE x IN (FROM remote_cluster:sub_index | KEEP a)
            """;

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);

        Keep keep = as(inSubquery.subquery(), Keep.class);
        UnresolvedRelation subRelation = as(keep.child(), UnresolvedRelation.class);
        assertEquals("remote_cluster:sub_index", subRelation.indexPattern().indexPattern());
    }

    /**
     * IN subquery whose FROM command contains a nested FROM-subquery:
     * {@code FROM main | WHERE x IN (FROM sub1, (FROM sub2) | KEEP a)}
     */
    public void testWhereInSubqueryWithNestedFromSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
            FROM main_index
            | WHERE x IN (FROM sub1, (FROM sub2) | KEEP a)
            """;

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);

        Keep keep = as(inSubquery.subquery(), Keep.class);
        UnionAll unionAll = as(keep.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());
        UnresolvedRelation sub1 = as(unionAll.children().get(0), UnresolvedRelation.class);
        assertEquals("sub1", sub1.indexPattern().indexPattern());
        Subquery nested = as(unionAll.children().get(1), Subquery.class);
        UnresolvedRelation sub2 = as(nested.child(), UnresolvedRelation.class);
        assertEquals("sub2", sub2.indexPattern().indexPattern());
    }

    /**
     * Nested IN subqueries: the inner subquery itself contains a WHERE IN subquery:
     * {@code FROM main | WHERE x IN (FROM sub1 | WHERE y IN (FROM sub2 | KEEP b) | KEEP a)}
     */
    public void testWhereInSubqueryWithNestedInSubquery() {
        String query = """
            FROM main_index
            | WHERE x IN (FROM sub1 | WHERE y IN (FROM sub2 | KEEP b) | KEEP a)
            """;

        LogicalPlan plan = query(query);
        Filter outerFilter = as(plan, Filter.class);
        InSubquery outerIn = as(outerFilter.condition(), InSubquery.class);

        Keep outerKeep = as(outerIn.subquery(), Keep.class);
        Filter innerFilter = as(outerKeep.child(), Filter.class);
        InSubquery innerIn = as(innerFilter.condition(), InSubquery.class);

        Keep innerKeep = as(innerIn.subquery(), Keep.class);
        UnresolvedRelation sub2 = as(innerKeep.child(), UnresolvedRelation.class);
        assertEquals("sub2", sub2.indexPattern().indexPattern());

        UnresolvedRelation sub1 = as(innerFilter.child(), UnresolvedRelation.class);
        assertEquals("sub1", sub1.indexPattern().indexPattern());

        UnresolvedRelation main = as(outerFilter.child(), UnresolvedRelation.class);
        assertEquals("main_index", main.indexPattern().indexPattern());
    }

    /**
     * FROM subquery where one branch contains a WHERE IN subquery:
     * {@code FROM main, (FROM sub1 | WHERE x IN (FROM sub2 | KEEP a) | KEEP x)}
     */
    public void testFromSubqueryWithWhereInSubqueryInside() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
            FROM main,
                 (FROM sub1 | WHERE x IN (FROM sub2 | KEEP a) | KEEP x)
            """;

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // main query
        UnresolvedRelation mainRelation = as(unionAll.children().get(0), UnresolvedRelation.class);
        assertEquals("main", mainRelation.indexPattern().indexPattern());

        // FROM subquery branch: Subquery -> Keep -> Filter(InSubquery) -> UnresolvedRelation
        Subquery subquery = as(unionAll.children().get(1), Subquery.class);
        Keep keep = as(subquery.plan(), Keep.class);
        Filter filter = as(keep.child(), Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);

        // the IN subquery's plan
        Keep innerKeep = as(inSubquery.subquery(), Keep.class);
        UnresolvedRelation sub2 = as(innerKeep.child(), UnresolvedRelation.class);
        assertEquals("sub2", sub2.indexPattern().indexPattern());

        // the FROM of the branch
        UnresolvedRelation sub1 = as(filter.child(), UnresolvedRelation.class);
        assertEquals("sub1", sub1.indexPattern().indexPattern());
    }

    // ---- WHERE IN subquery negative tests ----

    public void testWhereInSubqueryRejectsTsSourceCommand() {
        var e = expectThrows(ParsingException.class, () -> query("FROM main | WHERE x IN (TS sub_index)"));
        assertThat(e.getMessage(), containsString("no viable alternative at input 'x IN (TS'"));
    }

    public void testWhereInSubqueryRejectsRowSourceCommand() {
        var e = expectThrows(ParsingException.class, () -> query("FROM main | WHERE x IN (ROW a = 1)"));
        assertThat(e.getMessage(), containsString("no viable alternative at input 'x IN (ROW'"));
    }

    public void testWhereInSubqueryRejectsShowSourceCommand() {
        var e = expectThrows(ParsingException.class, () -> query("FROM main | WHERE x IN (SHOW INFO)"));
        assertThat(e.getMessage(), containsString("no viable alternative at input 'x IN (SHOW'"));
    }

    public void testWhereInSubqueryRejectsPromqlSourceCommand() {
        var e = expectThrows(ParsingException.class, () -> query("FROM main | WHERE x IN (PROMQL 'up')"));
        assertThat(e.getMessage(), containsString("no viable alternative at input 'x IN (PROMQL'"));
    }

    public void testWhereInSubqueryRejectsSubqueryWithTrailingTokens() {
        var e1 = expectThrows(ParsingException.class, () -> query("FROM main | WHERE x IN (FROM sub | KEEP a, 1)"));
        assertThat(e1.getMessage(), containsString("token recognition error at: '1'"));
        var e2 = expectThrows(ParsingException.class, () -> query("FROM main | WHERE x IN (FROM sub | KEEP a KEEP b)"));
        assertThat(e2.getMessage(), containsString("extraneous input 'KEEP' expecting {'|', ')'}"));
    }

    public void testWhereInSubqueryRejectsMissingClosingParen() {
        var e = expectThrows(ParsingException.class, () -> query("FROM main | WHERE x IN (FROM sub"));
        assertThat(e.getMessage(), containsString("mismatched input '<EOF>' expecting {'|', ')'}"));
    }

    public void testWhereInSubqueryRejectsEmptySubquery() {
        var e = expectThrows(ParsingException.class, () -> query("FROM main | WHERE x IN ()"));
        assertThat(e.getMessage(), containsString("no viable alternative at input 'x IN ()'"));
    }

    public void testWhereInSubqueryRejectsMultipleFromCommands() {
        var e = expectThrows(ParsingException.class, () -> query("FROM main | WHERE x IN (FROM sub1 | FROM sub2)"));
        assertThat(e.getMessage(), containsString("mismatched input 'FROM'"));
    }

    // ---- helpers ----

    private void assertInPredicate(Expression expr, boolean negated, boolean isSubquery, String expectedIndex) {
        Expression inner = negated ? as(expr, Not.class).field() : expr;
        if (isSubquery) {
            InSubquery inSubquery = as(inner, InSubquery.class);
            Keep keep = as(inSubquery.subquery(), Keep.class);
            UnresolvedRelation relation = as(keep.child(), UnresolvedRelation.class);
            assertEquals(expectedIndex, relation.indexPattern().indexPattern());
        } else {
            as(inner, In.class);
        }
    }
}
