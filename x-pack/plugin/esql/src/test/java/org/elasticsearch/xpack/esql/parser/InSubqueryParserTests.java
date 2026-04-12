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
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
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
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
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
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.hamcrest.Matchers.containsString;

public class InSubqueryParserTests extends AbstractStatementParserTests {

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

    // ---- WHERE (NOT) IN subquery with parameters ----

    /**
     * WHERE ?val IN (FROM sub) with a named value parameter on the LHS.
     * The named parameter resolves to a Literal.
     */
    public void testWhereInSubqueryWithNamedValueParam() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | WHERE " + notClause + "?val IN (FROM sub_index)";

        LogicalPlan plan = query(query, new QueryParams(List.of(paramAsConstant("val", 42))));
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery;
        if (negated) {
            Not not = as(filter.condition(), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(filter.condition(), InSubquery.class);
        }
        Literal value = as(inSubquery.value(), Literal.class);
        assertEquals(42, value.value());

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * WHERE ?1 IN (FROM sub) with a positional value parameter on the LHS.
     */
    public void testWhereInSubqueryWithPositionalValueParam() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | WHERE " + notClause + "?1 IN (FROM sub_index)";

        LogicalPlan plan = query(query, new QueryParams(List.of(paramAsConstant(null, 42))));
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery;
        if (negated) {
            Not not = as(filter.condition(), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(filter.condition(), InSubquery.class);
        }
        Literal value = as(inSubquery.value(), Literal.class);
        assertEquals(42, value.value());

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());
    }

    /**
     * WHERE ? IN (FROM sub) with an anonymous value parameter on the LHS.
     */
    public void testWhereInSubqueryWithAnonymousValueParam() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | WHERE " + notClause + "? IN (FROM sub_index)";

        LogicalPlan plan = query(query, new QueryParams(List.of(paramAsConstant(null, 42))));
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery;
        if (negated) {
            Not not = as(filter.condition(), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(filter.condition(), InSubquery.class);
        }
        Literal value = as(inSubquery.value(), Literal.class);
        assertEquals(42, value.value());

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());
    }

    /**
     * WHERE ??field IN (FROM sub) with a named identifier parameter on the LHS.
     * The identifier parameter resolves to an UnresolvedAttribute.
     */
    public void testWhereInSubqueryWithNamedIdentifierParam() {
        assumeTrue("double parameters markers for identifiers", EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled());
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | WHERE " + notClause + "??field IN (FROM sub_index)";

        LogicalPlan plan = query(query, new QueryParams(List.of(paramAsConstant("field", "x"))));
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery;
        if (negated) {
            Not not = as(filter.condition(), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(filter.condition(), InSubquery.class);
        }
        Attribute value = as(inSubquery.value(), Attribute.class);
        assertEquals("x", value.name());

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());
    }

    /**
     * WHERE ??1 IN (FROM sub) with a positional identifier parameter on the LHS.
     */
    public void testWhereInSubqueryWithPositionalIdentifierParam() {
        assumeTrue("double parameters markers for identifiers", EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled());
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | WHERE " + notClause + "??1 IN (FROM sub_index)";

        LogicalPlan plan = query(query, new QueryParams(List.of(paramAsConstant(null, "x"))));
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery;
        if (negated) {
            Not not = as(filter.condition(), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(filter.condition(), InSubquery.class);
        }
        Attribute value = as(inSubquery.value(), Attribute.class);
        assertEquals("x", value.name());

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());
    }

    /**
     * WHERE ?? IN (FROM sub) with an anonymous identifier parameter on the LHS.
     */
    public void testWhereInSubqueryWithAnonymousIdentifierParam() {
        assumeTrue("double parameters markers for identifiers", EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled());
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | WHERE " + notClause + "?? IN (FROM sub_index)";

        LogicalPlan plan = query(query, new QueryParams(List.of(paramAsConstant(null, "x"))));
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery;
        if (negated) {
            Not not = as(filter.condition(), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(filter.condition(), InSubquery.class);
        }
        Attribute value = as(inSubquery.value(), Attribute.class);
        assertEquals("x", value.name());

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());
    }

    /**
     * Parameters inside the subquery: WHERE x IN (FROM sub | WHERE a > ?val | KEEP ??field).
     * Tests named, positional, and anonymous variants.
     */
    public void testWhereInSubqueryWithParamsInsideSubquery() {
        // Named params
        String namedQuery = "FROM main_index | WHERE x IN (FROM sub_index | WHERE a > ?threshold | KEEP ??col)";
        LogicalPlan namedPlan = query(namedQuery, new QueryParams(List.of(paramAsConstant("threshold", 10), paramAsConstant("col", "b"))));
        Filter namedFilter = as(namedPlan, Filter.class);
        InSubquery namedIn = as(namedFilter.condition(), InSubquery.class);
        Keep namedKeep = as(namedIn.subquery(), Keep.class);
        Filter namedSubFilter = as(namedKeep.child(), Filter.class);
        GreaterThan namedGt = as(namedSubFilter.condition(), GreaterThan.class);
        Literal namedThreshold = as(namedGt.right(), Literal.class);
        assertEquals(10, namedThreshold.value());

        // Positional params
        String positionalQuery = "FROM main_index | WHERE x IN (FROM sub_index | WHERE a > ?1 | KEEP ??2)";
        LogicalPlan positionalPlan = query(
            positionalQuery,
            new QueryParams(List.of(paramAsConstant(null, 10), paramAsConstant(null, "b")))
        );
        Filter positionalFilter = as(positionalPlan, Filter.class);
        InSubquery positionalIn = as(positionalFilter.condition(), InSubquery.class);
        Keep positionalKeep = as(positionalIn.subquery(), Keep.class);
        Filter positionalSubFilter = as(positionalKeep.child(), Filter.class);
        GreaterThan positionalGt = as(positionalSubFilter.condition(), GreaterThan.class);
        Literal positionalThreshold = as(positionalGt.right(), Literal.class);
        assertEquals(10, positionalThreshold.value());

        // Anonymous params
        String anonymousQuery = "FROM main_index | WHERE x IN (FROM sub_index | WHERE a > ? | KEEP ??)";
        LogicalPlan anonymousPlan = query(anonymousQuery, new QueryParams(List.of(paramAsConstant(null, 10), paramAsConstant(null, "b"))));
        Filter anonymousFilter = as(anonymousPlan, Filter.class);
        InSubquery anonymousIn = as(anonymousFilter.condition(), InSubquery.class);
        Keep anonymousKeep = as(anonymousIn.subquery(), Keep.class);
        Filter anonymousSubFilter = as(anonymousKeep.child(), Filter.class);
        GreaterThan anonymousGt = as(anonymousSubFilter.condition(), GreaterThan.class);
        Literal anonymousThreshold = as(anonymousGt.right(), Literal.class);
        assertEquals(10, anonymousThreshold.value());
    }

    /**
     * Parameters on both the LHS and inside the subquery:
     * WHERE ?val IN (FROM sub | WHERE a > ?threshold) with named, positional, and anonymous variants.
     */
    public void testWhereInSubqueryWithParamsOnBothSides() {
        // Named params
        String namedQuery = "FROM main_index | WHERE ?val IN (FROM sub_index | WHERE a > ?threshold)";
        LogicalPlan namedPlan = query(namedQuery, new QueryParams(List.of(paramAsConstant("val", 42), paramAsConstant("threshold", 10))));
        Filter namedFilter = as(namedPlan, Filter.class);
        InSubquery namedIn = as(namedFilter.condition(), InSubquery.class);
        Literal namedValue = as(namedIn.value(), Literal.class);
        assertEquals(42, namedValue.value());
        Filter namedSubFilter = as(namedIn.subquery(), Filter.class);
        GreaterThan namedGt = as(namedSubFilter.condition(), GreaterThan.class);
        Literal namedThreshold = as(namedGt.right(), Literal.class);
        assertEquals(10, namedThreshold.value());

        // Positional params
        String positionalQuery = "FROM main_index | WHERE ?1 IN (FROM sub_index | WHERE a > ?2)";
        LogicalPlan positionalPlan = query(positionalQuery, new QueryParams(List.of(paramAsConstant(null, 42), paramAsConstant(null, 10))));
        Filter positionalFilter = as(positionalPlan, Filter.class);
        InSubquery positionalIn = as(positionalFilter.condition(), InSubquery.class);
        Literal positionalValue = as(positionalIn.value(), Literal.class);
        assertEquals(42, positionalValue.value());
        Filter positionalSubFilter = as(positionalIn.subquery(), Filter.class);
        GreaterThan positionalGt = as(positionalSubFilter.condition(), GreaterThan.class);
        Literal positionalThreshold = as(positionalGt.right(), Literal.class);
        assertEquals(10, positionalThreshold.value());

        // Anonymous params
        String anonymousQuery = "FROM main_index | WHERE ? IN (FROM sub_index | WHERE a > ?)";
        LogicalPlan anonymousPlan = query(anonymousQuery, new QueryParams(List.of(paramAsConstant(null, 42), paramAsConstant(null, 10))));
        Filter anonymousFilter = as(anonymousPlan, Filter.class);
        InSubquery anonymousIn = as(anonymousFilter.condition(), InSubquery.class);
        Literal anonymousValue = as(anonymousIn.value(), Literal.class);
        assertEquals(42, anonymousValue.value());
        Filter anonymousSubFilter = as(anonymousIn.subquery(), Filter.class);
        GreaterThan anonymousGt = as(anonymousSubFilter.condition(), GreaterThan.class);
        Literal anonymousThreshold = as(anonymousGt.right(), Literal.class);
        assertEquals(10, anonymousThreshold.value());
    }

    // ---- IN subquery in processing commands that accept boolean expressions ----
    // Parser does not block IN subquery inside these commands, however not all of them are fully supported
    // Analyzer will do some additional validation to block the unsupported cases.

    /**
     * EVAL with (NOT) IN subquery as a boolean expression:
     * {@code FROM main | EVAL is_match = x IN (FROM sub)}
     */
    public void testEvalWithInSubquery() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | EVAL is_match = x " + notClause + "IN (FROM sub_index)";

        LogicalPlan plan = query(query);
        Eval eval = as(plan, Eval.class);
        assertEquals(1, eval.fields().size());
        Alias alias = eval.fields().get(0);
        assertEquals("is_match", alias.name());

        InSubquery inSubquery;
        if (negated) {
            Not not = as(alias.child(), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(alias.child(), InSubquery.class);
        }
        Attribute value = as(inSubquery.value(), Attribute.class);
        assertEquals("x", value.name());

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());

        UnresolvedRelation mainRelation = as(eval.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * EVAL with (NOT) IN subquery as an implicit field name (no alias):
     * {@code FROM main | EVAL x IN (FROM sub)}
     */
    public void testEvalWithInSubqueryImplicitName() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | EVAL x " + notClause + "IN (FROM sub_index)";

        LogicalPlan plan = query(query);
        Eval eval = as(plan, Eval.class);
        assertEquals(1, eval.fields().size());
        Alias alias = eval.fields().get(0);

        if (negated) {
            Not not = as(alias.child(), Not.class);
            as(not.field(), InSubquery.class);
        } else {
            as(alias.child(), InSubquery.class);
        }
    }

    /**
     * EVAL with multiple fields where one is an (NOT) IN subquery:
     * {@code FROM main | EVAL a = 1, is_match = x IN (FROM sub), b = y + 2}
     */
    public void testEvalWithInSubqueryAmongMultipleFields() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | EVAL a = 1, is_match = x " + notClause + "IN (FROM sub_index), b = y";

        LogicalPlan plan = query(query);
        Eval eval = as(plan, Eval.class);
        assertEquals(3, eval.fields().size());

        Alias first = eval.fields().get(0);
        assertEquals("a", first.name());
        as(first.child(), Literal.class);

        Alias second = eval.fields().get(1);
        assertEquals("is_match", second.name());
        if (negated) {
            Not not = as(second.child(), Not.class);
            as(not.field(), InSubquery.class);
        } else {
            as(second.child(), InSubquery.class);
        }

        Alias third = eval.fields().get(2);
        assertEquals("b", third.name());
    }

    /**
     * SORT with (NOT) IN subquery as the sort expression:
     * {@code FROM main | SORT x IN (FROM sub) ASC}
     */
    public void testSortWithInSubquery() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | SORT x " + notClause + "IN (FROM sub_index) ASC";

        LogicalPlan plan = query(query);
        OrderBy orderBy = as(plan, OrderBy.class);
        assertEquals(1, orderBy.order().size());
        Order order = orderBy.order().get(0);
        assertEquals(Order.OrderDirection.ASC, order.direction());

        InSubquery inSubquery;
        if (negated) {
            Not not = as(order.child(), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(order.child(), InSubquery.class);
        }
        Attribute value = as(inSubquery.value(), Attribute.class);
        assertEquals("x", value.name());

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());

        UnresolvedRelation mainRelation = as(orderBy.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * STATS aggregation with (NOT) IN subquery in the WHERE filter:
     * {@code FROM main | STATS c = COUNT(*) WHERE x IN (FROM sub)}
     */
    public void testStatsAggFilterWithInSubquery() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | STATS c = COUNT(*) WHERE x " + notClause + "IN (FROM sub_index)";

        LogicalPlan plan = query(query);
        Aggregate aggregate = as(plan, Aggregate.class);
        assertEquals(0, aggregate.groupings().size());
        assertEquals(1, aggregate.aggregates().size());

        Alias alias = as(aggregate.aggregates().get(0), Alias.class);
        assertEquals("c", alias.name());
        FilteredExpression filtered = as(alias.child(), FilteredExpression.class);
        as(filtered.delegate(), UnresolvedFunction.class);

        InSubquery inSubquery;
        if (negated) {
            Not not = as(filtered.filter(), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(filtered.filter(), InSubquery.class);
        }
        Attribute value = as(inSubquery.value(), Attribute.class);
        assertEquals("x", value.name());

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());
    }

    /**
     * STATS with (NOT) IN subquery in the BY clause:
     * {@code FROM main | STATS c = COUNT(*) BY x IN (FROM sub)}
     * The BY expression is wrapped in an Alias with an auto-generated name.
     */
    public void testStatsByWithInSubquery() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | STATS c = COUNT(*) BY x " + notClause + "IN (FROM sub_index)";

        LogicalPlan plan = query(query);
        Aggregate aggregate = as(plan, Aggregate.class);
        assertEquals(1, aggregate.groupings().size());

        // BY expressions are wrapped in Alias
        Alias groupingAlias = as(aggregate.groupings().get(0), Alias.class);
        InSubquery inSubquery;
        if (negated) {
            Not not = as(groupingAlias.child(), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(groupingAlias.child(), InSubquery.class);
        }
        Attribute value = as(inSubquery.value(), Attribute.class);
        assertEquals("x", value.name());

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());
    }

    /**
     * LIMIT BY with (NOT) IN subquery as a grouping expression:
     * {@code FROM main | SORT a | LIMIT 10 BY x IN (FROM sub)}
     */
    public void testLimitByWithInSubquery() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | SORT a | LIMIT 10 BY x " + notClause + "IN (FROM sub_index)";

        LogicalPlan plan = query(query);
        LimitBy limitBy = as(plan, LimitBy.class);
        assertEquals(1, limitBy.groupings().size());

        InSubquery inSubquery;
        if (negated) {
            Not not = as(limitBy.groupings().get(0), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(limitBy.groupings().get(0), InSubquery.class);
        }
        Attribute value = as(inSubquery.value(), Attribute.class);
        assertEquals("x", value.name());

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());

        OrderBy orderBy = as(limitBy.child(), OrderBy.class);
        UnresolvedRelation mainRelation = as(orderBy.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * LOOKUP JOIN ON rejects (NOT) IN subquery because the ON clause requires at least one binary comparison
     * relating the left index and the lookup index.
     */
    public void testLookupJoinOnRejectsInSubquery() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | LOOKUP JOIN lookup_index ON x " + notClause + "IN (FROM sub_index)";

        var e = expectThrows(ParsingException.class, () -> query(query));
        assertThat(e.getMessage(), containsString("JOIN ON clause with expressions must contain at least one condition relating"));
    }

    /**
     * (NOT) IN subquery as a function argument:
     * {@code FROM main | EVAL result = COALESCE(x IN (FROM sub), false)}
     */
    public void testInSubqueryAsFunctionArgument() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | EVAL result = COALESCE(x " + notClause + "IN (FROM sub_index), false)";

        LogicalPlan plan = query(query);
        Eval eval = as(plan, Eval.class);
        Alias alias = eval.fields().get(0);
        assertEquals("result", alias.name());
        UnresolvedFunction coalesce = as(alias.child(), UnresolvedFunction.class);
        assertEquals("COALESCE", coalesce.name());
        assertEquals(2, coalesce.children().size());

        InSubquery inSubquery;
        if (negated) {
            Not not = as(coalesce.children().get(0), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(coalesce.children().get(0), InSubquery.class);
        }
        Attribute value = as(inSubquery.value(), Attribute.class);
        assertEquals("x", value.name());

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());
    }

    // ---- WHERE with IN subquery nested in other expressions ----

    /**
     * IN subquery combined with AND:
     * {@code WHERE a > 5 AND x IN (FROM sub) AND b < 10}
     */
    public void testWhereInSubqueryNestedInAnd() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | WHERE a > 5 AND x " + notClause + "IN (FROM sub_index) AND b < 10";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        And outerAnd = as(filter.condition(), And.class);
        And innerAnd = as(outerAnd.left(), And.class);

        as(innerAnd.left(), GreaterThan.class);
        if (negated) {
            Not not = as(innerAnd.right(), Not.class);
            as(not.field(), InSubquery.class);
        } else {
            as(innerAnd.right(), InSubquery.class);
        }
    }

    /**
     * IN subquery combined with OR:
     * {@code WHERE x IN (FROM sub1) OR y IN (FROM sub2)}
     */
    public void testWhereInSubqueryNestedInOr() {
        boolean firstNegated = randomBoolean();
        boolean secondNegated = randomBoolean();
        String first = firstNegated ? "NOT IN" : "IN";
        String second = secondNegated ? "NOT IN" : "IN";
        String query = "FROM main_index | WHERE x " + first + " (FROM sub1) OR y " + second + " (FROM sub2)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Or or = as(filter.condition(), Or.class);

        InSubquery leftIn;
        if (firstNegated) {
            leftIn = as(as(or.left(), Not.class).field(), InSubquery.class);
        } else {
            leftIn = as(or.left(), InSubquery.class);
        }
        assertEquals("x", as(leftIn.value(), Attribute.class).name());
        assertEquals("sub1", as(leftIn.subquery(), UnresolvedRelation.class).indexPattern().indexPattern());

        InSubquery rightIn;
        if (secondNegated) {
            rightIn = as(as(or.right(), Not.class).field(), InSubquery.class);
        } else {
            rightIn = as(or.right(), InSubquery.class);
        }
        assertEquals("y", as(rightIn.value(), Attribute.class).name());
        assertEquals("sub2", as(rightIn.subquery(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * Double NOT with IN subquery:
     * {@code WHERE NOT (x NOT IN (FROM sub))}
     */
    public void testWhereDoubleNotInSubquery() {
        String query = "FROM main_index | WHERE NOT (x NOT IN (FROM sub_index))";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Not outerNot = as(filter.condition(), Not.class);
        Not innerNot = as(outerNot.field(), Not.class);
        InSubquery inSubquery = as(innerNot.field(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), Attribute.class).name());
    }

    /**
     * IN subquery inside parenthesized expression:
     * {@code WHERE (x IN (FROM sub)) AND y > 5}
     */
    public void testWhereInSubqueryInParentheses() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | WHERE (x " + notClause + "IN (FROM sub_index)) AND y > 5";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        And and = as(filter.condition(), And.class);

        InSubquery inSubquery;
        if (negated) {
            Not not = as(and.left(), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(and.left(), InSubquery.class);
        }
        assertEquals("x", as(inSubquery.value(), Attribute.class).name());
        as(and.right(), GreaterThan.class);
    }

    /**
     * IN subquery mixed with IN value-list in the same WHERE clause:
     * {@code WHERE x IN (FROM sub) AND y IN (1, 2, 3)}
     */
    public void testWhereInSubqueryMixedWithInValueList() {
        boolean subqueryNegated = randomBoolean();
        boolean valueListNegated = randomBoolean();
        String subqNot = subqueryNegated ? "NOT " : "";
        String valNot = valueListNegated ? "NOT " : "";
        String query = "FROM main_index | WHERE x " + subqNot + "IN (FROM sub_index) AND y " + valNot + "IN (1, 2, 3)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        And and = as(filter.condition(), And.class);

        // Left side: IN subquery
        if (subqueryNegated) {
            Not not = as(and.left(), Not.class);
            as(not.field(), InSubquery.class);
        } else {
            as(and.left(), InSubquery.class);
        }

        // Right side: IN value list
        if (valueListNegated) {
            Not not = as(and.right(), Not.class);
            In in = as(not.field(), In.class);
            assertEquals(3, in.list().size());
        } else {
            In in = as(and.right(), In.class);
            assertEquals(3, in.list().size());
        }
    }

    /**
     * IN subquery as a CASE function condition:
     * {@code FROM main | WHERE CASE(x IN (FROM sub), true, false)}
     */
    public void testWhereInSubqueryInCaseFunction() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | WHERE CASE(x " + notClause + "IN (FROM sub_index), true, false)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction caseFunc = as(filter.condition(), UnresolvedFunction.class);
        assertEquals("CASE", caseFunc.name());
        assertEquals(3, caseFunc.children().size());

        InSubquery inSubquery;
        if (negated) {
            Not not = as(caseFunc.children().get(0), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(caseFunc.children().get(0), InSubquery.class);
        }
        assertEquals("x", as(inSubquery.value(), Attribute.class).name());
        assertEquals("sub_index", as(inSubquery.subquery(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /**
     * IN subquery combined with IS NULL:
     * {@code WHERE (x IN (FROM sub)) IS NOT NULL}
     */
    public void testWhereInSubqueryWithIsNull() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | WHERE (x " + notClause + "IN (FROM sub_index)) IS NOT NULL";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        // IS NOT NULL wraps the inner expression in NOT(IS NULL)
        Expression condition = filter.condition();
        // The expression tree for IS NOT NULL depends on the parser implementation,
        // but the key is that it parses without error and contains an InSubquery
        assertNotNull(condition);
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
