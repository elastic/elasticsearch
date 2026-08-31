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
import org.elasticsearch.xpack.esql.core.expression.Lambda;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.MultiColumnInSubquery;
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
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;

public class InSubqueryParserTests extends AbstractStatementParserTests {

    private static void checkLambda() {
        assumeTrue("Requires Lambda syntax support", EsqlCapabilities.Cap.LAMBDA_SYNTAX.isEnabled());
    }

    private static void checkMultiColumnInSubquery() {
        assumeTrue("multi-column IN subquery", EsqlCapabilities.Cap.WHERE_IN_MULTI_COLUMN_SUBQUERY.isEnabled());
    }

    /*
     * Filter[InSubquery[?x,UnresolvedRelation[sub_index]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testWhereInSubqueryBasic() {
        String query = "FROM main_index | WHERE x IN (FROM sub_index)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);
        UnresolvedAttribute value = as(inSubquery.value(), UnresolvedAttribute.class);
        assertEquals("x", value.name());

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /*
     * Verifies that hidden tokens (whitespace, comments) between the IN keyword and
     * the opening '(' of the subquery don't break recognition. The IN_SUBQUERY mode
     * routes WS / LINE_COMMENT / MULTILINE_COMMENT to the hidden channel, so the
     * IN_SUBQUERY_LP rule should still fire when the next default-channel char is '('.
     */
    public void testWhereInSubqueryWithHiddenTokensBeforeParenthesis() {
        String[] queries = new String[] {
            "FROM main_index | WHERE x IN               (FROM sub_index)",
            "FROM main_index | WHERE x IN       (        FROM sub_index)",
            "FROM main_index | WHERE x IN /* some comment */ (FROM sub_index)",
            "FROM main_index | WHERE x IN // line comment\n (FROM sub_index)" };

        for (String query : queries) {
            LogicalPlan plan = query(query);
            Filter filter = as(plan, Filter.class);
            InSubquery inSubquery = as(filter.condition(), InSubquery.class);
            UnresolvedAttribute value = as(inSubquery.value(), UnresolvedAttribute.class);
            assertEquals(query, "x", value.name());

            UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
            assertEquals(query, "sub_index", subqueryRelation.indexPattern().indexPattern());

            UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
            assertEquals(query, "main_index", mainRelation.indexPattern().indexPattern());
        }
    }

    /*
     * Same as the previous test but with the hidden tokens between the opening '(' and
     * the source command keyword. This is harder than the IN→'(' case because the IN_SUBQUERY_LP
     * rule has to match the whole `( ... keyword` span as a single token (it can't yield to
     * the surrounding hidden-channel rules for arbitrary content inside the lookahead), so
     * WS / LINE_COMMENT / MULTILINE_COMMENT have to be spelled out explicitly inside the rule.
     */
    public void testWhereInSubqueryWithHiddenTokensAfterParenthesis() {
        String[] queries = new String[] {
            "FROM main_index | WHERE x IN (   FROM sub_index)",
            "FROM main_index | WHERE x IN ( /* some comment */ FROM sub_index)",
            "FROM main_index | WHERE x IN ( /* one */ /* two */ FROM sub_index)",
            "FROM main_index | WHERE x IN ( // line comment\n FROM sub_index)",
            "FROM main_index | WHERE x IN ( /* mixed */ \n  // line\n FROM sub_index)" };

        for (String query : queries) {
            LogicalPlan plan = query(query);
            Filter filter = as(plan, Filter.class);
            InSubquery inSubquery = as(filter.condition(), InSubquery.class);
            UnresolvedAttribute value = as(inSubquery.value(), UnresolvedAttribute.class);
            assertEquals(query, "x", value.name());

            UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
            assertEquals(query, "sub_index", subqueryRelation.indexPattern().indexPattern());

            UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
            assertEquals(query, "main_index", mainRelation.indexPattern().indexPattern());
        }
    }

    /*
     * Filter[NOT(InSubquery[?x,UnresolvedRelation[sub_index]])]
     * \_UnresolvedRelation[main_index]
     */
    public void testWhereNotInSubquery() {
        String query = "FROM main_index | WHERE x NOT IN (FROM sub_index)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Not not = as(filter.condition(), Not.class);
        InSubquery inSubquery = as(not.field(), InSubquery.class);
        UnresolvedAttribute value = as(inSubquery.value(), UnresolvedAttribute.class);
        assertEquals("x", value.name());

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /*
     * (NOT) IN subquery with multiple processing commands in the subquery.
     *
     * Filter[(NOT) InSubquery[?x, subquery_plan]]
     * \_UnresolvedRelation[main_index]
     *
     * subquery_plan: ChangePoint -> Enrich -> LookupJoin[right=UnresolvedRelation[lookup_index]]
     *   -> MvExpand -> Rename -> Keep -> Drop -> Limit -> OrderBy -> Grok -> Dissect
     *   -> InlineStats -> Aggregate -> Aggregate -> Fork[2 branches]
     *   each branch: Eval -> Filter -> Eval -> Filter -> UnresolvedRelation[sub_index]
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

    /*
     * WHERE (NOT) IN subquery ends with different modes to verify lexer mode transitions.
     */
    public void testWhereInSubqueryEndsWithDifferentModes() {
        Map<String, Class<? extends LogicalPlan>> processingCommands = Map.ofEntries(
            Map.entry("WHERE a > 10", Filter.class),
            Map.entry("EVAL b = a * 2", Eval.class),
            Map.entry("KEEP x", Keep.class),
            Map.entry("DROP y", Drop.class),
            Map.entry("SORT a", OrderBy.class),
            Map.entry("LIMIT 10", Limit.class),
            Map.entry("STATS cnt = COUNT(*) BY a", Aggregate.class),
            Map.entry("RENAME a AS b", Rename.class),
            Map.entry("MV_EXPAND m", MvExpand.class),
            Map.entry("CHANGE_POINT a ON b", ChangePoint.class),
            Map.entry("ENRICH my_policy ON x", Enrich.class),
            Map.entry("FORK (WHERE a > 1)(WHERE a < 10)", Fork.class),
            Map.entry("INLINE STATS cnt = COUNT(*) BY a", InlineStats.class),
            Map.entry("LOOKUP JOIN lookup_index ON x", LookupJoin.class)
        );
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT" : "";
        for (var entry : processingCommands.entrySet()) {
            String query = LoggerMessageFormat.format(null, """
                FROM main_index | WHERE x {} IN (FROM sub_index | {})
                """, notClause, entry.getKey());

            LogicalPlan plan = query(query);
            Filter filter = as(plan, Filter.class);
            InSubquery inSubquery;
            if (negated) {
                Not not = as(filter.condition(), Not.class);
                inSubquery = as(not.field(), InSubquery.class);
            } else {
                inSubquery = as(filter.condition(), InSubquery.class);
            }
            as(inSubquery.subquery(), entry.getValue());
            UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
            assertEquals("main_index", mainRelation.indexPattern().indexPattern());
        }
    }

    /*
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
        Keep keep = as(inSubquery.subquery(), Keep.class);
        UnresolvedRelation subqueryRelation = as(keep.child(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());
        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /*
     * Existing value list IN still works after the grammar changes.
     *
     * Filter[In[?x, [1, 2, 3]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testWhereInValueListStillWorks() {
        String query = "FROM main_index | WHERE x IN (1, 2, 3)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        In in = as(filter.condition(), In.class);
        Attribute value = as(in.value(), Attribute.class);
        assertEquals("x", value.name());
        assertEquals(3, in.list().size());
        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /*
     * IN value-list with a mix of constants and field references:
     * {@code WHERE x IN (1, y, "hello", z)}
     *
     * Filter[In[?x, [1, ?y, "hello", ?z]]]
     * \_UnresolvedRelation[main_index]
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
        Literal literal = as(list.get(0), Literal.class);
        assertEquals(1, literal.value());
        UnresolvedAttribute fieldY = as(list.get(1), UnresolvedAttribute.class);
        assertEquals("y", fieldY.name());
        literal = as(list.get(2), Literal.class);
        assertEquals(new BytesRef("hello"), literal.value());
        UnresolvedAttribute fieldZ = as(list.get(3), UnresolvedAttribute.class);
        assertEquals("z", fieldZ.name());

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /*
     * Multiple IN and/or NOT IN subqueries in the same WHERE clause, combined with AND or OR.
     *
     * Filter[And|Or[(NOT) InSubquery[?x, Keep[UnresolvedRelation[sub1]]], (NOT) InSubquery[?y, Keep[UnresolvedRelation[sub2]]]]]
     * \_UnresolvedRelation[main_index]
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

    /*
     * IN/NOT IN subqueries combined with AND and OR. Operator precedence makes
     * AND bind tighter than OR, so the parse tree is:
     *
     * Filter[Or[And[InSubquery[?f1, Keep[UnresolvedRelation[sub1]]],
     *               Not[InSubquery[?f2, Keep[Filter[UnresolvedRelation[sub2]]]]]],
     *           InSubquery[?f3, Keep[Limit[OrderBy[Aggregate[UnresolvedRelation[sub3]]]]]]]]
     * \_UnresolvedRelation[main_index]
     *
     */
    public void testMultipleWhereInSubqueries() {
        String query = """
            FROM main_index
            | WHERE main_index_field1 IN (FROM sub_index1
                                          | KEEP sub_index1_field1)
              AND main_index_field2 NOT IN (FROM sub_index2
                                            | WHERE sub_index2_field1 > 0
                                            | KEEP sub_index2_field2)
              OR main_index_field3 IN (FROM sub_index3
                                       | STATS count=COUNT(*) BY sub_index3_field1
                                       | SORT count DESC
                                       | LIMIT 5
                                       | KEEP sub_index3_field1)
            """;

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Or or = as(filter.condition(), Or.class);
        And and = as(or.left(), And.class);

        // First branch: main_index_field1 IN (FROM sub_index1 | KEEP sub_index1_field1)
        InSubquery firstIn = as(and.left(), InSubquery.class);
        assertEquals("main_index_field1", as(firstIn.value(), UnresolvedAttribute.class).name());
        Keep firstKeep = as(firstIn.subquery(), Keep.class);
        UnresolvedRelation firstRelation = as(firstKeep.child(), UnresolvedRelation.class);
        assertEquals("sub_index1", firstRelation.indexPattern().indexPattern());

        // Second branch: main_index_field2 NOT IN (FROM sub_index2 | WHERE ... | KEEP ...)
        Not not = as(and.right(), Not.class);
        InSubquery secondIn = as(not.field(), InSubquery.class);
        assertEquals("main_index_field2", as(secondIn.value(), UnresolvedAttribute.class).name());
        Keep secondKeep = as(secondIn.subquery(), Keep.class);
        Filter secondFilter = as(secondKeep.child(), Filter.class);
        as(secondFilter.condition(), GreaterThan.class);
        UnresolvedRelation secondRelation = as(secondFilter.child(), UnresolvedRelation.class);
        assertEquals("sub_index2", secondRelation.indexPattern().indexPattern());

        // Third branch: main_index_field3 IN (FROM sub_index3 | STATS ... | SORT ... | LIMIT 5 | KEEP ...)
        InSubquery thirdIn = as(or.right(), InSubquery.class);
        assertEquals("main_index_field3", as(thirdIn.value(), UnresolvedAttribute.class).name());
        Keep thirdKeep = as(thirdIn.subquery(), Keep.class);
        Limit thirdLimit = as(thirdKeep.child(), Limit.class);
        OrderBy thirdOrderBy = as(thirdLimit.child(), OrderBy.class);
        Aggregate thirdAggregate = as(thirdOrderBy.child(), Aggregate.class);
        UnresolvedRelation thirdRelation = as(thirdAggregate.child(), UnresolvedRelation.class);
        assertEquals("sub_index3", thirdRelation.indexPattern().indexPattern());

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /*
     * Two IN predicates in the same WHERE clause, each randomly an IN value-list or IN subquery, with random NOT.
     *
     * Filter[And[(NOT) In|InSubquery[?x, ...], (NOT) In|InSubquery[?y, ...]]]
     * \_UnresolvedRelation[main_index]
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

    /*
     * IN subquery where the subquery's FROM command includes METADATA fields.
     *
     * Filter[InSubquery[?x, Keep[UnresolvedRelation[sub_index, METADATA _id, _index]]]]
     * \_UnresolvedRelation[main_index]
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

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /*
     * IN subquery where the subquery's FROM command references a remote cluster index.
     *
     * Filter[InSubquery[?x, Keep[UnresolvedRelation[remote_cluster:sub_index]]]]
     * \_UnresolvedRelation[main_index]
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

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /*
     * IN subquery whose FROM command contains a nested FROM-subquery:
     * {@code FROM main | WHERE x IN (FROM sub1, (FROM sub2) | KEEP a)}
     *
     * Filter[InSubquery[?x, Keep[UnionAll[UnresolvedRelation[sub1], Subquery[UnresolvedRelation[sub2]]]]]]
     * \_UnresolvedRelation[main_index]
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
        Subquery subquery = as(unionAll.children().get(1), Subquery.class);
        UnresolvedRelation sub2 = as(subquery.child(), UnresolvedRelation.class);
        assertEquals("sub2", sub2.indexPattern().indexPattern());

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /*
     * Nested IN subqueries: the inner subquery itself contains a WHERE IN subquery:
     * {@code FROM main | WHERE x IN (FROM sub1 | WHERE y IN (FROM sub2 | KEEP b) | KEEP a)}
     *
     * Filter[InSubquery[?x, Keep[Filter[InSubquery[?y, Keep[UnresolvedRelation[sub2]]]]][UnresolvedRelation[sub1]]]]]
     * \_UnresolvedRelation[main_index]
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

    /*
     * FROM subquery where one branch contains a WHERE IN subquery:
     * {@code FROM main, (FROM sub1 | WHERE x IN (FROM sub2 | KEEP a) | KEEP x)}
     *
     * UnionAll
     * \_UnresolvedRelation[main]
     * \_Subquery[Keep[Filter[InSubquery[?x, Keep[UnresolvedRelation[sub2]]]][UnresolvedRelation[sub1]]]]
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

    /*
     * Single parameter for constant values on the LHS of IN subquery, the parameter resolves to a Literal.
     *
     * Filter[(NOT) InSubquery[42, Filter[Equals[42, ?x]][UnresolvedRelation[sub_index]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testWhereInSubqueryWithSingleParam() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        Map<String, QueryParam> params = Map.ofEntries(
            Map.entry("?val", paramAsConstant("val", 42)),
            Map.entry("?1", paramAsConstant(null, 42)),
            Map.entry("?", paramAsConstant(null, 42))
        );

        for (Map.Entry<String, QueryParam> entry : params.entrySet()) {
            String query = "FROM main_index | WHERE "
                + notClause
                + entry.getKey()
                + " IN (FROM sub_index | WHERE "
                + entry.getKey()
                + " == x)";

            LogicalPlan plan = query(query, new QueryParams(List.of(entry.getValue(), entry.getValue())));
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

            Filter subqueryFilter = as(inSubquery.subquery(), Filter.class);
            Equals equals = as(subqueryFilter.condition(), Equals.class);
            value = as(equals.left(), Literal.class);
            assertEquals(42, value.value());
            UnresolvedRelation subqueryRelation = as(subqueryFilter.child(), UnresolvedRelation.class);
            assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());

            UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
            assertEquals("main_index", mainRelation.indexPattern().indexPattern());
        }
    }

    /*
     * Double parameter for identifiers on the LHS of IN subquery, the parameter resolves to an UnresolvedAttribute.
     *
     * Filter[(NOT) InSubquery[?x, Filter[Equals[?x, 1]][UnresolvedRelation[sub_index]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testWhereInSubqueryWithDoubleParam() {
        assumeTrue("double parameters markers for identifiers", EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled());
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        Map<String, QueryParam> params = Map.ofEntries(
            Map.entry("??field", paramAsConstant("field", "x")),
            Map.entry("??1", paramAsConstant(null, "x")),
            Map.entry("??", paramAsConstant(null, "x"))
        );
        for (Map.Entry<String, QueryParam> entry : params.entrySet()) {
            String query = "FROM main_index | WHERE "
                + notClause
                + entry.getKey()
                + " IN (FROM sub_index | WHERE "
                + entry.getKey()
                + " == 1 )";

            LogicalPlan plan = query(query, new QueryParams(List.of(entry.getValue(), entry.getValue())));
            Filter filter = as(plan, Filter.class);
            InSubquery inSubquery;
            if (negated) {
                Not not = as(filter.condition(), Not.class);
                inSubquery = as(not.field(), InSubquery.class);
            } else {
                inSubquery = as(filter.condition(), InSubquery.class);
            }
            Attribute attribute = as(inSubquery.value(), Attribute.class);
            assertEquals("x", attribute.name());

            Filter subqueryFilter = as(inSubquery.subquery(), Filter.class);
            Equals equals = as(subqueryFilter.condition(), Equals.class);
            attribute = as(equals.left(), Attribute.class);
            assertEquals("x", attribute.name());
            UnresolvedRelation subqueryRelation = as(subqueryFilter.child(), UnresolvedRelation.class);
            assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());

            UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
            assertEquals("main_index", mainRelation.indexPattern().indexPattern());
        }
    }

    /*
     * Parameters inside the subquery: WHERE x IN (FROM sub | WHERE a > ?val | KEEP ??field).
     * Tests named, positional, and anonymous variants.
     *
     * Filter[InSubquery[?x, Keep[Filter[GreaterThan[?a, 10]][UnresolvedRelation[sub_index]]]]]
     * \_UnresolvedRelation[main_index]
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

    /*
     * Parameters on both the LHS and inside the subquery:
     * WHERE ?val IN (FROM sub | WHERE a > ?threshold) with named, positional, and anonymous variants.
     *
     * Filter[InSubquery[42, Filter[GreaterThan[?a, 10]][UnresolvedRelation[sub_index]]]]
     * \_UnresolvedRelation[main_index]
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

    /*
     * EVAL with (NOT) IN subquery as a boolean expression:
     * {@code FROM main | EVAL is_match = x IN (FROM sub)}
     *
     * Eval[is_match = (NOT) InSubquery[?x, UnresolvedRelation[sub_index]]]
     * \_UnresolvedRelation[main_index]
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

    /*
     * EVAL with (NOT) IN subquery as an implicit field name (no alias):
     * {@code FROM main | EVAL x IN (FROM sub)}
     *
     * Eval[(NOT) InSubquery[?x, UnresolvedRelation[sub_index]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testEvalWithInSubqueryImplicitName() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | EVAL x " + notClause + "IN (FROM sub_index)";

        LogicalPlan plan = query(query);
        Eval eval = as(plan, Eval.class);
        assertEquals(1, eval.fields().size());
        Alias alias = eval.fields().get(0);
        InSubquery inSubquery;
        if (negated) {
            Not not = as(alias.child(), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(alias.child(), InSubquery.class);
        }

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());

        UnresolvedRelation mainRelation = as(eval.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /*
     * EVAL with multiple fields where one is an (NOT) IN subquery:
     * {@code FROM main | EVAL a = 1, is_match = x IN (FROM sub), b = y + 2}
     *
     * Eval[a = 1, is_match = (NOT) InSubquery[?x, UnresolvedRelation[sub_index]], b = ?y]
     * \_UnresolvedRelation[main_index]
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
        InSubquery inSubquery;
        if (negated) {
            Not not = as(second.child(), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(second.child(), InSubquery.class);
        }

        Alias third = eval.fields().get(2);
        assertEquals("b", third.name());

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());

        UnresolvedRelation mainRelation = as(eval.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /*
     * SORT with (NOT) IN subquery as the sort expression:
     * {@code FROM main | SORT x IN (FROM sub) ASC}
     *
     * OrderBy[(NOT) InSubquery[?x, UnresolvedRelation[sub_index]] ASC]
     * \_UnresolvedRelation[main_index]
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

    /*
     * STATS aggregation with (NOT) IN subquery in the WHERE filter:
     * {@code FROM main | STATS c = COUNT(*) WHERE x IN (FROM sub)}
     *
     * Aggregate[c = COUNT(*) WHERE (NOT) InSubquery[?x, UnresolvedRelation[sub_index]]]
     * \_UnresolvedRelation[main_index]
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

        UnresolvedRelation mainRelation = as(aggregate.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /*
     * STATS aggregation filter with a conjunctive (NOT) IN subquery mixed with other predicates:
     * {@code FROM main | STATS c = COUNT(*) WHERE a > 5 AND x IN (FROM sub) AND b == 10}
     *
     * Aggregate[c = COUNT(*) WHERE And[And[?a > 5, (NOT) InSubquery[?x, UnresolvedRelation[sub_index]]], ?b == 10]]
     * \_UnresolvedRelation[main_index]
     */
    public void testStatsAggFilterWithConjunctiveInSubquery() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | STATS c = COUNT(*) WHERE a > 5 AND x " + notClause + "IN (FROM sub_index) AND b == 10";

        LogicalPlan plan = query(query);
        Aggregate aggregate = as(plan, Aggregate.class);
        FilteredExpression filtered = as(as(aggregate.aggregates().get(0), Alias.class).child(), FilteredExpression.class);

        And outerAnd = as(filtered.filter(), And.class);
        as(outerAnd.right(), Equals.class);
        And innerAnd = as(outerAnd.left(), And.class);
        as(innerAnd.left(), GreaterThan.class);
        InSubquery inSubquery;
        if (negated) {
            inSubquery = as(as(innerAnd.right(), Not.class).field(), InSubquery.class);
        } else {
            inSubquery = as(innerAnd.right(), InSubquery.class);
        }
        assertEquals("x", as(inSubquery.value(), Attribute.class).name());
        assertEquals("sub_index", as(inSubquery.subquery(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /*
     * STATS aggregation filter with a disjunctive (NOT) IN subquery mixed with another predicate:
     * {@code FROM main | STATS c = COUNT(*) WHERE x IN (FROM sub) OR y > 5}
     *
     * Aggregate[c = COUNT(*) WHERE Or[(NOT) InSubquery[?x, UnresolvedRelation[sub_index]], ?y > 5]]
     * \_UnresolvedRelation[main_index]
     */
    public void testStatsAggFilterWithDisjunctiveInSubquery() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | STATS c = COUNT(*) WHERE x " + notClause + "IN (FROM sub_index) OR y > 5";

        LogicalPlan plan = query(query);
        Aggregate aggregate = as(plan, Aggregate.class);
        FilteredExpression filtered = as(as(aggregate.aggregates().get(0), Alias.class).child(), FilteredExpression.class);

        Or or = as(filtered.filter(), Or.class);
        as(or.right(), GreaterThan.class);
        InSubquery inSubquery;
        if (negated) {
            inSubquery = as(as(or.left(), Not.class).field(), InSubquery.class);
        } else {
            inSubquery = as(or.left(), InSubquery.class);
        }
        assertEquals("x", as(inSubquery.value(), Attribute.class).name());
        assertEquals("sub_index", as(inSubquery.subquery(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /*
     * STATS aggregation filter mixing conjunction and disjunction with two IN subqueries:
     * {@code FROM main | STATS c = COUNT(*) WHERE (x IN (FROM sub1) OR a > 5) AND y NOT IN (FROM sub2)}
     *
     * Aggregate[c = COUNT(*) WHERE And[Or[InSubquery[?x, sub1], ?a > 5], Not[InSubquery[?y, sub2]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testStatsAggFilterWithMixedConjunctiveDisjunctiveInSubqueries() {
        String query = "FROM main_index | STATS c = COUNT(*) WHERE (x IN (FROM sub1) OR a > 5) AND y NOT IN (FROM sub2)";

        LogicalPlan plan = query(query);
        Aggregate aggregate = as(plan, Aggregate.class);
        FilteredExpression filtered = as(as(aggregate.aggregates().get(0), Alias.class).child(), FilteredExpression.class);

        And and = as(filtered.filter(), And.class);
        Or or = as(and.left(), Or.class);
        InSubquery firstIn = as(or.left(), InSubquery.class);
        assertEquals("x", as(firstIn.value(), Attribute.class).name());
        assertEquals("sub1", as(firstIn.subquery(), UnresolvedRelation.class).indexPattern().indexPattern());
        as(or.right(), GreaterThan.class);

        InSubquery secondIn = as(as(and.right(), Not.class).field(), InSubquery.class);
        assertEquals("y", as(secondIn.value(), Attribute.class).name());
        assertEquals("sub2", as(secondIn.subquery(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /*
     * STATS aggregation filter with a CASE referencing a (NOT) IN subquery:
     * {@code FROM main | STATS c = COUNT(*) WHERE CASE(x IN (FROM sub), a > 5, false)}
     *
     * Aggregate[c = COUNT(*) WHERE CASE[(NOT) InSubquery[?x, UnresolvedRelation[sub_index]], ?a > 5, false]]
     * \_UnresolvedRelation[main_index]
     */
    public void testStatsAggFilterWithCaseReferencingInSubquery() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | STATS c = COUNT(*) WHERE CASE(x " + notClause + "IN (FROM sub_index), a > 5, false)";

        LogicalPlan plan = query(query);
        Aggregate aggregate = as(plan, Aggregate.class);
        FilteredExpression filtered = as(as(aggregate.aggregates().get(0), Alias.class).child(), FilteredExpression.class);

        UnresolvedFunction caseFunction = as(filtered.filter(), UnresolvedFunction.class);
        assertEquals("CASE", caseFunction.name());
        assertEquals(3, caseFunction.children().size());
        InSubquery inSubquery;
        if (negated) {
            inSubquery = as(as(caseFunction.children().get(0), Not.class).field(), InSubquery.class);
        } else {
            inSubquery = as(caseFunction.children().get(0), InSubquery.class);
        }
        assertEquals("x", as(inSubquery.value(), Attribute.class).name());
        assertEquals("sub_index", as(inSubquery.subquery(), UnresolvedRelation.class).indexPattern().indexPattern());
        as(caseFunction.children().get(1), GreaterThan.class);
        as(caseFunction.children().get(2), Literal.class);
    }

    /*
     * STATS aggregation filter with a COALESCE referencing a (NOT) IN subquery:
     * {@code FROM main | STATS c = COUNT(*) WHERE COALESCE(x IN (FROM sub), false)}
     *
     * Aggregate[c = COUNT(*) WHERE COALESCE[(NOT) InSubquery[?x, UnresolvedRelation[sub_index]], false]]
     * \_UnresolvedRelation[main_index]
     */
    public void testStatsAggFilterWithCoalesceReferencingInSubquery() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | STATS c = COUNT(*) WHERE COALESCE(x " + notClause + "IN (FROM sub_index), false)";

        LogicalPlan plan = query(query);
        Aggregate aggregate = as(plan, Aggregate.class);
        FilteredExpression filtered = as(as(aggregate.aggregates().get(0), Alias.class).child(), FilteredExpression.class);

        UnresolvedFunction coalesce = as(filtered.filter(), UnresolvedFunction.class);
        assertEquals("COALESCE", coalesce.name());
        assertEquals(2, coalesce.children().size());
        InSubquery inSubquery;
        if (negated) {
            inSubquery = as(as(coalesce.children().get(0), Not.class).field(), InSubquery.class);
        } else {
            inSubquery = as(coalesce.children().get(0), InSubquery.class);
        }
        assertEquals("x", as(inSubquery.value(), Attribute.class).name());
        assertEquals("sub_index", as(inSubquery.subquery(), UnresolvedRelation.class).indexPattern().indexPattern());
        as(coalesce.children().get(1), Literal.class);
    }

    /*
     * STATS aggregation filter with IS [NOT] NULL over an IN subquery:
     * {@code FROM main | STATS c = COUNT(*) WHERE (x IN (FROM sub)) IS [NOT] NULL}
     *
     * Aggregate[c = COUNT(*) WHERE Is(Not)Null[InSubquery[?x, UnresolvedRelation[sub_index]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testStatsAggFilterWithIsNullReferencingInSubquery() {
        boolean isNotNull = randomBoolean();
        String nullClause = isNotNull ? "IS NOT NULL" : "IS NULL";
        String query = "FROM main_index | STATS c = COUNT(*) WHERE (x IN (FROM sub_index)) " + nullClause;

        LogicalPlan plan = query(query);
        Aggregate aggregate = as(plan, Aggregate.class);
        FilteredExpression filtered = as(as(aggregate.aggregates().get(0), Alias.class).child(), FilteredExpression.class);

        InSubquery inSubquery;
        if (isNotNull) {
            inSubquery = as(as(filtered.filter(), IsNotNull.class).field(), InSubquery.class);
        } else {
            inSubquery = as(as(filtered.filter(), IsNull.class).field(), InSubquery.class);
        }
        assertEquals("x", as(inSubquery.value(), Attribute.class).name());
        assertEquals("sub_index", as(inSubquery.subquery(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /*
     * STATS aggregation filter with a single-column IN subquery using ROW source:
     * {@code FROM main | STATS c = COUNT(*) WHERE x IN (ROW a = 1 | KEEP a)}
     */
    public void testStatsAggFilterWithRowInSubquery() {
        String query = "FROM main | STATS c = COUNT(*) WHERE x IN (ROW a = 1 | KEEP a)";

        LogicalPlan plan = query(query);
        Aggregate aggregate = as(plan, Aggregate.class);
        FilteredExpression filtered = as(as(aggregate.aggregates().get(0), Alias.class).child(), FilteredExpression.class);

        InSubquery inSubquery = as(filtered.filter(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), Attribute.class).name());
        Keep keep = as(inSubquery.subquery(), Keep.class);
        as(keep.child(), Row.class);
    }

    /*
     * STATS aggregation filter with a single-column IN subquery using TS source:
     * {@code FROM main | STATS c = COUNT(*) WHERE x IN (TS sub_source | STATS max(rate(val)) BY ts | KEEP a)}
     */
    public void testStatsAggFilterWithTsInSubquery() {
        String query = "FROM main | STATS c = COUNT(*) WHERE x IN (TS sub_source | STATS max(rate(val)) BY ts | KEEP a)";

        LogicalPlan plan = query(query);
        Aggregate aggregate = as(plan, Aggregate.class);
        FilteredExpression filtered = as(as(aggregate.aggregates().get(0), Alias.class).child(), FilteredExpression.class);

        InSubquery inSubquery = as(filtered.filter(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), Attribute.class).name());
        Keep keep = as(inSubquery.subquery(), Keep.class);
        as(keep.child(), TimeSeriesAggregate.class);
    }

    /*
     * STATS aggregation filter with a multi-column IN subquery using ROW source:
     * {@code FROM main | STATS c = COUNT(*) WHERE (x, y) IN (ROW a = 1, b = 2 | KEEP a, b)}
     */
    public void testStatsAggFilterWithMultiColumnRowInSubquery() {
        checkMultiColumnInSubquery();
        String query = "FROM main | STATS c = COUNT(*) WHERE (x, y) IN (ROW a = 1, b = 2 | KEEP a, b)";

        LogicalPlan plan = query(query);
        Aggregate aggregate = as(plan, Aggregate.class);
        FilteredExpression filtered = as(as(aggregate.aggregates().get(0), Alias.class).child(), FilteredExpression.class);

        MultiColumnInSubquery mcs = as(filtered.filter(), MultiColumnInSubquery.class);
        assertEquals(2, mcs.values().size());
        Keep keep = as(mcs.subquery(), Keep.class);
        as(keep.child(), Row.class);
    }

    /*
     * STATS aggregation filter with a multi-column IN subquery using TS source:
     * {@code FROM main | STATS c = COUNT(*) WHERE (x, y) IN (TS sub_source | STATS max(rate(val)) BY ts | KEEP a, b)}
     */
    public void testStatsAggFilterWithMultiColumnTsInSubquery() {
        checkMultiColumnInSubquery();
        String query = "FROM main | STATS c = COUNT(*) WHERE (x, y) IN (TS sub_source | STATS max(rate(val)) BY ts | KEEP a, b)";

        LogicalPlan plan = query(query);
        Aggregate aggregate = as(plan, Aggregate.class);
        FilteredExpression filtered = as(as(aggregate.aggregates().get(0), Alias.class).child(), FilteredExpression.class);

        MultiColumnInSubquery mcs = as(filtered.filter(), MultiColumnInSubquery.class);
        assertEquals(2, mcs.values().size());
        Keep keep = as(mcs.subquery(), Keep.class);
        as(keep.child(), TimeSeriesAggregate.class);
    }

    /*
     * STATS aggregation filter with an IN subquery nested inside complex functions:
     */
    public void testStatsAggFilterWithInSubqueryInNestedFunctions() {
        checkMultiColumnInSubquery();
        String query = """
            FROM main
            | STATS c = COUNT(*) WHERE COALESCE(CASE(x IN (ROW a = 1 | KEEP a),
                                                     true,
                                                     (y, z) IN (TS sub | STATS max(rate(val)) BY ts | KEEP b, c)),
                                                false)""";

        LogicalPlan plan = query(query);
        Aggregate aggregate = as(plan, Aggregate.class);
        FilteredExpression filtered = as(as(aggregate.aggregates().get(0), Alias.class).child(), FilteredExpression.class);

        UnresolvedFunction coalesce = as(filtered.filter(), UnresolvedFunction.class);
        assertEquals("COALESCE", coalesce.name());
        UnresolvedFunction caseFunc = as(coalesce.children().get(0), UnresolvedFunction.class);
        assertEquals("CASE", caseFunc.name());

        // CASE condition: x IN (ROW ...)
        InSubquery firstIn = as(caseFunc.children().get(0), InSubquery.class);
        as(as(firstIn.subquery(), Keep.class).child(), Row.class);

        // CASE else: (y, z) IN (TS ...)
        MultiColumnInSubquery secondIn = as(caseFunc.children().get(2), MultiColumnInSubquery.class);
        as(as(secondIn.subquery(), Keep.class).child(), TimeSeriesAggregate.class);
    }

    /*
     * STATS aggregation filter with IN subqueries nested inside IS NULL and IS NOT NULL:
     */
    public void testStatsAggFilterWithInSubqueryInNullPredicates() {
        checkMultiColumnInSubquery();
        String query = """
            FROM main
            | STATS c = COUNT(*) WHERE (x IN (ROW a = 1 | KEEP a)) IS NULL
                                   OR ((y, z) IN (TS sub | STATS max(rate(val)) BY ts | KEEP b, c)) IS NOT NULL""";

        LogicalPlan plan = query(query);
        Aggregate aggregate = as(plan, Aggregate.class);
        FilteredExpression filtered = as(as(aggregate.aggregates().get(0), Alias.class).child(), FilteredExpression.class);

        Or or = as(filtered.filter(), Or.class);

        // Left: (x IN (ROW ...)) IS NULL
        IsNull isNull = as(or.left(), IsNull.class);
        as(isNull.field(), InSubquery.class);

        // Right: ((y, z) IN (TS ...)) IS NOT NULL
        IsNotNull isNotNull = as(or.right(), IsNotNull.class);
        as(isNotNull.field(), MultiColumnInSubquery.class);
    }

    /*
     * STATS with (NOT) IN subquery in the BY clause:
     * {@code FROM main | STATS c = COUNT(*) BY x IN (FROM sub)}
     * The BY expression is wrapped in an Alias with an auto-generated name.
     *
     * Aggregate[c = COUNT(*), BY (NOT) InSubquery[?x, UnresolvedRelation[sub_index]]]
     * \_UnresolvedRelation[main_index]
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

        UnresolvedRelation mainRelation = as(aggregate.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /*
     * LIMIT BY with (NOT) IN subquery as a grouping expression:
     * {@code FROM main | SORT a | LIMIT 10 BY x IN (FROM sub)}
     *
     * LimitBy[(NOT) InSubquery[?x, UnresolvedRelation[sub_index]]]
     * \_OrderBy
     *   \_UnresolvedRelation[main_index]
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

    /*
     * (NOT) IN subquery as a function argument:
     * {@code FROM main | EVAL result = COALESCE(x IN (FROM sub), false)}
     *
     * Eval[result = COALESCE((NOT) InSubquery[?x, UnresolvedRelation[sub_index]], false)]
     * \_UnresolvedRelation[main_index]
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

        UnresolvedRelation mainRelation = as(eval.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /*
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

    // ---- WHERE with IN subquery nested in other expressions ----

    /*
     * IN subquery combined with AND:
     * {@code WHERE a > 5 AND x IN (FROM sub) AND b < 10}
     *
     * Filter[And[And[GreaterThan[?a, 5], (NOT) InSubquery[?x, UnresolvedRelation[sub_index]]], LessThan[?b, 10]]]
     * \_UnresolvedRelation[main_index]
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
        InSubquery inSubquery;
        if (negated) {
            Not not = as(innerAnd.right(), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(innerAnd.right(), InSubquery.class);
        }

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());
        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /*
     * IN subquery combined with OR:
     * {@code WHERE x IN (FROM sub1) OR y IN (FROM sub2)}
     *
     * Filter[Or[(NOT) InSubquery[?x, UnresolvedRelation[sub1]], (NOT) InSubquery[?y, UnresolvedRelation[sub2]]]]
     * \_UnresolvedRelation[main_index]
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
        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /*
     * Double NOT with IN subquery:
     * {@code WHERE NOT (x NOT IN (FROM sub))}
     *
     * Filter[NOT(NOT(InSubquery[?x, UnresolvedRelation[sub_index]]))]
     * \_UnresolvedRelation[main_index]
     */
    public void testWhereDoubleNotInSubquery() {
        String query = "FROM main_index | WHERE NOT (x NOT IN (FROM sub_index))";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Not outerNot = as(filter.condition(), Not.class);
        Not innerNot = as(outerNot.field(), Not.class);
        InSubquery inSubquery = as(innerNot.field(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), Attribute.class).name());

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());
        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /*
     * IN subquery inside parenthesized expression:
     * {@code WHERE (x IN (FROM sub)) AND y > 5}
     *
     * Filter[And[(NOT) InSubquery[?x, UnresolvedRelation[sub_index]], GreaterThan[?y, 5]]]
     * \_UnresolvedRelation[main_index]
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

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());
        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /*
     * IN subquery mixed with IN value-list in the same WHERE clause:
     * {@code WHERE x IN (FROM sub) AND y IN (1, 2, 3)}
     *
     * Filter[And[(NOT) InSubquery[?x, UnresolvedRelation[sub_index]], (NOT) In[?y, [1, 2, 3]]]]
     * \_UnresolvedRelation[main_index]
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
        InSubquery inSubquery;
        if (subqueryNegated) {
            Not not = as(and.left(), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(and.left(), InSubquery.class);
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

        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());
        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /*
     * IN subquery as a CASE function condition:
     * {@code FROM main | WHERE CASE(x IN (FROM sub), true, false)}
     *
     * Filter[CASE((NOT) InSubquery[?x, UnresolvedRelation[sub_index]], true, false)]
     * \_UnresolvedRelation[main_index]
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

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /*
     * IN subquery combined with IS NULL:
     * {@code WHERE (x IN (FROM sub)) IS NOT NULL}
     *
     * Filter[IsNotNull[(NOT) InSubquery[?x, UnresolvedRelation[sub_index]]]]
     * \_UnresolvedRelation[main_index]
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
        IsNotNull isNotNull = as(condition, IsNotNull.class);
        InSubquery inSubquery;
        if (negated) {
            Not not = as(isNotNull.field(), Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(isNotNull.field(), InSubquery.class);
        }
        assertEquals("x", as(inSubquery.value(), Attribute.class).name());
        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());
        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    public void testInSubqueryNestedInsideCaseAddAndEquals() {
        String query = "FROM main | WHERE (CASE(x IN (FROM sub), 1, 0) + 1) == 2";
        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Equals equals = as(filter.condition(), Equals.class);
        // Add
        Add add = as(equals.left(), Add.class);
        // CASE
        UnresolvedFunction caseFunc = as(add.left(), UnresolvedFunction.class);
        assertEquals("CASE", caseFunc.name());
        // InSubquery
        InSubquery inSubquery = as(caseFunc.children().get(0), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), Attribute.class).name());
        assertEquals("sub", as(inSubquery.subquery(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    public void testInSubqueryNestedInsideCoalesceAddAndEquals() {
        String query = "FROM main | WHERE (CASE(COALESCE(x IN (FROM sub), false), 1, 0) + 1) == 2";
        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Equals equals = as(filter.condition(), Equals.class);
        // Add
        Add add = as(equals.left(), Add.class);
        // CASE
        UnresolvedFunction caseFunc = as(add.left(), UnresolvedFunction.class);
        assertEquals("CASE", caseFunc.name());
        // COALESCE
        UnresolvedFunction coalesceFunc = as(caseFunc.children().get(0), UnresolvedFunction.class);
        assertEquals("COALESCE", coalesceFunc.name());
        // InSubquery
        InSubquery inSubquery = as(coalesceFunc.children().get(0), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), Attribute.class).name());
        assertEquals("sub", as(inSubquery.subquery(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    public void testInSubqueryNestedInsideNotAndCase() {
        String query = "FROM main | WHERE CASE(NOT(x IN (FROM sub)), true, false)";
        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction caseFunc = as(filter.condition(), UnresolvedFunction.class);
        assertEquals("CASE", caseFunc.name());
        Not not = as(caseFunc.children().get(0), Not.class);
        InSubquery inSubquery = as(not.field(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), Attribute.class).name());
        assertEquals("sub", as(inSubquery.subquery(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    public void testWhereInSubqueryInsideLambda() {
        checkLambda();
        String query = "FROM main | WHERE filter(a, x -> x IN (FROM sub))";
        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction filterFunc = as(filter.condition(), UnresolvedFunction.class);
        assertEquals("filter", filterFunc.name());
        // Lambda
        Lambda lambda = as(filterFunc.children().get(1), Lambda.class);
        InSubquery inSubquery = as(lambda.body(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), Attribute.class).name());
        assertEquals("sub", as(inSubquery.subquery(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    public void testWhereInSubqueryInsideCoalesceInsideLambda() {
        checkLambda();
        String query = "FROM main | WHERE filter(a, x -> COALESCE(x IN (FROM sub), false))";
        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction filterFunc = as(filter.condition(), UnresolvedFunction.class);
        assertEquals("filter", filterFunc.name());
        // Lambda
        Lambda lambda = as(filterFunc.children().get(1), Lambda.class);
        // COALESCE
        UnresolvedFunction coalesceFunc = as(lambda.body(), UnresolvedFunction.class);
        assertEquals("COALESCE", coalesceFunc.name());
        // InSubquery
        InSubquery inSubquery = as(coalesceFunc.children().get(0), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), Attribute.class).name());
        assertEquals("sub", as(inSubquery.subquery(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    // ---- WHERE IN subquery negative tests ----

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

    // ---- multi-column IN subquery tests ----

    /*
     * Filter[MultiColumnInSubquery[[?emp_no, ?salary], Keep[UnresolvedRelation[employees]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testMultiColumnInSubquery() {
        checkMultiColumnInSubquery();
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | WHERE (emp_no, salary) " + notClause + "IN (FROM sub_index | KEEP emp_no, salary)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Expression condition = filter.condition();
        MultiColumnInSubquery mcs;
        if (negated) {
            Not not = as(condition, Not.class);
            mcs = as(not.field(), MultiColumnInSubquery.class);
        } else {
            mcs = as(condition, MultiColumnInSubquery.class);
        }
        assertThat(mcs.values().size(), equalTo(2));
        Keep keep = as(mcs.subquery(), Keep.class);
        UnresolvedRelation relation = as(keep.child(), UnresolvedRelation.class);
        assertEquals("sub_index", relation.indexPattern().indexPattern());
        relation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", relation.indexPattern().indexPattern());
    }

    /*
     * Filter[MultiColumnInSubquery[[?emp_no, ?salary, ?hire_date], Keep[UnresolvedRelation[employees]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testMultiColumnInSubqueryThreeColumns() {
        checkMultiColumnInSubquery();
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | WHERE (emp_no, salary, hire_date) "
            + notClause
            + "IN (FROM sub_index | KEEP emp_no, salary, hire_date)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Expression condition = filter.condition();
        MultiColumnInSubquery mcs;
        if (negated) {
            Not not = as(condition, Not.class);
            mcs = as(not.field(), MultiColumnInSubquery.class);
        } else {
            mcs = as(condition, MultiColumnInSubquery.class);
        }
        assertThat(mcs.values().size(), equalTo(3));
        Keep keep = as(mcs.subquery(), Keep.class);
        UnresolvedRelation relation = as(keep.child(), UnresolvedRelation.class);
        assertEquals("sub_index", relation.indexPattern().indexPattern());
        relation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", relation.indexPattern().indexPattern());
    }

    /*
     * Filter[InSubquery[[?emp_no], Keep[UnresolvedRelation[employees]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testMultiColumnInSubquerySingleColumn() {
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | WHERE (emp_no) " + notClause + "IN (FROM sub_index | KEEP emp_no)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Expression condition = filter.condition();
        InSubquery inSubquery;
        if (negated) {
            Not not = as(condition, Not.class);
            inSubquery = as(not.field(), InSubquery.class);
        } else {
            inSubquery = as(condition, InSubquery.class);
        }
        Keep keep = as(inSubquery.subquery(), Keep.class);
        UnresolvedRelation relation = as(keep.child(), UnresolvedRelation.class);
        assertEquals("sub_index", relation.indexPattern().indexPattern());
        relation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", relation.indexPattern().indexPattern());
    }

    // ---- mixed single-column and multi-column IN subquery tests ----

    /*
     * Filter[And[InSubquery[?x, Keep[UnresolvedRelation[sub1]]],
     *            (NOT) MultiColumnInSubquery[[?f1, ?f2], Keep[UnresolvedRelation[sub2]]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testMixedSingleAndMultiColumnInSubqueryWithAnd() {
        checkMultiColumnInSubquery();
        boolean multiNegated = randomBoolean();
        String notClause = multiNegated ? "NOT " : "";
        String query = "FROM main_index | WHERE x IN (FROM sub1 | KEEP a) AND (f1, f2) " + notClause + "IN (FROM sub2 | KEEP f1, f2)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        And and = as(filter.condition(), And.class);

        InSubquery inSubquery = as(and.left(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), Attribute.class).name());
        assertEquals("sub1", as(as(inSubquery.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());

        MultiColumnInSubquery mcs;
        if (multiNegated) {
            mcs = as(as(and.right(), Not.class).field(), MultiColumnInSubquery.class);
        } else {
            mcs = as(and.right(), MultiColumnInSubquery.class);
        }
        assertEquals(2, mcs.values().size());
        assertEquals("sub2", as(as(mcs.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());

        assertEquals("main_index", as(filter.child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /*
     * Filter[Or[(NOT) MultiColumnInSubquery[[?f1, ?f2], Keep[UnresolvedRelation[sub1]]],
     *           (NOT) InSubquery[?y, Keep[UnresolvedRelation[sub2]]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testMixedMultiAndSingleColumnInSubqueryWithOr() {
        checkMultiColumnInSubquery();
        boolean multiNegated = randomBoolean();
        boolean singleNegated = randomBoolean();
        String multiNot = multiNegated ? "NOT " : "";
        String singleNot = singleNegated ? "NOT " : "";
        String query = "FROM main_index | WHERE (f1, f2) "
            + multiNot
            + "IN (FROM sub1 | KEEP f1, f2) OR y "
            + singleNot
            + "IN (FROM sub2 | KEEP b)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Or or = as(filter.condition(), Or.class);

        MultiColumnInSubquery mcs;
        if (multiNegated) {
            mcs = as(as(or.left(), Not.class).field(), MultiColumnInSubquery.class);
        } else {
            mcs = as(or.left(), MultiColumnInSubquery.class);
        }
        assertEquals(2, mcs.values().size());
        assertEquals("sub1", as(as(mcs.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());

        InSubquery inSubquery;
        if (singleNegated) {
            inSubquery = as(as(or.right(), Not.class).field(), InSubquery.class);
        } else {
            inSubquery = as(or.right(), InSubquery.class);
        }
        assertEquals("y", as(inSubquery.value(), Attribute.class).name());
        assertEquals("sub2", as(as(inSubquery.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());

        assertEquals("main_index", as(filter.child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /*
     * Filter[Or[And[InSubquery[?x, Keep[UnresolvedRelation[sub1]]],
     *               Not[MultiColumnInSubquery[[?f1, ?f2], Keep[UnresolvedRelation[sub2]]]]],
     *           InSubquery[?y, Keep[UnresolvedRelation[sub3]]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testMixedSingleAndMultiColumnInSubqueryWithAndOr() {
        checkMultiColumnInSubquery();
        String query = """
            FROM main_index
            | WHERE x IN (FROM sub1 | KEEP a)
              AND (f1, f2) NOT IN (FROM sub2 | KEEP f1, f2)
              OR y IN (FROM sub3 | KEEP b)
            """;

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Or or = as(filter.condition(), Or.class);
        And and = as(or.left(), And.class);

        InSubquery firstIn = as(and.left(), InSubquery.class);
        assertEquals("x", as(firstIn.value(), Attribute.class).name());
        assertEquals("sub1", as(as(firstIn.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());

        MultiColumnInSubquery mcs = as(as(and.right(), Not.class).field(), MultiColumnInSubquery.class);
        assertEquals(2, mcs.values().size());
        assertEquals("sub2", as(as(mcs.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());

        InSubquery thirdIn = as(or.right(), InSubquery.class);
        assertEquals("y", as(thirdIn.value(), Attribute.class).name());
        assertEquals("sub3", as(as(thirdIn.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());

        assertEquals("main_index", as(filter.child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /*
     * Precedence: AND binds tighter than OR, resulting in:
     * Filter[Or[And[And[Not[MultiColumnInSubquery[2]], InSubquery[?x]], MultiColumnInSubquery[3]],
     *           Not[InSubquery[?y]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testMixedSubqueryChainWithAndOr() {
        checkMultiColumnInSubquery();
        String query = """
            FROM main_index
            | WHERE (f1, f2) NOT IN (FROM sub1 | KEEP f1, f2)
              AND x IN (FROM sub2 | KEEP a)
              AND (f1, f2, f3) IN (FROM sub3 | KEEP f1, f2, f3)
              OR y NOT IN (FROM sub4 | KEEP b)
            """;

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Or or = as(filter.condition(), Or.class);

        Not outerNot = as(or.right(), Not.class);
        InSubquery lastIn = as(outerNot.field(), InSubquery.class);
        assertEquals("y", as(lastIn.value(), Attribute.class).name());
        assertEquals("sub4", as(as(lastIn.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());

        And outerAnd = as(or.left(), And.class);

        MultiColumnInSubquery mcs3 = as(outerAnd.right(), MultiColumnInSubquery.class);
        assertEquals(3, mcs3.values().size());
        assertEquals("sub3", as(as(mcs3.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());

        And innerAnd = as(outerAnd.left(), And.class);
        MultiColumnInSubquery mcs1 = as(as(innerAnd.left(), Not.class).field(), MultiColumnInSubquery.class);
        assertEquals(2, mcs1.values().size());
        assertEquals("sub1", as(as(mcs1.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());

        InSubquery xIn = as(innerAnd.right(), InSubquery.class);
        assertEquals("x", as(xIn.value(), Attribute.class).name());
        assertEquals("sub2", as(as(xIn.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());

        assertEquals("main_index", as(filter.child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    // ---- nested multi-column IN subquery tests ----

    /*
     * Filter[(NOT) MultiColumnInSubquery[[?f1, ?f2],
     *   Keep[Filter[(NOT) MultiColumnInSubquery[[?g1, ?g2], Keep[UnresolvedRelation[sub2]]]]
     *              [UnresolvedRelation[sub1]]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testNestedMultiColumnInSubqueryInsideMultiColumnInSubquery() {
        checkMultiColumnInSubquery();
        boolean outerNegated = randomBoolean();
        boolean innerNegated = randomBoolean();
        String outerNot = outerNegated ? "NOT " : "";
        String innerNot = innerNegated ? "NOT " : "";
        String query = "FROM main_index | WHERE (f1, f2) "
            + outerNot
            + "IN (FROM sub1 | WHERE (g1, g2) "
            + innerNot
            + "IN (FROM sub2 | KEEP g1, g2) | KEEP f1, f2)";

        LogicalPlan plan = query(query);
        Filter outerFilter = as(plan, Filter.class);

        MultiColumnInSubquery outerMcs;
        if (outerNegated) {
            outerMcs = as(as(outerFilter.condition(), Not.class).field(), MultiColumnInSubquery.class);
        } else {
            outerMcs = as(outerFilter.condition(), MultiColumnInSubquery.class);
        }
        assertEquals(2, outerMcs.values().size());
        assertEquals("f1", as(outerMcs.values().get(0), Attribute.class).name());
        assertEquals("f2", as(outerMcs.values().get(1), Attribute.class).name());

        Keep outerKeep = as(outerMcs.subquery(), Keep.class);
        Filter innerFilter = as(outerKeep.child(), Filter.class);

        MultiColumnInSubquery innerMcs;
        if (innerNegated) {
            innerMcs = as(as(innerFilter.condition(), Not.class).field(), MultiColumnInSubquery.class);
        } else {
            innerMcs = as(innerFilter.condition(), MultiColumnInSubquery.class);
        }
        assertEquals(2, innerMcs.values().size());
        assertEquals("g1", as(innerMcs.values().get(0), Attribute.class).name());
        assertEquals("g2", as(innerMcs.values().get(1), Attribute.class).name());
        assertEquals("sub2", as(as(innerMcs.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("sub1", as(innerFilter.child(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main_index", as(outerFilter.child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /*
     * Filter[(NOT) MultiColumnInSubquery[[?f1, ?f2],
     *   Keep[Filter[(NOT) InSubquery[?x, Keep[UnresolvedRelation[sub2]]]]
     *              [UnresolvedRelation[sub1]]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testNestedSingleColumnInSubqueryInsideMultiColumnInSubquery() {
        checkMultiColumnInSubquery();
        boolean outerNegated = randomBoolean();
        boolean innerNegated = randomBoolean();
        String outerNot = outerNegated ? "NOT " : "";
        String innerNot = innerNegated ? "NOT " : "";
        String query = "FROM main_index | WHERE (f1, f2) "
            + outerNot
            + "IN (FROM sub1 | WHERE x "
            + innerNot
            + "IN (FROM sub2 | KEEP b) | KEEP f1, f2)";

        LogicalPlan plan = query(query);
        Filter outerFilter = as(plan, Filter.class);

        MultiColumnInSubquery outerMcs;
        if (outerNegated) {
            outerMcs = as(as(outerFilter.condition(), Not.class).field(), MultiColumnInSubquery.class);
        } else {
            outerMcs = as(outerFilter.condition(), MultiColumnInSubquery.class);
        }
        assertEquals(2, outerMcs.values().size());
        assertEquals("f1", as(outerMcs.values().get(0), Attribute.class).name());
        assertEquals("f2", as(outerMcs.values().get(1), Attribute.class).name());

        Keep outerKeep = as(outerMcs.subquery(), Keep.class);
        Filter innerFilter = as(outerKeep.child(), Filter.class);

        InSubquery innerIn;
        if (innerNegated) {
            innerIn = as(as(innerFilter.condition(), Not.class).field(), InSubquery.class);
        } else {
            innerIn = as(innerFilter.condition(), InSubquery.class);
        }
        assertEquals("x", as(innerIn.value(), Attribute.class).name());
        assertEquals("sub2", as(as(innerIn.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("sub1", as(innerFilter.child(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main_index", as(outerFilter.child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /*
     * Filter[(NOT) InSubquery[?x,
     *   Keep[Filter[(NOT) MultiColumnInSubquery[[?g1, ?g2], Keep[UnresolvedRelation[sub2]]]]
     *              [UnresolvedRelation[sub1]]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testNestedMultiColumnInSubqueryInsideSingleColumnInSubquery() {
        checkMultiColumnInSubquery();
        boolean outerNegated = randomBoolean();
        boolean innerNegated = randomBoolean();
        String outerNot = outerNegated ? "NOT " : "";
        String innerNot = innerNegated ? "NOT " : "";
        String query = "FROM main_index | WHERE x "
            + outerNot
            + "IN (FROM sub1 | WHERE (g1, g2) "
            + innerNot
            + "IN (FROM sub2 | KEEP g1, g2) | KEEP a)";

        LogicalPlan plan = query(query);
        Filter outerFilter = as(plan, Filter.class);

        InSubquery outerIn;
        if (outerNegated) {
            outerIn = as(as(outerFilter.condition(), Not.class).field(), InSubquery.class);
        } else {
            outerIn = as(outerFilter.condition(), InSubquery.class);
        }
        assertEquals("x", as(outerIn.value(), Attribute.class).name());

        Keep outerKeep = as(outerIn.subquery(), Keep.class);
        Filter innerFilter = as(outerKeep.child(), Filter.class);

        MultiColumnInSubquery innerMcs;
        if (innerNegated) {
            innerMcs = as(as(innerFilter.condition(), Not.class).field(), MultiColumnInSubquery.class);
        } else {
            innerMcs = as(innerFilter.condition(), MultiColumnInSubquery.class);
        }
        assertEquals(2, innerMcs.values().size());
        assertEquals("g1", as(innerMcs.values().get(0), Attribute.class).name());
        assertEquals("g2", as(innerMcs.values().get(1), Attribute.class).name());
        assertEquals("sub2", as(as(innerMcs.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("sub1", as(innerFilter.child(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main_index", as(outerFilter.child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /*
     * {@code FROM main | WHERE CASE((a, b) IN (FROM sub), true, false)}
     */
    public void testMultiColumnInSubqueryInCaseFunction() {
        checkMultiColumnInSubquery();
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | WHERE CASE((a, b) " + notClause + "IN (FROM sub_index | KEEP a, b), true, false)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction caseFunc = as(filter.condition(), UnresolvedFunction.class);
        assertEquals("CASE", caseFunc.name());

        MultiColumnInSubquery mcs;
        if (negated) {
            Not not = as(caseFunc.children().get(0), Not.class);
            mcs = as(not.field(), MultiColumnInSubquery.class);
        } else {
            mcs = as(caseFunc.children().get(0), MultiColumnInSubquery.class);
        }
        assertEquals(2, mcs.values().size());
        assertEquals("a", as(mcs.values().get(0), Attribute.class).name());
        assertEquals("b", as(mcs.values().get(1), Attribute.class).name());
        assertEquals("sub_index", as(as(mcs.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main_index", as(filter.child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /*
     * {@code FROM main | WHERE COALESCE((a, b) IN (FROM sub), false)}
     */
    public void testMultiColumnInSubqueryInCoalesceFunction() {
        checkMultiColumnInSubquery();
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | WHERE COALESCE((a, b) " + notClause + "IN (FROM sub_index | KEEP a, b), false)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction coalesceFunc = as(filter.condition(), UnresolvedFunction.class);
        assertEquals("COALESCE", coalesceFunc.name());

        MultiColumnInSubquery mcs;
        if (negated) {
            Not not = as(coalesceFunc.children().get(0), Not.class);
            mcs = as(not.field(), MultiColumnInSubquery.class);
        } else {
            mcs = as(coalesceFunc.children().get(0), MultiColumnInSubquery.class);
        }
        assertEquals(2, mcs.values().size());
        assertEquals("sub_index", as(as(mcs.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main_index", as(filter.child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /*
     * {@code WHERE ((a, b) IN (FROM sub)) IS NULL}
     */
    public void testMultiColumnInSubqueryWithIsNull() {
        checkMultiColumnInSubquery();
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | WHERE ((a, b) " + notClause + "IN (FROM sub_index | KEEP a, b)) IS NULL";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        IsNull isNull = as(filter.condition(), IsNull.class);
        MultiColumnInSubquery mcs;
        if (negated) {
            Not not = as(isNull.field(), Not.class);
            mcs = as(not.field(), MultiColumnInSubquery.class);
        } else {
            mcs = as(isNull.field(), MultiColumnInSubquery.class);
        }
        assertEquals(2, mcs.values().size());
        assertEquals("sub_index", as(as(mcs.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main_index", as(filter.child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /*
     * {@code WHERE ((a, b) IN (FROM sub)) IS NOT NULL}
     */
    public void testMultiColumnInSubqueryWithIsNotNull() {
        checkMultiColumnInSubquery();
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | WHERE ((a, b) " + notClause + "IN (FROM sub_index | KEEP a, b)) IS NOT NULL";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        IsNotNull isNotNull = as(filter.condition(), IsNotNull.class);
        MultiColumnInSubquery mcs;
        if (negated) {
            Not not = as(isNotNull.field(), Not.class);
            mcs = as(not.field(), MultiColumnInSubquery.class);
        } else {
            mcs = as(isNotNull.field(), MultiColumnInSubquery.class);
        }
        assertEquals(2, mcs.values().size());
        assertEquals("sub_index", as(as(mcs.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main_index", as(filter.child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /*
     * {@code WHERE (CASE((a, b) IN (FROM sub), 1, 0) + 1) == 2}
     */
    public void testMultiColumnInSubqueryNestedInsideCaseAddAndEquals() {
        checkMultiColumnInSubquery();
        String query = "FROM main | WHERE (CASE((a, b) IN (FROM sub | KEEP a, b), 1, 0) + 1) == 2";
        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Equals equals = as(filter.condition(), Equals.class);
        Add add = as(equals.left(), Add.class);
        UnresolvedFunction caseFunc = as(add.left(), UnresolvedFunction.class);
        assertEquals("CASE", caseFunc.name());
        MultiColumnInSubquery mcs = as(caseFunc.children().get(0), MultiColumnInSubquery.class);
        assertEquals(2, mcs.values().size());
        assertEquals("sub", as(as(mcs.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(filter.child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /*
     * {@code WHERE (CASE(COALESCE((a, b) IN (FROM sub), false), 1, 0) + 1) == 2}
     */
    public void testMultiColumnInSubqueryNestedInsideCoalesceAddAndEquals() {
        checkMultiColumnInSubquery();
        String query = "FROM main | WHERE (CASE(COALESCE((a, b) IN (FROM sub | KEEP a, b), false), 1, 0) + 1) == 2";
        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Equals equals = as(filter.condition(), Equals.class);
        Add add = as(equals.left(), Add.class);
        UnresolvedFunction caseFunc = as(add.left(), UnresolvedFunction.class);
        assertEquals("CASE", caseFunc.name());
        UnresolvedFunction coalesceFunc = as(caseFunc.children().get(0), UnresolvedFunction.class);
        assertEquals("COALESCE", coalesceFunc.name());
        MultiColumnInSubquery mcs = as(coalesceFunc.children().get(0), MultiColumnInSubquery.class);
        assertEquals(2, mcs.values().size());
        assertEquals("sub", as(as(mcs.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(filter.child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /*
     * {@code WHERE CASE((a, b) IN (FROM sub), 1, 0) != 0}
     */
    public void testMultiColumnInSubqueryNestedInsideCaseAndNotEquals() {
        checkMultiColumnInSubquery();
        String query = "FROM main | WHERE CASE((a, b) IN (FROM sub | KEEP a, b), 1, 0) != 0";
        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Not not = as(filter.condition(), Not.class);
        Equals equals = as(not.field(), Equals.class);
        UnresolvedFunction caseFunc = as(equals.left(), UnresolvedFunction.class);
        assertEquals("CASE", caseFunc.name());
        MultiColumnInSubquery mcs = as(caseFunc.children().get(0), MultiColumnInSubquery.class);
        assertEquals(2, mcs.values().size());
        assertEquals("sub", as(as(mcs.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(filter.child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /*
     * {@code WHERE CASE(NOT((a, b) IN (FROM sub)), true, false)}
     */
    public void testMultiColumnInSubqueryNestedInsideNotAndCase() {
        checkMultiColumnInSubquery();
        String query = "FROM main | WHERE CASE(NOT((a, b) IN (FROM sub | KEEP a, b)), true, false)";
        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction caseFunc = as(filter.condition(), UnresolvedFunction.class);
        assertEquals("CASE", caseFunc.name());
        Not not = as(caseFunc.children().get(0), Not.class);
        MultiColumnInSubquery mcs = as(not.field(), MultiColumnInSubquery.class);
        assertEquals(2, mcs.values().size());
        assertEquals("sub", as(as(mcs.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(filter.child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /*
     * {@code WHERE filter(a, x -> (x, b) IN (FROM sub))}
     */
    public void testWhereMultiColumnInSubqueryInsideLambda() {
        checkLambda();
        checkMultiColumnInSubquery();
        String query = "FROM main | WHERE filter(a, x -> (x, b) IN (FROM sub | KEEP x, b))";
        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction filterFunc = as(filter.condition(), UnresolvedFunction.class);
        assertEquals("filter", filterFunc.name());
        // Lambda
        Lambda lambda = as(filterFunc.children().get(1), Lambda.class);
        MultiColumnInSubquery mcs = as(lambda.body(), MultiColumnInSubquery.class);
        assertEquals(2, mcs.values().size());
        assertEquals("x", as(mcs.values().get(0), UnresolvedAttribute.class).name());
        assertEquals("b", as(mcs.values().get(1), UnresolvedAttribute.class).name());
        assertEquals("sub", as(as(mcs.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(filter.child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /*
     * {@code WHERE filter(a, x -> COALESCE((x, b) IN (FROM sub), false))}
     */
    public void testWhereMultiColumnInSubqueryInsideCoalesceInsideLambda() {
        checkLambda();
        checkMultiColumnInSubquery();
        String query = "FROM main | WHERE filter(a, x -> COALESCE((x, b) IN (FROM sub | KEEP x, b), false))";
        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        UnresolvedFunction filterFunc = as(filter.condition(), UnresolvedFunction.class);
        assertEquals("filter", filterFunc.name());
        // Lambda
        Lambda lambda = as(filterFunc.children().get(1), Lambda.class);
        // COALESCE
        UnresolvedFunction coalesceFunc = as(lambda.body(), UnresolvedFunction.class);
        assertEquals("COALESCE", coalesceFunc.name());
        // MultiColumnInSubquery
        MultiColumnInSubquery mcs = as(coalesceFunc.children().get(0), MultiColumnInSubquery.class);
        assertEquals(2, mcs.values().size());
        assertEquals("sub", as(as(mcs.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());
        assertEquals("main", as(filter.child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

    /*
     * Multi-column IN subquery inside CASE connected with AND:
     * {@code WHERE CASE((a, b) IN (FROM sub), true, false) AND c > 0}
     */
    public void testMultiColumnInSubqueryInCaseWithAnd() {
        checkMultiColumnInSubquery();
        String query = "FROM main | WHERE CASE((a, b) IN (FROM sub | KEEP a, b), true, false) AND c > 0";
        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        And and = as(filter.condition(), And.class);
        UnresolvedFunction caseFunc = as(and.left(), UnresolvedFunction.class);
        assertEquals("CASE", caseFunc.name());
        as(and.right(), GreaterThan.class);
        MultiColumnInSubquery mcs = as(caseFunc.children().get(0), MultiColumnInSubquery.class);
        assertEquals(2, mcs.values().size());
    }

    /*
     * Multi-column IN subquery inside COALESCE connected with OR:
     * {@code WHERE COALESCE((a, b) IN (FROM sub), false) OR c < 0}
     */
    public void testMultiColumnInSubqueryInCoalesceWithOr() {
        checkMultiColumnInSubquery();
        String query = "FROM main | WHERE COALESCE((a, b) IN (FROM sub | KEEP a, b), false) OR c < 0";
        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Or or = as(filter.condition(), Or.class);
        UnresolvedFunction coalesceFunc = as(or.left(), UnresolvedFunction.class);
        assertEquals("COALESCE", coalesceFunc.name());
        as(or.right(), LessThan.class);
        MultiColumnInSubquery mcs = as(coalesceFunc.children().get(0), MultiColumnInSubquery.class);
        assertEquals(2, mcs.values().size());
    }

    /*
     * Multi-column IN subquery with IS NULL and other predicates:
     * {@code WHERE (a > 0 AND ((b, c) IN (FROM sub1) IS NULL)) OR ((d, e) IN (FROM sub2) IS NOT NULL AND f < 0)}
     */
    public void testComplexBooleanWithMultiColumnInSubqueryAndNullPredicates() {
        checkMultiColumnInSubquery();
        String query = """
            FROM main
            | WHERE (a > 0 AND ((b, c) IN (FROM sub1 | KEEP b, c)) IS NULL)
                OR (((d, e) IN (FROM sub2 | KEEP d, e)) IS NOT NULL AND f < 0)""";
        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Or topOr = as(filter.condition(), Or.class);

        // Left side of OR: (a > 0 AND (((b, c) IN (FROM sub1) IS NULL)))
        And leftAnd = as(topOr.left(), And.class);
        as(leftAnd.left(), GreaterThan.class);
        IsNull isNull = as(leftAnd.right(), IsNull.class);
        MultiColumnInSubquery mcs1 = as(isNull.field(), MultiColumnInSubquery.class);
        assertEquals("sub1", as(as(mcs1.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());

        // Right side of OR: (((d, e) IN (FROM sub2) IS NOT NULL) AND f < 0)
        And rightAnd = as(topOr.right(), And.class);
        IsNotNull isNotNull = as(rightAnd.left(), IsNotNull.class);
        as(rightAnd.right(), LessThan.class);
        MultiColumnInSubquery mcs2 = as(isNotNull.field(), MultiColumnInSubquery.class);
        assertEquals("sub2", as(as(mcs2.subquery(), Keep.class).child(), UnresolvedRelation.class).indexPattern().indexPattern());
    }

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

    /*
     * {@code FROM main | EVAL is_match = (f1, f2) IN (FROM sub | KEEP f1, f2)}
     *
     * Eval[is_match = (NOT) MultiColumnInSubquery[[?f1, ?f2], Keep[UnresolvedRelation[sub_index]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testEvalWithMultiColumnInSubquery() {
        checkMultiColumnInSubquery();
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | EVAL is_match = (f1, f2) " + notClause + "IN (FROM sub_index | KEEP f1, f2)";

        LogicalPlan plan = query(query);
        Eval eval = as(plan, Eval.class);
        assertEquals(1, eval.fields().size());
        Alias alias = eval.fields().get(0);
        assertEquals("is_match", alias.name());

        MultiColumnInSubquery mcs;
        if (negated) {
            Not not = as(alias.child(), Not.class);
            mcs = as(not.field(), MultiColumnInSubquery.class);
        } else {
            mcs = as(alias.child(), MultiColumnInSubquery.class);
        }
        assertThat(mcs.values().size(), equalTo(2));
        assertEquals("f1", as(mcs.values().get(0), Attribute.class).name());
        assertEquals("f2", as(mcs.values().get(1), Attribute.class).name());

        Keep keep = as(mcs.subquery(), Keep.class);
        UnresolvedRelation subqueryRelation = as(keep.child(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());

        UnresolvedRelation mainRelation = as(eval.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /*
     * {@code FROM main | EVAL is_match = CASE((f1, f2) IN (FROM sub), true, false)}
     *
     * Eval[is_match = CASE((NOT) MultiColumnInSubquery[[?f1, ?f2], UnresolvedRelation[sub_index]], true, false)]
     * \_UnresolvedRelation[main_index]
     */
    public void testEvalWithMultiColumnInSubqueryNestedInCase() {
        checkMultiColumnInSubquery();
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | EVAL is_match = CASE((f1, f2) " + notClause + "IN (FROM sub_index | KEEP f1, f2), true, false)";

        LogicalPlan plan = query(query);
        Eval eval = as(plan, Eval.class);
        Alias alias = eval.fields().get(0);
        UnresolvedFunction caseFunc = as(alias.child(), UnresolvedFunction.class);
        assertEquals("CASE", caseFunc.name());

        MultiColumnInSubquery mcs;
        if (negated) {
            Not not = as(caseFunc.children().get(0), Not.class);
            mcs = as(not.field(), MultiColumnInSubquery.class);
        } else {
            mcs = as(caseFunc.children().get(0), MultiColumnInSubquery.class);
        }
        assertThat(mcs.values().size(), equalTo(2));
    }

    /*
     * {@code FROM main | EVAL is_match = COALESCE((f1, f2) IN (FROM sub), false)}
     *
     * Eval[is_match = COALESCE((NOT) MultiColumnInSubquery[[?f1, ?f2], UnresolvedRelation[sub_index]], false)]
     * \_UnresolvedRelation[main_index]
     */
    public void testEvalWithMultiColumnInSubqueryNestedInCoalesce() {
        checkMultiColumnInSubquery();
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | EVAL is_match = COALESCE((f1, f2) " + notClause + "IN (FROM sub_index | KEEP f1, f2), false)";

        LogicalPlan plan = query(query);
        Eval eval = as(plan, Eval.class);
        Alias alias = eval.fields().get(0);
        UnresolvedFunction coalesce = as(alias.child(), UnresolvedFunction.class);
        assertEquals("COALESCE", coalesce.name());

        MultiColumnInSubquery mcs;
        if (negated) {
            Not not = as(coalesce.children().get(0), Not.class);
            mcs = as(not.field(), MultiColumnInSubquery.class);
        } else {
            mcs = as(coalesce.children().get(0), MultiColumnInSubquery.class);
        }
        assertThat(mcs.values().size(), equalTo(2));
    }

    /*
     * {@code FROM main | EVAL is_match = ISNULL((f1, f2) IN (FROM sub))}
     *
     * Eval[is_match = ISNULL((NOT) MultiColumnInSubquery[[?f1, ?f2], UnresolvedRelation[sub_index]])]
     * \_UnresolvedRelation[main_index]
     */
    public void testEvalWithMultiColumnInSubqueryNestedInIsNull() {
        checkMultiColumnInSubquery();
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | EVAL is_match = ISNULL((f1, f2) " + notClause + "IN (FROM sub_index | KEEP f1, f2))";

        LogicalPlan plan = query(query);
        Eval eval = as(plan, Eval.class);
        Alias alias = eval.fields().get(0);
        UnresolvedFunction isnull = as(alias.child(), UnresolvedFunction.class);
        assertEquals("ISNULL", isnull.name());

        MultiColumnInSubquery mcs;
        if (negated) {
            Not not = as(isnull.children().get(0), Not.class);
            mcs = as(not.field(), MultiColumnInSubquery.class);
        } else {
            mcs = as(isnull.children().get(0), MultiColumnInSubquery.class);
        }
        assertThat(mcs.values().size(), equalTo(2));
    }

    /*
     * {@code FROM main | EVAL is_match = ISNOTNULL((f1, f2) IN (FROM sub))}
     *
     * Eval[is_match = ISNOTNULL((NOT) MultiColumnInSubquery[[?f1, ?f2], UnresolvedRelation[sub_index]])]
     * \_UnresolvedRelation[main_index]
     */
    public void testEvalWithMultiColumnInSubqueryNestedInIsNotNull() {
        checkMultiColumnInSubquery();
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT " : "";
        String query = "FROM main_index | EVAL is_match = ISNOTNULL((f1, f2) " + notClause + "IN (FROM sub_index | KEEP f1, f2))";

        LogicalPlan plan = query(query);
        Eval eval = as(plan, Eval.class);
        Alias alias = eval.fields().get(0);
        UnresolvedFunction isnotnull = as(alias.child(), UnresolvedFunction.class);
        assertEquals("ISNOTNULL", isnotnull.name());

        MultiColumnInSubquery mcs;
        if (negated) {
            Not not = as(isnotnull.children().get(0), Not.class);
            mcs = as(not.field(), MultiColumnInSubquery.class);
        } else {
            mcs = as(isnotnull.children().get(0), MultiColumnInSubquery.class);
        }
        assertThat(mcs.values().size(), equalTo(2));
    }

    /*
     * {@code FROM main | EVAL is_match = x IN (TS sub)}
     *
     * Eval[is_match = InSubquery[?x, UnresolvedRelation[TS sub_index]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testEvalWithInTsSubquery() {
        String query = "FROM main_index | EVAL is_match = x IN (TS sub_index)";

        LogicalPlan plan = query(query);
        Eval eval = as(plan, Eval.class);
        Alias alias = eval.fields().get(0);
        InSubquery inSubquery = as(alias.child(), InSubquery.class);
        UnresolvedRelation subqueryRelation = as(inSubquery.subquery(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());
        assertEquals("TS", subqueryRelation.telemetryLabel());
    }

    /*
     * {@code FROM main | EVAL is_match = (f1, f2) IN (TS sub | KEEP f1, f2)}
     *
     * Eval[is_match = MultiColumnInSubquery[[?f1, ?f2], Keep[UnresolvedRelation[TS sub_index]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testEvalWithMultiColumnInTsSubquery() {
        checkMultiColumnInSubquery();
        String query = "FROM main_index | EVAL is_match = (f1, f2) IN (TS sub_index | KEEP f1, f2)";

        LogicalPlan plan = query(query);
        Eval eval = as(plan, Eval.class);
        Alias alias = eval.fields().get(0);
        MultiColumnInSubquery mcs = as(alias.child(), MultiColumnInSubquery.class);
        assertThat(mcs.values().size(), equalTo(2));
        Keep keep = as(mcs.subquery(), Keep.class);
        UnresolvedRelation subqueryRelation = as(keep.child(), UnresolvedRelation.class);
        assertEquals("sub_index", subqueryRelation.indexPattern().indexPattern());
        assertEquals("TS", subqueryRelation.telemetryLabel());
    }

    /*
     * {@code FROM main | EVAL is_match = x IN (ROW a = 1)}
     *
     * Eval[is_match = InSubquery[?x, Row[a = 1]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testEvalWithInRowSubquery() {
        String query = "FROM main_index | EVAL is_match = x IN (ROW a = 1)";

        LogicalPlan plan = query(query);
        Eval eval = as(plan, Eval.class);
        Alias alias = eval.fields().get(0);
        InSubquery inSubquery = as(alias.child(), InSubquery.class);
        Row row = as(inSubquery.subquery(), Row.class);
        assertEquals(1, row.fields().size());
        assertEquals("a", row.fields().get(0).name());
    }

    /*
     * {@code FROM main | EVAL is_match = (f1, f2) IN (ROW f1 = 1, f2 = 2)}
     *
     * Eval[is_match = MultiColumnInSubquery[[?f1, ?f2], Row[f1 = 1, f2 = 2]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testEvalWithMultiColumnInRowSubquery() {
        checkMultiColumnInSubquery();
        String query = "FROM main_index | EVAL is_match = (f1, f2) IN (ROW f1 = 1, f2 = 2)";

        LogicalPlan plan = query(query);
        Eval eval = as(plan, Eval.class);
        Alias alias = eval.fields().get(0);
        MultiColumnInSubquery mcs = as(alias.child(), MultiColumnInSubquery.class);
        assertThat(mcs.values().size(), equalTo(2));
        Row row = as(mcs.subquery(), Row.class);
        assertEquals(2, row.fields().size());
        assertEquals("f1", row.fields().get(0).name());
        assertEquals("f2", row.fields().get(1).name());
    }
}
