/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.plan.logical.ChangePoint;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.randomIndexPatterns;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.unquoteIndexPattern;
import static org.hamcrest.Matchers.containsString;

/**
 * Parser tests for subqueries whose source command is {@code ROW}.
 */
public class SubqueryWithRowCommandTests extends AbstractStatementParserTests {

    @Before
    public void requireSubqueryWithRowCommand() {
        assumeTrue("Requires subquery with row command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
    }

    private static void requireSubqueryInFromCommand() {
        assumeTrue("Requires subquery in from command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
    }

    private static void requireWhereInSubquery() {
        assumeTrue("Requires where in subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY.isEnabled());
    }

    /**
     * Single ROW subquery alongside an index pattern in the main FROM:
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_Row[[1[INTEGER] AS x]]
     */
    public void testIndexPatternWithRowSubquery() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (ROW x = 1)
            """, mainQueryIndexPattern);

        LogicalPlan plan = query(query);

        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), unresolvedRelation.indexPattern().indexPattern());

        Subquery subquery = as(children.get(1), Subquery.class);
        Row row = as(subquery.plan(), Row.class);
        assertRowField(row, "x", 1);
    }

    /**
     * ROW with multiple fields and different value types is parsed as a single {@link Row} leaf.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_Row[[1[INTEGER] AS a, 2[INTEGER] AS b, hello[KEYWORD] AS c]]
     */
    public void testIndexPatternWithRowSubqueryMultipleFields() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (ROW a = 1, b = 2, c = "hello")
            """, mainQueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), unresolvedRelation.indexPattern().indexPattern());

        Subquery subquery = as(children.get(1), Subquery.class);
        Row row = as(subquery.plan(), Row.class);
        assertEquals(3, row.fields().size());
        assertEquals("a", row.fields().get(0).name());
        assertEquals("b", row.fields().get(1).name());
        assertEquals("c", row.fields().get(2).name());
    }

    /**
     * Mix of an index pattern, a ROW subquery and a FROM subquery — the user-facing example.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * |_Subquery[]
     * | \_Filter[?x &gt; 1[INTEGER]]
     * |   \_Row[[1[INTEGER] AS x]]
     * \_Subquery[]
     *   \_Filter[?x &gt; 1[INTEGER]]
     *     \_UnresolvedRelation[]
     */
    public void testIndexPatternWithRowAndFromSubqueries() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        var subqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (ROW x = 1 | WHERE x > 1), (FROM {} | WHERE x > 1)
            """, mainQueryIndexPattern, subqueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(3, children.size());

        // main statement
        UnresolvedRelation mainRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), mainRelation.indexPattern().indexPattern());

        // ROW subquery
        Subquery rowSubquery = as(children.get(1), Subquery.class);
        Filter rowFilter = as(rowSubquery.plan(), Filter.class);
        GreaterThan rowFilterCondition = as(rowFilter.condition(), GreaterThan.class);
        Attribute rowLeft = as(rowFilterCondition.left(), Attribute.class);
        assertEquals("x", rowLeft.name());
        Literal rowRight = as(rowFilterCondition.right(), Literal.class);
        assertEquals(1, rowRight.value());
        Row row = as(rowFilter.child(), Row.class);
        assertRowField(row, "x", 1);

        // FROM subquery
        Subquery fromSubquery = as(children.get(2), Subquery.class);
        Filter fromFilter = as(fromSubquery.plan(), Filter.class);
        UnresolvedRelation fromRelation = as(fromFilter.child(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(subqueryIndexPattern), fromRelation.indexPattern().indexPattern());
    }

    /**
     * A ROW subquery with several processing commands inside.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_Limit[5[INTEGER],false]
     *     \_Eval[[?x + 1[INTEGER] AS y]]
     *       \_Filter[?x &gt; 0[INTEGER]]
     *         \_Row[[1[INTEGER] AS x]]
     */
    public void testRowSubqueryWithProcessingCommandsInSubquery() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (ROW x = 1
                      | WHERE x > 0
                      | EVAL y = x + 1
                      | LIMIT 5)
            """, mainQueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        UnresolvedRelation mainRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), mainRelation.indexPattern().indexPattern());

        Subquery subquery = as(children.get(1), Subquery.class);
        Limit limit = as(subquery.plan(), Limit.class);
        Eval eval = as(limit.child(), Eval.class);
        Filter filter = as(eval.child(), Filter.class);
        Row row = as(filter.child(), Row.class);
        assertRowField(row, "x", 1);
    }

    /**
     * ROW subquery combined with processing commands in the main query.
     *
     * Limit[10[INTEGER],false]
     * \_Filter[?x &gt; 5[INTEGER]]
     *   \_UnionAll[[]]
     *     |_UnresolvedRelation[]
     *     \_Subquery[]
     *       \_Row[[1[INTEGER] AS x]]
     */
    public void testRowSubqueryWithProcessingCommandsInMainQuery() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (ROW x = 1)
            | WHERE x > 5
            | LIMIT 10
            """, mainQueryIndexPattern);

        LogicalPlan plan = query(query);
        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        UnresolvedRelation mainRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), mainRelation.indexPattern().indexPattern());

        Subquery subquery = as(children.get(1), Subquery.class);
        Row row = as(subquery.plan(), Row.class);
        assertRowField(row, "x", 1);
    }

    /**
     * Processing commands in both the ROW subquery and the main query.
     *
     * Limit[10[INTEGER],false]
     * \_Filter[?y &gt; 0[INTEGER]]
     *   \_UnionAll[[]]
     *     |_UnresolvedRelation[]
     *     \_Subquery[]
     *       \_Eval[[?x + 1[INTEGER] AS y]]
     *         \_Row[[1[INTEGER] AS x]]
     */
    public void testRowSubqueryWithProcessingCommandsInSubqueryAndMainQuery() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (ROW x = 1 | EVAL y = x + 1)
            | WHERE y > 0
            | LIMIT 10
            """, mainQueryIndexPattern);

        LogicalPlan plan = query(query);
        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        UnresolvedRelation mainRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), mainRelation.indexPattern().indexPattern());

        Subquery subquery = as(children.get(1), Subquery.class);
        Eval eval = as(subquery.plan(), Eval.class);
        Row row = as(eval.child(), Row.class);
        assertRowField(row, "x", 1);
    }

    /**
     * If the only child of FROM is a ROW subquery without an index pattern, the {@code UnionAll}
     * is collapsed and the {@link Row} is returned directly, mirroring the behavior for a single
     * FROM subquery in {@link SubqueryTests#testSubqueryOnly()}.
     *
     * Row[[1[INTEGER] AS x]]
     */
    public void testRowSubqueryOnly() {
        requireSubqueryInFromCommand();
        String query = "FROM (ROW x = 1)";
        LogicalPlan plan = query(query);
        Row row = as(plan, Row.class);
        assertRowField(row, "x", 1);
    }

    /**
     * Multiple ROW subqueries with no main index pattern produce a {@code UnionAll} of {@code Subquery}
     * over {@link Row}s.
     *
     * UnionAll[[]]
     * |_Subquery[]
     * | \_Row[[1[INTEGER] AS a]]
     * |_Subquery[]
     * | \_Row[[2[INTEGER] AS b]]
     * \_Subquery[]
     *   \_Row[[3[INTEGER] AS c]]
     */
    public void testMultipleRowSubqueriesOnly() {
        requireSubqueryInFromCommand();
        String query = "FROM (ROW a = 1), (ROW b = 2), (ROW c = 3)";

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(3, children.size());

        Subquery subquery1 = as(children.get(0), Subquery.class);
        assertRowField(as(subquery1.plan(), Row.class), "a", 1);

        Subquery subquery2 = as(children.get(1), Subquery.class);
        assertRowField(as(subquery2.plan(), Row.class), "b", 2);

        Subquery subquery3 = as(children.get(2), Subquery.class);
        assertRowField(as(subquery3.plan(), Row.class), "c", 3);
    }

    /**
     * A ROW subquery and a FROM subquery without a main index pattern.
     *
     * UnionAll[[]]
     * |_Subquery[]
     * | \_Row[[1[INTEGER] AS x]]
     * \_Subquery[]
     *   \_UnresolvedRelation[]
     */
    public void testRowAndFromSubqueriesOnly() {
        requireSubqueryInFromCommand();
        var subqueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM (ROW x = 1), (FROM {})
            """, subqueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        Subquery rowSubquery = as(children.get(0), Subquery.class);
        assertRowField(as(rowSubquery.plan(), Row.class), "x", 1);

        Subquery fromSubquery = as(children.get(1), Subquery.class);
        UnresolvedRelation unresolvedRelation = as(fromSubquery.plan(), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(subqueryIndexPattern), unresolvedRelation.indexPattern().indexPattern());
    }

    /**
     * A ROW subquery nested inside a FROM subquery.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_UnionAll[[]]
     *     |_UnresolvedRelation[]
     *     \_Subquery[]
     *       \_Row[[1[INTEGER] AS x]]
     */
    public void testRowSubqueryNestedInsideFromSubquery() {
        requireSubqueryInFromCommand();
        var outerIndexPattern = randomIndexPatterns();
        var innerIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (FROM {}, (ROW x = 1))
            """, outerIndexPattern, innerIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll outerUnion = as(plan, UnionAll.class);
        List<LogicalPlan> outerChildren = outerUnion.children();
        assertEquals(2, outerChildren.size());

        UnresolvedRelation outerRelation = as(outerChildren.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(outerIndexPattern), outerRelation.indexPattern().indexPattern());

        Subquery outerSubquery = as(outerChildren.get(1), Subquery.class);
        UnionAll innerUnion = as(outerSubquery.plan(), UnionAll.class);
        List<LogicalPlan> innerChildren = innerUnion.children();
        assertEquals(2, innerChildren.size());

        UnresolvedRelation innerRelation = as(innerChildren.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(innerIndexPattern), innerRelation.indexPattern().indexPattern());

        Subquery innerSubquery = as(innerChildren.get(1), Subquery.class);
        Row row = as(innerSubquery.plan(), Row.class);
        assertRowField(row, "x", 1);
    }

    /**
     * Verifies the parser accepts a ROW subquery whose trailing processing command sits in each of the
     * different ANTLR lexer modes the {@code processingCommand} rule can transition into. The shape of the
     * tree is asserted only at a high level since the goal is to ensure no parse errors occur.
     */
    public void testRowSubqueryEndsWithProcessingCommandsInDifferentMode() {
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
        for (String processingCommand : processingCommandInDifferentMode) {
            String query = LoggerMessageFormat.format(null, """
                FROM {}, (ROW x = 1 | {})
                | WHERE x > 0
                """, mainQueryIndexPattern, processingCommand);

            LogicalPlan plan = query(query);
            Filter filter = as(plan, Filter.class);
            UnionAll unionAll = as(filter.child(), UnionAll.class);
            List<LogicalPlan> children = unionAll.children();
            assertEquals(2, children.size());
            UnresolvedRelation mainRelation = as(children.get(0), UnresolvedRelation.class);
            assertEquals(unquoteIndexPattern(mainQueryIndexPattern), mainRelation.indexPattern().indexPattern());
            as(children.get(1), Subquery.class);
        }
    }

    /**
     * A ROW subquery whose single field is assigned a multivalue (list) of integers. The parser
     * stores the values inside a single {@link Literal} of type {@link DataType#INTEGER}, with the
     * value being a {@link List} of boxed integers.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_Row[[[1, 2, 3][INTEGER] AS x]]
     */
    public void testIndexPatternWithMultivalueIntRowSubquery() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (ROW x = [1, 2, 3])
            """, mainQueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), unresolvedRelation.indexPattern().indexPattern());

        Subquery subquery = as(children.get(1), Subquery.class);
        Row row = as(subquery.plan(), Row.class);
        assertMultivalueRowField(row, "x", DataType.INTEGER, List.of(1, 2, 3));
    }

    /**
     * A ROW subquery declaring multiple fields, each with a multivalue of a different element type
     * (integer, double, boolean, keyword string). All values end up wrapped in a single {@link Row}
     * leaf where each {@link Alias}'s child is a multivalue {@link Literal}.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_Row[[[1, 2][INTEGER] AS a, [1.5, -2.5][DOUBLE] AS b,
     *           [true, false, true][BOOLEAN] AS c, [cat, dog][KEYWORD] AS d]]
     */
    public void testMultivalueRowSubqueryMultipleFields() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (ROW a = [1, 2], b = [1.5, -2.5], c = [true, false, true], d = ["cat", "dog"])
            """, mainQueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), unresolvedRelation.indexPattern().indexPattern());

        Subquery subquery = as(children.get(1), Subquery.class);
        Row row = as(subquery.plan(), Row.class);
        assertEquals(4, row.fields().size());
        assertMultivalueAlias(row.fields().get(0), "a", DataType.INTEGER, List.of(1, 2));
        assertMultivalueAlias(row.fields().get(1), "b", DataType.DOUBLE, List.of(1.5, -2.5));
        assertMultivalueAlias(row.fields().get(2), "c", DataType.BOOLEAN, List.of(true, false, true));
        assertMultivalueAlias(row.fields().get(3), "d", DataType.KEYWORD, List.of(new BytesRef("cat"), new BytesRef("dog")));
    }

    /**
     * A single ROW subquery declaring a mix of scalar and multivalue fields. Each {@link Alias} keeps
     * the shape it was assigned: scalars stay as plain {@link Literal}s while multivalues are stored
     * as a single {@link Literal} whose value is a {@link List}.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_Row[[1[INTEGER] AS a, [10, 20, 30][INTEGER] AS b, hello[KEYWORD] AS c,
     *           [cat, dog][KEYWORD] AS d, true[BOOLEAN] AS e, [1.5, -2.5][DOUBLE] AS f]]
     */
    public void testRowSubqueryWithMixedScalarAndMultivalueFields() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (ROW a = 1, b = [10, 20, 30], c = "hello", d = ["cat", "dog"], e = true, f = [1.5, -2.5])
            """, mainQueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        UnresolvedRelation unresolvedRelation = as(children.get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), unresolvedRelation.indexPattern().indexPattern());

        Subquery subquery = as(children.get(1), Subquery.class);
        Row row = as(subquery.plan(), Row.class);
        assertEquals(6, row.fields().size());
        // Scalars: plain single-value Literal.
        assertScalarAlias(row.fields().get(0), "a", DataType.INTEGER, 1);
        // Multivalues: single Literal wrapping a List of values, with the appropriate DataType.
        assertMultivalueAlias(row.fields().get(1), "b", DataType.INTEGER, List.of(10, 20, 30));
        assertScalarAlias(row.fields().get(2), "c", DataType.KEYWORD, new BytesRef("hello"));
        assertMultivalueAlias(row.fields().get(3), "d", DataType.KEYWORD, List.of(new BytesRef("cat"), new BytesRef("dog")));
        assertScalarAlias(row.fields().get(4), "e", DataType.BOOLEAN, true);
        assertMultivalueAlias(row.fields().get(5), "f", DataType.DOUBLE, List.of(1.5, -2.5));
    }

    /**
     * Multivalue ROW subquery with widening element types: a literal larger than {@link Integer#MAX_VALUE}
     * promotes the whole list to {@link DataType#LONG}, mirroring the behaviour validated by
     * {@code StatementParserTests#testRowCommandMultivalueLongAndInt()}.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_Row[[[2147483648, 1][LONG] AS c]]
     */
    public void testMultivalueRowSubqueryWithWideningType() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (ROW c = [2147483648, 1])
            """, mainQueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        as(children.get(0), UnresolvedRelation.class);
        Subquery subquery = as(children.get(1), Subquery.class);
        Row row = as(subquery.plan(), Row.class);
        assertMultivalueRowField(row, "c", DataType.LONG, List.of(2147483648L, 1L));
    }

    /**
     * A multivalue ROW with processing commands inside the subquery — including {@code MV_EXPAND}
     * which is the typical consumer of multivalue fields. Verifies the parser keeps the multivalue
     * literal intact at the {@link Row} leaf and stacks the processing commands above it.
     *
     * UnionAll[[]]
     * |_UnresolvedRelation[]
     * \_Subquery[]
     *   \_Filter[?x &gt; 1[INTEGER]]
     *     \_MvExpand[?x,?x]
     *       \_Row[[[1, 2, 3][INTEGER] AS x]]
     */
    public void testMultivalueRowSubqueryWithProcessingCommands() {
        requireSubqueryInFromCommand();
        var mainQueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (ROW x = [1, 2, 3]
                      | MV_EXPAND x
                      | WHERE x > 1)
            """, mainQueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        as(children.get(0), UnresolvedRelation.class);
        Subquery subquery = as(children.get(1), Subquery.class);
        Filter filter = as(subquery.plan(), Filter.class);
        GreaterThan filterCondition = as(filter.condition(), GreaterThan.class);
        Attribute filterLeft = as(filterCondition.left(), Attribute.class);
        assertEquals("x", filterLeft.name());
        Literal filterRight = as(filterCondition.right(), Literal.class);
        assertEquals(1, filterRight.value());

        // The Row leaf sits below the MV_EXPAND that the inner pipeline produced.
        Row row = as(filter.child().children().get(0), Row.class);
        assertMultivalueRowField(row, "x", DataType.INTEGER, List.of(1, 2, 3));
    }

    /**
     * A multivalue ROW subquery alongside a scalar ROW subquery. Each subquery preserves the value
     * shape that was declared inside it.
     *
     * UnionAll[[]]
     * |_Subquery[]
     * | \_Row[[1[INTEGER] AS x]]
     * \_Subquery[]
     *   \_Row[[[10, 20, 30][INTEGER] AS x]]
     */
    public void testMixedScalarAndMultivalueRowSubqueries() {
        requireSubqueryInFromCommand();
        String query = "FROM (ROW x = 1), (ROW x = [10, 20, 30])";

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        List<LogicalPlan> children = unionAll.children();
        assertEquals(2, children.size());

        Subquery scalarSubquery = as(children.get(0), Subquery.class);
        assertRowField(as(scalarSubquery.plan(), Row.class), "x", 1);

        Subquery multivalueSubquery = as(children.get(1), Subquery.class);
        assertMultivalueRowField(as(multivalueSubquery.plan(), Row.class), "x", DataType.INTEGER, List.of(10, 20, 30));
    }

    /**
     * If the only child of FROM is a multivalue ROW subquery, the {@code UnionAll} is collapsed and
     * the {@link Row} is returned directly — same behaviour as the scalar case in
     * {@link #testRowSubqueryOnly()}.
     *
     * Row[[[cat, dog][KEYWORD] AS animals]]
     */
    public void testMultivalueRowSubqueryOnly() {
        requireSubqueryInFromCommand();
        String query = "FROM (ROW animals = [\"cat\", \"dog\"])";
        LogicalPlan plan = query(query);
        Row row = as(plan, Row.class);
        assertMultivalueRowField(row, "animals", DataType.KEYWORD, List.of(new BytesRef("cat"), new BytesRef("dog")));
    }

    /**
     * A {@code WHERE x IN (subquery)} where the subquery's source command is {@code ROW}. The
     * parser stores the subquery's plan directly on {@link InSubquery#subquery()} (no
     * {@link Subquery} wrapper), so the {@link Row} appears immediately under the
     * {@link InSubquery} expression.
     *
     * Filter[InSubquery[?x, Row[[1[INTEGER] AS x]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testWhereInSubqueryWithRowSource() {
        requireWhereInSubquery();
        String query = "FROM main_index | WHERE x IN (ROW x = 1)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), UnresolvedAttribute.class).name());

        Row row = as(inSubquery.subquery(), Row.class);
        assertRowField(row, "x", 1);

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * The negated form {@code WHERE x NOT IN (ROW x = 1)} wraps the {@link InSubquery} in a
     * {@link Not}; everything below the {@code NOT} matches the non-negated case.
     *
     * Filter[NOT(InSubquery[?x, Row[[1[INTEGER] AS x]]])]
     * \_UnresolvedRelation[main_index]
     */
    public void testWhereNotInSubqueryWithRowSource() {
        requireWhereInSubquery();
        String query = "FROM main_index | WHERE x NOT IN (ROW x = 1)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        Not not = as(filter.condition(), Not.class);
        InSubquery inSubquery = as(not.field(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), UnresolvedAttribute.class).name());

        Row row = as(inSubquery.subquery(), Row.class);
        assertRowField(row, "x", 1);

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * A {@code WHERE x IN (ROW x = 1 | WHERE x &gt; 0)}: a single trailing processing command
     * (another {@code WHERE}) is stacked on top of the ROW. The IN-subquery's plan is
     * {@code Filter -> Row}.
     *
     * Filter[InSubquery[?x, Filter[?x &gt; 0[INTEGER]] -> Row[[1[INTEGER] AS x]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testWhereInSubqueryWithRowSourceAndWhereInside() {
        requireWhereInSubquery();
        String query = "FROM main_index | WHERE x IN (ROW x = 1 | WHERE x > 0)";

        LogicalPlan plan = query(query);
        Filter outerFilter = as(plan, Filter.class);
        InSubquery inSubquery = as(outerFilter.condition(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), UnresolvedAttribute.class).name());

        Filter innerFilter = as(inSubquery.subquery(), Filter.class);
        GreaterThan condition = as(innerFilter.condition(), GreaterThan.class);
        assertEquals("x", as(condition.left(), Attribute.class).name());
        assertEquals(0, as(condition.right(), Literal.class).value());

        Row row = as(innerFilter.child(), Row.class);
        assertRowField(row, "x", 1);

        UnresolvedRelation mainRelation = as(outerFilter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * Several processing commands chained inside the IN-subquery on top of a ROW source. Verifies
     * each command is parsed and stacked in the expected order, with the ROW remaining at the leaf.
     *
     * Filter[InSubquery[?x,
     *   Limit[5,
     *     Keep[[?x],
     *       Eval[[?x + 1[INTEGER] AS y],
     *         Filter[?x &gt; 0[INTEGER],
     *           Row[[1[INTEGER] AS x]]]]]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testWhereInSubqueryWithRowSourceAndMultipleProcessingCommands() {
        requireWhereInSubquery();
        String query = """
            FROM main_index
            | WHERE x IN (ROW x = 1
                          | WHERE x > 0
                          | EVAL y = x + 1
                          | KEEP x
                          | LIMIT 5)
            """;

        LogicalPlan plan = query(query);
        Filter outerFilter = as(plan, Filter.class);
        InSubquery inSubquery = as(outerFilter.condition(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), UnresolvedAttribute.class).name());

        Limit limit = as(inSubquery.subquery(), Limit.class);
        assertEquals(5, as(limit.limit(), Literal.class).value());
        Keep keep = as(limit.child(), Keep.class);
        Eval eval = as(keep.child(), Eval.class);
        assertEquals("y", eval.fields().get(0).name());
        Filter innerFilter = as(eval.child(), Filter.class);
        GreaterThan condition = as(innerFilter.condition(), GreaterThan.class);
        assertEquals("x", as(condition.left(), Attribute.class).name());
        assertEquals(0, as(condition.right(), Literal.class).value());
        Row row = as(innerFilter.child(), Row.class);
        assertRowField(row, "x", 1);

        UnresolvedRelation mainRelation = as(outerFilter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * IN-subquery whose ROW source declares a multivalue list. The multivalue {@link Literal} is
     * preserved at the {@link Row} leaf and the {@link InSubquery} sees the bare {@link Row}.
     *
     * Filter[InSubquery[?x, Row[[[1, 2, 3][INTEGER] AS x]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testWhereInSubqueryWithMultivalueRowSource() {
        requireWhereInSubquery();
        String query = "FROM main_index | WHERE x IN (ROW x = [1, 2, 3])";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), UnresolvedAttribute.class).name());

        Row row = as(inSubquery.subquery(), Row.class);
        assertMultivalueRowField(row, "x", DataType.INTEGER, List.of(1, 2, 3));

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * A multivalue ROW combined with {@code MV_EXPAND} inside the IN-subquery — the typical use
     * case for a synthetic value list. Confirms the parser keeps the multivalue literal at the
     * {@link Row} leaf and stacks the {@link MvExpand} above it.
     *
     * Filter[InSubquery[?x, MvExpand[?x, Row[[[1, 2, 3][INTEGER] AS x]]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testWhereInSubqueryWithMultivalueRowSourceAndMvExpand() {
        requireWhereInSubquery();
        String query = "FROM main_index | WHERE x IN (ROW x = [1, 2, 3] | MV_EXPAND x)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), UnresolvedAttribute.class).name());

        MvExpand mvExpand = as(inSubquery.subquery(), MvExpand.class);
        Row row = as(mvExpand.child(), Row.class);
        assertMultivalueRowField(row, "x", DataType.INTEGER, List.of(1, 2, 3));

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * A ROW-sourced IN-subquery combined with another condition via {@code AND}. The IN-subquery
     * sits on the right of the {@link And}; its plan is unchanged from the basic ROW-source case.
     *
     * Filter[And[GreaterThan[?a, 5], InSubquery[?x, Row[[1[INTEGER] AS x]]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testWhereInSubqueryWithRowSourceAndOtherConditions() {
        requireWhereInSubquery();
        String query = "FROM main_index | WHERE a > 5 AND x IN (ROW x = 1 | WHERE x > 0)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        And and = as(filter.condition(), And.class);
        as(and.left(), GreaterThan.class);

        InSubquery inSubquery = as(and.right(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), UnresolvedAttribute.class).name());

        Filter innerFilter = as(inSubquery.subquery(), Filter.class);
        as(innerFilter.condition(), GreaterThan.class);
        Row row = as(innerFilter.child(), Row.class);
        assertRowField(row, "x", 1);

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * A ROW-sourced IN-subquery whose ROW declares multiple fields and the inner pipeline projects
     * only the matching one. Confirms the parser keeps all declared fields on the {@link Row} and
     * lets the {@link Keep} on top trim the projection.
     *
     * Filter[InSubquery[?x, Keep[[?x], Row[[1[INTEGER] AS x, 2[INTEGER] AS y]]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testWhereInSubqueryWithRowSourceMultipleFieldsAndKeep() {
        requireWhereInSubquery();
        String query = "FROM main_index | WHERE x IN (ROW x = 1, y = 2 | KEEP x)";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), UnresolvedAttribute.class).name());

        Keep keep = as(inSubquery.subquery(), Keep.class);
        Row row = as(keep.child(), Row.class);
        assertEquals(2, row.fields().size());
        assertEquals("x", row.fields().get(0).name());
        assertEquals(1, as(row.fields().get(0).child(), Literal.class).value());
        assertEquals("y", row.fields().get(1).name());
        assertEquals(2, as(row.fields().get(1).child(), Literal.class).value());

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * Verifies the parser accepts a {@code WHERE x IN (ROW x = 1 | <processing_command>)} for each
     * processing command that lives in a different ANTLR lexer mode. Mirrors
     * {@link #testRowSubqueryEndsWithProcessingCommandsInDifferentMode()} but for the IN-subquery
     * grammar path, and additionally covers {@code LOOKUP JOIN} to confirm the
     * {@code LOOKUP_JOIN_RP} closing token correctly hands control back to the IN-subquery's
     * closing {@code )}.
     *
     * Filter[(NOT) InSubquery[?x, &lt;processing_command_node&gt;[..., Row[[1[INTEGER] AS x]]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testWhereInSubqueryWithRowSourceEndsWithProcessingCommandsInDifferentMode() {
        requireWhereInSubquery();
        Map<String, Class<? extends LogicalPlan>> processingCommands = Map.ofEntries(
            Map.entry("INLINE STATS max_x = MAX(x) BY x", InlineStats.class),
            Map.entry("DISSECT y \"%{a} %{b}\"", Dissect.class),
            Map.entry("ENRICH clientip_policy ON x WITH env", Enrich.class),
            Map.entry("CHANGE_POINT x ON x AS type, pvalue", ChangePoint.class),
            Map.entry("FORK (WHERE x < 100) (WHERE x > 200)", Fork.class),
            Map.entry("MV_EXPAND x", MvExpand.class),
            Map.entry("RENAME x AS z", Rename.class),
            Map.entry("DROP x", Drop.class),
            Map.entry("LOOKUP JOIN lookup_index ON x", LookupJoin.class)
        );
        boolean negated = randomBoolean();
        String notClause = negated ? "NOT" : "";
        for (var entry : processingCommands.entrySet()) {
            String query = LoggerMessageFormat.format(null, """
                FROM main_index | WHERE x {} IN (ROW x = 1 | {})
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
            assertEquals(query, "x", as(inSubquery.value(), UnresolvedAttribute.class).name());
            as(inSubquery.subquery(), entry.getValue());

            UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
            assertEquals(query, "main_index", mainRelation.indexPattern().indexPattern());
        }
    }

    /**
     * IN-subquery whose body is a {@code FROM} fan-out of multiple ROW subqueries. Each ROW becomes
     * its own {@link Subquery} child of a {@link UnionAll} sitting directly under the
     * {@link InSubquery}.
     *
     * Filter[InSubquery[?x, UnionAll[Subquery[Row[a=1]], Subquery[Row[a=2]], Subquery[Row[a=3]]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testWhereInSubqueryWithMultipleRowCommands() {
        requireWhereInSubquery();
        String query = "FROM main_index | WHERE x IN (FROM (ROW a = 1), (ROW a = 2), (ROW a = 3))";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), UnresolvedAttribute.class).name());

        UnionAll unionAll = as(inSubquery.subquery(), UnionAll.class);
        assertEquals(3, unionAll.children().size());

        Subquery first = as(unionAll.children().get(0), Subquery.class);
        assertRowField(as(first.plan(), Row.class), "a", 1);
        Subquery second = as(unionAll.children().get(1), Subquery.class);
        assertRowField(as(second.plan(), Row.class), "a", 2);
        Subquery third = as(unionAll.children().get(2), Subquery.class);
        assertRowField(as(third.plan(), Row.class), "a", 3);

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * IN-subquery whose body mixes an index pattern with a ROW subquery. The index pattern is the
     * left-hand source of the {@link UnionAll} (kept as a bare {@link UnresolvedRelation}) while
     * the ROW source is wrapped in a {@link Subquery}, matching the behaviour validated by
     * {@link #testIndexPatternWithRowSubquery()}.
     *
     * Filter[InSubquery[?x, UnionAll[UnresolvedRelation[sub_index], Subquery[Row[x=1]]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testWhereInSubqueryWithIndexPatternAndRowCommand() {
        requireWhereInSubquery();
        String query = "FROM main_index | WHERE x IN (FROM sub_index, (ROW x = 1))";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), UnresolvedAttribute.class).name());

        UnionAll unionAll = as(inSubquery.subquery(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        UnresolvedRelation subRelation = as(unionAll.children().get(0), UnresolvedRelation.class);
        assertEquals("sub_index", subRelation.indexPattern().indexPattern());
        Subquery rowSubquery = as(unionAll.children().get(1), Subquery.class);
        assertRowField(as(rowSubquery.plan(), Row.class), "x", 1);

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * IN-subquery whose body mixes all three FROM-source flavours: an index pattern, a nested
     * FROM-subquery, and a ROW subquery. Each non-leading source is wrapped in its own
     * {@link Subquery}.
     *
     * Filter[InSubquery[?x, UnionAll[
     *   UnresolvedRelation[sub_index],
     *   Subquery[UnresolvedRelation[another_index]],
     *   Subquery[Row[x=1]]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testWhereInSubqueryWithMixedIndexPatternFromSubqueryAndRowCommand() {
        requireWhereInSubquery();
        String query = "FROM main_index | WHERE x IN (FROM sub_index, (FROM another_index), (ROW x = 1))";

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), UnresolvedAttribute.class).name());

        UnionAll unionAll = as(inSubquery.subquery(), UnionAll.class);
        assertEquals(3, unionAll.children().size());

        UnresolvedRelation subRelation = as(unionAll.children().get(0), UnresolvedRelation.class);
        assertEquals("sub_index", subRelation.indexPattern().indexPattern());

        Subquery fromSubquery = as(unionAll.children().get(1), Subquery.class);
        UnresolvedRelation anotherRelation = as(fromSubquery.plan(), UnresolvedRelation.class);
        assertEquals("another_index", anotherRelation.indexPattern().indexPattern());

        Subquery rowSubquery = as(unionAll.children().get(2), Subquery.class);
        assertRowField(as(rowSubquery.plan(), Row.class), "x", 1);

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * IN-subquery whose body mixes an index pattern with a ROW subquery and stacks processing
     * commands on top of the resulting {@link UnionAll}. Confirms outer-pipeline commands attach
     * above the {@link UnionAll} rather than to any single source.
     *
     * Filter[InSubquery[?x, Limit[10, Keep[[?x],
     *   UnionAll[UnresolvedRelation[sub_index], Subquery[Row[x=1]]]]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testWhereInSubqueryWithMixedSourcesAndProcessingCommands() {
        requireWhereInSubquery();
        String query = """
            FROM main_index
            | WHERE x IN (FROM sub_index, (ROW x = 1)
                          | KEEP x
                          | LIMIT 10)
            """;

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), UnresolvedAttribute.class).name());

        Limit limit = as(inSubquery.subquery(), Limit.class);
        assertEquals(10, as(limit.limit(), Literal.class).value());
        Keep keep = as(limit.child(), Keep.class);
        UnionAll unionAll = as(keep.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        UnresolvedRelation subRelation = as(unionAll.children().get(0), UnresolvedRelation.class);
        assertEquals("sub_index", subRelation.indexPattern().indexPattern());
        Subquery rowSubquery = as(unionAll.children().get(1), Subquery.class);
        assertRowField(as(rowSubquery.plan(), Row.class), "x", 1);

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    /**
     * IN-subquery whose body fans out into multiple ROW subqueries, each carrying its own inner
     * processing pipeline. Verifies the inner {@link Filter}/{@link Eval} stack on each ROW branch
     * is preserved independently of the surrounding {@link UnionAll}.
     *
     * Filter[InSubquery[?x, UnionAll[
     *   Subquery[Filter[?x &gt; 0[INTEGER], Row[x=1]]],
     *   Subquery[Eval[[?x + 1[INTEGER] AS y], Row[x=2]]]]]]
     * \_UnresolvedRelation[main_index]
     */
    public void testWhereInSubqueryWithMultipleRowCommandsAndProcessingCommandsEach() {
        requireWhereInSubquery();
        String query = """
            FROM main_index
            | WHERE x IN (FROM (ROW x = 1 | WHERE x > 0),
                               (ROW x = 2 | EVAL y = x + 1))
            """;

        LogicalPlan plan = query(query);
        Filter filter = as(plan, Filter.class);
        InSubquery inSubquery = as(filter.condition(), InSubquery.class);
        assertEquals("x", as(inSubquery.value(), UnresolvedAttribute.class).name());

        UnionAll unionAll = as(inSubquery.subquery(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // First branch: ROW x = 1 | WHERE x > 0
        Subquery firstBranch = as(unionAll.children().get(0), Subquery.class);
        Filter firstFilter = as(firstBranch.plan(), Filter.class);
        GreaterThan firstCondition = as(firstFilter.condition(), GreaterThan.class);
        assertEquals("x", as(firstCondition.left(), Attribute.class).name());
        assertEquals(0, as(firstCondition.right(), Literal.class).value());
        assertRowField(as(firstFilter.child(), Row.class), "x", 1);

        // Second branch: ROW x = 2 | EVAL y = x + 1
        Subquery secondBranch = as(unionAll.children().get(1), Subquery.class);
        Eval secondEval = as(secondBranch.plan(), Eval.class);
        assertEquals(1, secondEval.fields().size());
        assertEquals("y", secondEval.fields().get(0).name());
        assertRowField(as(secondEval.child(), Row.class), "x", 2);

        UnresolvedRelation mainRelation = as(filter.child(), UnresolvedRelation.class);
        assertEquals("main_index", mainRelation.indexPattern().indexPattern());
    }

    public void testRowSubqueryInReleaseBuild() {
        requireSubqueryInFromCommand();
        assumeFalse("only relevant for non-snapshot builds", Build.current().isSnapshot());
        var mainQueryIndexPattern = randomIndexPatterns();
        String query = LoggerMessageFormat.format(null, """
            FROM {}, (ROW x = 1)
            """, mainQueryIndexPattern);

        LogicalPlan plan = query(query);
        UnionAll unionAll = as(plan, UnionAll.class);
        assertEquals(2, unionAll.children().size());

        UnresolvedRelation mainRelation = as(unionAll.children().get(0), UnresolvedRelation.class);
        assertEquals(unquoteIndexPattern(mainQueryIndexPattern), mainRelation.indexPattern().indexPattern());

        Subquery subquery = as(unionAll.children().get(1), Subquery.class);
        as(subquery.plan(), Row.class);
    }

    // negative tests
    /**
     * The TS source command does not allow subqueries, regardless of whether the subquery uses FROM or ROW.
     */
    public void testTimeSeriesWithRowSubquery() {
        requireSubqueryInFromCommand();
        String query = "TS index1, (ROW x = 1)";
        expectThrows(ParsingException.class, containsString("line 1:1: Subqueries are not supported in TS command"), () -> query(query));
    }

    /**
     * Asserts the given {@link Row} has exactly one {@link Alias} field with the given name
     * whose child is a multivalue {@link Literal} of the given type and values.
     */
    private static void assertMultivalueRowField(Row row, String aliasName, DataType type, List<?> expectedValues) {
        assertEquals(1, row.fields().size());
        assertMultivalueAlias(row.fields().get(0), aliasName, type, expectedValues);
    }

    /**
     * Asserts the given {@link Alias} has the expected name and that its child is a multivalue
     * {@link Literal} of the given type and list of values. Uses a generic {@link List}-based
     * comparison so each test can pass the natural Java types (boxed primitives or
     * {@link BytesRef}s for keyword strings).
     */
    private static void assertMultivalueAlias(Alias alias, String aliasName, DataType type, List<?> expectedValues) {
        assertEquals(aliasName, alias.name());
        Literal literal = as(alias.child(), Literal.class);
        assertEquals(type, literal.dataType());
        assertEquals(expectedValues, literal.value());
    }

    /**
     * Asserts the given {@link Alias} has the expected name and that its child is a single-value
     * (scalar) {@link Literal} of the given {@link DataType} and value.
     */
    private static void assertScalarAlias(Alias alias, String aliasName, DataType type, Object expectedValue) {
        assertEquals(aliasName, alias.name());
        Literal literal = as(alias.child(), Literal.class);
        assertEquals(type, literal.dataType());
        assertEquals(expectedValue, literal.value());
    }

    /**
     * Asserts the given {@link Row} has a single {@link Alias} field with the given name
     * whose child is an integer {@link Literal} with the given value.
     */
    private static void assertRowField(Row row, String aliasName, int aliasValue) {
        assertEquals(1, row.fields().size());
        Alias alias = row.fields().get(0);
        assertEquals(aliasName, alias.name());
        Literal literal = as(alias.child(), Literal.class);
        assertEquals(aliasValue, literal.value());
    }
}
