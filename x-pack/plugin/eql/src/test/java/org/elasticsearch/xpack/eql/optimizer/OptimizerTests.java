/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.optimizer;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.analysis.Analyzer;
import org.elasticsearch.xpack.eql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.eql.analysis.Verifier;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.parser.EqlParser;
import org.elasticsearch.xpack.eql.plan.logical.KeyedFilter;
import org.elasticsearch.xpack.eql.plan.logical.LimitWithOffset;
import org.elasticsearch.xpack.eql.plan.logical.Sequence;
import org.elasticsearch.xpack.eql.plan.logical.Tail;
import org.elasticsearch.xpack.eql.plan.physical.LocalRelation;
import org.elasticsearch.xpack.eql.stats.Metrics;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.EmptyAttribute;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.Order.NullsPosition;
import org.elasticsearch.xpack.ql.expression.Order.OrderDirection;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.ql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.TypesTests;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.eql.EqlTestUtils.TEST_CFG_CASE_INSENSITIVE;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class OptimizerTests extends ESTestCase {


    private static final String INDEX_NAME = "test";
    private EqlParser parser = new EqlParser();
    private IndexResolution index = loadIndexResolution("mapping-default.json");
    private Optimizer optimizer = new Optimizer();

    private static Map<String, EsField> loadEqlMapping(String name) {
        return TypesTests.loadMapping(name);
    }

    public static IndexResolution loadIndexResolution(String name) {
        return IndexResolution.valid(new EsIndex(INDEX_NAME, loadEqlMapping(name)));
    }

    private LogicalPlan accept(IndexResolution resolution, String eql) {
        PreAnalyzer preAnalyzer = new PreAnalyzer();
        Analyzer analyzer = new Analyzer(TEST_CFG_CASE_INSENSITIVE, new EqlFunctionRegistry(), new Verifier(new Metrics()));
        return optimizer.optimize(analyzer.analyze(preAnalyzer.preAnalyze(parser.createStatement(eql), resolution)));
    }

    private LogicalPlan accept(String eql) {
        return accept(index, eql);
    }

    public void testIsNull() {
        List<String> tests = Arrays.asList(
            "foo where command_line == null",
            "foo where null == command_line"
        );

        for (String q : tests) {
            LogicalPlan plan = defaultPipes(accept(q));
            assertTrue(plan instanceof Project);
            plan = ((Project) plan).child();

            assertTrue(plan instanceof Filter);

            Filter filter = (Filter) plan;
            And condition = (And) filter.condition();
            assertTrue(condition.right() instanceof IsNull);

            IsNull check = (IsNull) condition.right();
            assertEquals(((FieldAttribute) check.field()).name(), "command_line");
        }
    }
    public void testIsNotNull() {
        List<String> tests = Arrays.asList(
            "foo where command_line != null",
            "foo where null != command_line"
        );

        for (String q : tests) {
            LogicalPlan plan = defaultPipes(accept(q));
            assertTrue(plan instanceof Project);
            plan = ((Project) plan).child();
            assertTrue(plan instanceof Filter);

            Filter filter = (Filter) plan;
            And condition = (And) filter.condition();
            assertTrue(condition.right() instanceof IsNotNull);

            IsNotNull check = (IsNotNull) condition.right();
            assertEquals(((FieldAttribute) check.field()).name(), "command_line");
        }
    }

    public void testEqualsWildcard() {
        List<String> tests = Arrays.asList(
            "foo where command_line == '* bar *'",
            "foo where '* bar *' == command_line"
        );

        for (String q : tests) {
            LogicalPlan plan = defaultPipes(accept(q));
            assertTrue(plan instanceof Project);
            plan = ((Project) plan).child();
            assertTrue(plan instanceof Filter);

            Filter filter = (Filter) plan;
            And condition = (And) filter.condition();
            assertTrue(condition.right() instanceof Like);

            Like like = (Like) condition.right();
            assertEquals(((FieldAttribute) like.field()).name(), "command_line");
            assertEquals(like.pattern().asJavaRegex(), "^.* bar .*$");
            assertEquals(like.pattern().asLuceneWildcard(), "* bar *");
            assertEquals(like.pattern().asIndexNameWildcard(), "* bar *");
        }
    }

    public void testNotEqualsWildcard() {
        List<String> tests = Arrays.asList(
            "foo where command_line != '* baz *'",
            "foo where '* baz *' != command_line"
        );

        for (String q : tests) {
            LogicalPlan plan = defaultPipes(accept(q));
            assertTrue(plan instanceof Project);
            plan = ((Project) plan).child();

            assertTrue(plan instanceof Filter);

            Filter filter = (Filter) plan;
            And condition = (And) filter.condition();
            assertTrue(condition.right() instanceof Not);

            Not not = (Not) condition.right();
            Like like = (Like) not.field();
            assertEquals(((FieldAttribute) like.field()).name(), "command_line");
            assertEquals(like.pattern().asJavaRegex(), "^.* baz .*$");
            assertEquals(like.pattern().asLuceneWildcard(), "* baz *");
            assertEquals(like.pattern().asIndexNameWildcard(), "* baz *");
        }
    }

    public void testWildcardEscapes() {
        LogicalPlan plan = defaultPipes(accept("foo where command_line == '* %bar_ * \\\\ \\n \\r \\t'"));
        assertTrue(plan instanceof Project);
        plan = ((Project) plan).child();
        assertTrue(plan instanceof Filter);

        Filter filter = (Filter) plan;
        And condition = (And) filter.condition();
        assertTrue(condition.right() instanceof Like);

        Like like = (Like) condition.right();
        assertEquals(((FieldAttribute) like.field()).name(), "command_line");
        assertEquals(like.pattern().asJavaRegex(), "^.* %bar_ .* \\\\ \n \r \t$");
        assertEquals(like.pattern().asLuceneWildcard(), "* %bar_ * \\\\ \n \r \t");
        assertEquals(like.pattern().asIndexNameWildcard(), "* %bar_ * \\ \n \r \t");
    }

    public void testCombineHeadBigHeadSmall() {
        checkOffsetAndLimit(accept("process where true | head 10 | head 1"), 0, 1);
    }

    public void testCombineHeadSmallHeadBig() {
        checkOffsetAndLimit(accept("process where true | head 1 | head 12"), 0, 1);
    }

    public void testCombineTailBigTailSmall() {
        checkOffsetAndLimit(accept("process where true | tail 10 | tail 1"), 0, -1);
    }

    public void testCombineTailSmallTailBig() {
        checkOffsetAndLimit(accept("process where true | tail 1 | tail 12"), 0, -1);
    }

    public void testCombineHeadBigTailSmall() {
        checkOffsetAndLimit(accept("process where true | head 10 | tail 7"), 3, 7);
    }

    public void testCombineTailBigHeadSmall() {
        checkOffsetAndLimit(accept("process where true | tail 10 | head 7"), 3, -7);
    }

    public void testCombineTailSmallHeadBig() {
        checkOffsetAndLimit(accept("process where true | tail 7 | head 10"), 0, -7);
    }

    public void testCombineHeadBigTailBig() {
        checkOffsetAndLimit(accept("process where true | head 1 | tail 7"), 0, 1);
    }

    public void testCombineHeadTailWithHeadAndTail() {
        checkOffsetAndLimit(accept("process where true | head 10 | tail 7 | head 5 | tail 3"), 5, 3);
    }

    public void testCombineTailHeadWithTailAndHead() {
        checkOffsetAndLimit(accept("process where true | tail 10 | head 7 | tail 5 | head 3"), 5, -3);
    }

    private void checkOffsetAndLimit(LogicalPlan plan, int offset, int limit) {
        assertTrue(plan instanceof LimitWithOffset);
        LimitWithOffset lo = (LimitWithOffset) plan;
        assertEquals("Incorrect offset", offset, lo.offset());
        assertEquals("Incorrect limit", limit, lo.limit().fold());
    }

    private static Attribute timestamp() {
        return new FieldAttribute(EMPTY, "test", new EsField("field", DataTypes.INTEGER, emptyMap(), true));
    }

    private static Attribute tiebreaker() {
        return new EmptyAttribute(EMPTY);
    }

    private static LogicalPlan rel() {
        return new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, "catalog", "index"), "", false);
    }

    private static KeyedFilter keyedFilter(LogicalPlan child) {
        return new KeyedFilter(EMPTY, child, emptyList(), timestamp(), tiebreaker());
    }

    public void testSkipQueryOnLimitZero() {
        KeyedFilter rule1 = keyedFilter(new LocalRelation(EMPTY, emptyList()));
        KeyedFilter rule2 = keyedFilter(new Filter(EMPTY, rel(), new IsNull(EMPTY, Literal.TRUE)));
        KeyedFilter until = keyedFilter(new Filter(EMPTY, rel(), Literal.FALSE));
        Sequence s = new Sequence(EMPTY, asList(rule1, rule2), until, TimeValue.MINUS_ONE, timestamp(), tiebreaker(), OrderDirection.ASC);

        LogicalPlan optimized = optimizer.optimize(s);
        assertEquals(LocalRelation.class, optimized.getClass());
    }

    public void testSortByLimit() {
        Project p = new Project(EMPTY, rel(), emptyList());
        OrderBy o = new OrderBy(EMPTY, p, singletonList(new Order(EMPTY, tiebreaker(), OrderDirection.ASC, NullsPosition.FIRST)));
        Tail t = new Tail(EMPTY, new Literal(EMPTY, 1, DataTypes.INTEGER), o);

        LogicalPlan optimized = new Optimizer.SortByLimit().rule(t);
        assertEquals(LimitWithOffset.class, optimized.getClass());
        LimitWithOffset l = (LimitWithOffset) optimized;
        assertOrder(l, OrderDirection.DESC);
    }

    public void testPushdownOrderBy() {
        Filter filter = new Filter(EMPTY, rel(), new IsNull(EMPTY, Literal.TRUE));
        KeyedFilter rule1 = keyedFilter(filter);
        KeyedFilter rule2 = keyedFilter(filter);
        KeyedFilter until = keyedFilter(filter);
        Sequence s = new Sequence(EMPTY, asList(rule1, rule2), until, TimeValue.MINUS_ONE, timestamp(), tiebreaker(), OrderDirection.ASC);
        OrderBy o = new OrderBy(EMPTY, s, singletonList(new Order(EMPTY, tiebreaker(), OrderDirection.DESC, NullsPosition.FIRST)));

        LogicalPlan optimized = new Optimizer.PushDownOrderBy().rule(o);
        assertEquals(Sequence.class, optimized.getClass());
        Sequence seq = (Sequence) optimized;

        assertOrder(seq.until(), OrderDirection.ASC);
        assertOrder(seq.queries().get(0), OrderDirection.DESC);
        assertOrder(seq.queries().get(1), OrderDirection.ASC);
    }

    private void assertOrder(UnaryPlan plan, OrderDirection direction) {
        assertEquals(OrderBy.class, plan.child().getClass());
        OrderBy orderBy = (OrderBy) plan.child();
        Order order = orderBy.order().get(0);
        assertEquals(direction, order.direction());
    }

    private LogicalPlan defaultPipes(LogicalPlan plan) {
        assertTrue(plan instanceof LimitWithOffset);
        plan = ((LimitWithOffset) plan).child();
        assertTrue(plan instanceof OrderBy);
        return ((OrderBy) plan).child();
    }
}