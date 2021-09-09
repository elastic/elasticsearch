/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.optimizer;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.analysis.Analyzer;
import org.elasticsearch.xpack.eql.analysis.PostAnalyzer;
import org.elasticsearch.xpack.eql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.eql.analysis.Verifier;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.ToString;
import org.elasticsearch.xpack.eql.parser.EqlParser;
import org.elasticsearch.xpack.eql.plan.logical.KeyedFilter;
import org.elasticsearch.xpack.eql.plan.logical.LimitWithOffset;
import org.elasticsearch.xpack.eql.plan.logical.Sequence;
import org.elasticsearch.xpack.eql.plan.logical.Tail;
import org.elasticsearch.xpack.eql.plan.physical.LocalRelation;
import org.elasticsearch.xpack.eql.stats.Metrics;
import org.elasticsearch.xpack.ql.TestUtils;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.EmptyAttribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.Order.NullsPosition;
import org.elasticsearch.xpack.ql.expression.Order.OrderDirection;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.PushDownAndCombineFilters;
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

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.eql.EqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.ql.TestUtils.UTC;
import static org.elasticsearch.xpack.ql.expression.Literal.TRUE;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;

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
        PostAnalyzer postAnalyzer = new PostAnalyzer();
        Analyzer analyzer = new Analyzer(TEST_CFG, new EqlFunctionRegistry(), new Verifier(new Metrics()));
        return optimizer.optimize(postAnalyzer.postAnalyze(analyzer.analyze(preAnalyzer.preAnalyze(parser.createStatement(eql),
            resolution)), TEST_CFG));
    }

    private LogicalPlan accept(String eql) {
        return accept(index, eql);
    }

    public void testIsNull() {
        List<String> tests = asList(
            "foo where command_line == null",
            "foo where null == command_line"
        );

        for (String q : tests) {
            LogicalPlan plan = defaultPipes(accept(q));
            assertTrue(plan instanceof Filter);

            Filter filter = (Filter) plan;
            And condition = (And) filter.condition();
            assertTrue(condition.right() instanceof IsNull);

            IsNull check = (IsNull) condition.right();
            assertEquals(((FieldAttribute) check.field()).name(), "command_line");
        }
    }

    public void testIsNotNull() {
        List<String> tests = asList(
            "foo where command_line != null",
            "foo where null != command_line"
        );

        for (String q : tests) {
            LogicalPlan plan = defaultPipes(accept(q));
            assertTrue(plan instanceof Filter);

            Filter filter = (Filter) plan;
            And condition = (And) filter.condition();
            assertTrue(condition.right() instanceof Not);
            Not not = (Not) condition.right();
            List<Expression> children = not.children();
            assertEquals(1, children.size());
            assertTrue(children.get(0) instanceof IsNull);

            IsNull check = (IsNull) children.get(0);
            assertEquals(((FieldAttribute) check.field()).name(), "command_line");
        }
    }

    public void testEqualsWildcardOnRight() {
        String q = "foo where command_line : \"* bar *\"";

        LogicalPlan plan = defaultPipes(accept(q));
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

    public void testEqualsWildcardQuestionmarkOnRight() {
        String q = "foo where command_line : \"? bar ?\"";

        LogicalPlan plan = defaultPipes(accept(q));
        assertTrue(plan instanceof Filter);

        Filter filter = (Filter) plan;
        And condition = (And) filter.condition();
        assertTrue(condition.right() instanceof Like);

        Like like = (Like) condition.right();
        assertEquals("command_line", ((FieldAttribute) like.field()).name());
        assertEquals( "^. bar .$", like.pattern().asJavaRegex());
        assertEquals("? bar ?", like.pattern().asLuceneWildcard());
        assertEquals( "* bar *", like.pattern().asIndexNameWildcard());
    }

    public void testEqualsWildcardWithLiteralsOnLeft() {
        List<String> tests = asList(
            "foo where \"abc\": \"*b*\"",
            "foo where \"abc\": \"ab*\"",
            "foo where \"abc\": \"*bc\""
        );

        for (String q : tests) {
            LogicalPlan plan = accept(q);
            plan = defaultPipes(plan);
            assertTrue(plan instanceof Filter);
            // check the optimizer kicked in and folding was applied
            Filter filter = (Filter) plan;
            Equals condition = (Equals) filter.condition();
            assertEquals("foo", condition.right().fold());
        }
    }

    public void testEqualsWildcardIgnoredOnLeftLiteral() {
        List<String> tests = asList(
            "foo where \"*b*\" : \"abc\"",
            "foo where \"*b\" : \"abc\"",
            "foo where \"b*\" : \"abc\"",
            "foo where \"b*?\" : \"abc\"",
            "foo where \"b?\" : \"abc\"",
            "foo where \"?b\" : \"abc\"",
            "foo where \"?b*\" : \"abc\""
        );

        // string comparison that evaluates to false
        for (String q : tests) {
            LogicalPlan plan = accept(q);
            assertTrue(plan instanceof LocalRelation);
        }
    }

    public void testEqualsWildcardWithLiteralsOnLeftAndPatternOnRightNotMatching() {
        List<String> tests = asList(
            "foo where \"abc\": \"*b\"",
            "foo where \"abc\": \"b*\"",
            "foo where \"abc\": \"b?\"",
            "foo where \"abc\": \"?b\""
        );

        // string comparison that evaluates to false
        for (String q : tests) {
            LogicalPlan plan = accept(q);
            assertTrue(plan instanceof LocalRelation);
        }
    }

    public void testWildcardEscapes() {
        LogicalPlan plan = defaultPipes(accept("foo where command_line : \"* %bar_ * \\\\ \\n \\r \\t\""));
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
        assertTrue(plan instanceof Project);
        plan = ((Project) plan).child();
        assertTrue(plan instanceof LimitWithOffset);
        LimitWithOffset lo = (LimitWithOffset) plan;
        assertEquals("Incorrect offset", offset, lo.offset());
        assertEquals("Incorrect limit", limit, lo.limit().fold());
    }

    public void testSkipQueryOnLimitZero() {
        KeyedFilter rule1 = keyedFilter(new LocalRelation(EMPTY, emptyList()));
        KeyedFilter rule2 = keyedFilter(basicFilter(new IsNull(EMPTY, TRUE)));
        KeyedFilter until = keyedFilter(basicFilter(Literal.FALSE));
        Sequence s = new Sequence(EMPTY, asList(rule1, rule2), until, TimeValue.MINUS_ONE, timestamp(), tiebreaker(), OrderDirection.ASC);

        LogicalPlan optimized = optimizer.optimize(s);
        assertEquals(LocalRelation.class, optimized.getClass());
    }

    public void testSortByLimit() {
        Filter f = new Filter(EMPTY, rel(), TRUE);
        OrderBy o = new OrderBy(EMPTY, f, singletonList(new Order(EMPTY, tiebreaker(), OrderDirection.ASC, NullsPosition.FIRST)));
        Tail t = new Tail(EMPTY, new Literal(EMPTY, 1, INTEGER), o);

        LogicalPlan optimized = new Optimizer.SortByLimit().rule(t);
        assertEquals(LimitWithOffset.class, optimized.getClass());
        LimitWithOffset l = (LimitWithOffset) optimized;
        assertOrder(l, OrderDirection.DESC);
    }

    public void testPushdownOrderBy() {
        Filter filter = basicFilter(new IsNull(EMPTY, TRUE));
        KeyedFilter rule1 = keyedFilter(filter);
        KeyedFilter rule2 = keyedFilter(filter);
        Sequence s = sequence(rule1, rule2);
        OrderBy o = new OrderBy(EMPTY, s, singletonList(new Order(EMPTY, tiebreaker(), OrderDirection.DESC, NullsPosition.FIRST)));

        LogicalPlan optimized = new Optimizer.PushDownOrderBy().rule(o);
        assertEquals(Sequence.class, optimized.getClass());
        Sequence seq = (Sequence) optimized;

        assertOrder(seq.until(), OrderDirection.ASC);
        assertOrder(seq.queries().get(0), OrderDirection.DESC);
        assertOrder(seq.queries().get(1), OrderDirection.ASC);
    }

    /**
     * Filter X
     * Filter Y
     * ==
     * Filter X and Y
     */
    public void testCombineFilters() {
        Expression left = new IsNull(EMPTY, TRUE);
        Expression right = equalsExpression();

        Filter filterChild = basicFilter(left);
        Filter filterParent = new Filter(EMPTY, filterChild, right);

        LogicalPlan result = new PushDownAndCombineFilters().apply(filterParent);

        assertEquals(Filter.class, result.getClass());
        Expression condition = ((Filter) result).condition();
        assertEquals(And.class, condition.getClass());
        And and = (And) condition;
        assertEquals(left, and.left());
        assertEquals(right, and.right());
    }

    /**
     * Filter X
     * UnaryNode
     * LeafNode
     * ==
     * UnaryNode
     * Filter X
     * LeafNode
     */
    public void testPushDownFilterUnary() {
        Expression left = new IsNull(EMPTY, TRUE);

        OrderBy order = new OrderBy(EMPTY, rel(), emptyList());
        Filter filter = new Filter(EMPTY, order, left);

        LogicalPlan result = new PushDownAndCombineFilters().apply(filter);

        assertEquals(OrderBy.class, result.getClass());
        OrderBy o = (OrderBy) result;
        assertEquals(Filter.class, o.child().getClass());
        Filter f = (Filter) o.child();

        assertEquals(rel(), f.child());
        assertEquals(filter.condition(), f.condition());
    }

    /**
     * Filter
     * LeafNode
     * ==
     * Filter
     * LeafNode
     */
    public void testPushDownFilterDoesNotApplyOnNonUnary() {
        Expression left = new IsNull(EMPTY, TRUE);

        KeyedFilter rule1 = keyedFilter(new LocalRelation(EMPTY, emptyList()));
        KeyedFilter rule2 = keyedFilter(basicFilter(new IsNull(EMPTY, TRUE)));

        Sequence s = sequence(rule1, rule2);
        Filter filter = new Filter(EMPTY, s, left);

        LogicalPlan result = new PushDownAndCombineFilters().apply(filter);

        assertEquals(Filter.class, result.getClass());
        Filter f = (Filter) result;
        assertEquals(s, f.child());
    }

    /**
     * sequence
     * 1. filter a gt 1 by a
     * 2. filter X by a
     * ==
     * sequence
     * 1. filter a gt 1 by a
     * 2. filter a gt 1 by a
     * \filter X
     */
    public void testKeySameConstraints() {
        Attribute a = key("a");

        Expression keyCondition = gtExpression(a);
        Expression filter = equalsExpression();

        KeyedFilter rule1 = keyedFilter(basicFilter(keyCondition), a);
        KeyedFilter rule2 = keyedFilter(basicFilter(filter), a);

        Sequence s = sequence(rule1, rule2);

        LogicalPlan result = new Optimizer.PropagateJoinKeyConstraints().apply(s);

        assertEquals(Sequence.class, result.getClass());
        Sequence seq = (Sequence) result;

        List<KeyedFilter> queries = seq.queries();
        assertEquals(rule1, queries.get(0));
        KeyedFilter query2 = queries.get(1);
        assertEquals(keyCondition, filterCondition(query2.child()));
        assertEquals(filter, filterCondition(query2.child().children().get(0)));
    }

    /**
     * sequence
     * 1. filter a gt 1 by a
     * 2. filter b == true by b
     * ==
     * sequence
     * 1. filter a == true by a
     * \filter a gt 1
     * 2. filter b gt 1 by b
     * \filter b == true
     */
    public void testSameTwoKeysConstraints() {
        Attribute a = key("a");
        Attribute b = key("b");

        Expression keyACondition = gtExpression(a);
        Expression keyBCondition = new Equals(EMPTY, b, TRUE);

        KeyedFilter rule1 = keyedFilter(basicFilter(keyACondition), a, b);
        KeyedFilter rule2 = keyedFilter(basicFilter(keyBCondition), a, b);

        Sequence s = sequence(rule1, rule2);

        LogicalPlan result = new Optimizer.PropagateJoinKeyConstraints().apply(s);

        assertEquals(Sequence.class, result.getClass());
        Sequence seq = (Sequence) result;

        List<KeyedFilter> queries = seq.queries();
        KeyedFilter query1 = queries.get(0);
        assertEquals(keyBCondition, filterCondition(query1.child()));
        assertEquals(keyACondition, filterCondition(query1.child().children().get(0)));

        KeyedFilter query2 = queries.get(1);
        assertEquals(keyACondition, filterCondition(query2.child()));
        assertEquals(keyBCondition, filterCondition(query2.child().children().get(0)));
    }

    /**
     * sequence
     * 1. filter a gt 1 by a
     * 2. filter b == 1 by b
     * ==
     * sequence
     * 1. filter a == 1 by a
     * \filter a gt 1
     * 2. filter b gt 1 by b
     * \filter b == 1
     */
    public void testDifferentOneKeyConstraints() {
        Attribute a = key("a");
        Attribute b = key("b");

        Expression keyARuleACondition = gtExpression(a);
        Expression keyBRuleACondition = gtExpression(b);

        Expression keyARuleBCondition = new Equals(EMPTY, a, TRUE);
        Expression keyBRuleBCondition = new Equals(EMPTY, b, TRUE);

        KeyedFilter rule1 = keyedFilter(basicFilter(keyARuleACondition), a);
        KeyedFilter rule2 = keyedFilter(basicFilter(keyBRuleBCondition), b);

        Sequence s = sequence(rule1, rule2);

        LogicalPlan result = new Optimizer.PropagateJoinKeyConstraints().apply(s);

        assertEquals(Sequence.class, result.getClass());
        Sequence seq = (Sequence) result;

        List<KeyedFilter> queries = seq.queries();
        KeyedFilter query1 = queries.get(0);

        assertEquals(keyARuleBCondition, filterCondition(query1.child()));
        assertEquals(keyARuleACondition, filterCondition(query1.child().children().get(0)));

        KeyedFilter query2 = queries.get(1);
        assertEquals(keyBRuleACondition, filterCondition(query2.child()));
        assertEquals(keyBRuleBCondition, filterCondition(query2.child().children().get(0)));
    }

    /**
     * sequence
     * 1. filter a1 gt 1 and a2 lt 1 by a1, a2
     * 2. filter someKey == true by b1, b2
     * ==
     * sequence
     * 1. filter a1 gt 1 and a2 gt 1 by a1, a2
     * 2. filter b1 gt 1 and b2 gt 1 by b1, b2
     * \filter someKey == true
     */
    public void testQueryLevelTwoKeyConstraints() {
        ZoneId zd = randomZone();
        Attribute a1 = key("a1");
        Attribute a2 = key("a2");

        Attribute b1 = key("b1");
        Attribute b2 = key("b2");

        Expression keyA1RuleACondition = gtExpression(a1);
        Expression keyA2RuleACondition = new LessThan(EMPTY, a2, new Literal(EMPTY, 1, INTEGER), zd);
        Expression ruleACondition = new And(EMPTY, keyA1RuleACondition, keyA2RuleACondition);

        Expression ruleBCondition = new Equals(EMPTY, key("someKey"), TRUE);

        KeyedFilter rule1 = keyedFilter(basicFilter(ruleACondition), a1, a2);
        KeyedFilter rule2 = keyedFilter(basicFilter(ruleBCondition), b1, b2);

        Sequence s = sequence(rule1, rule2);

        LogicalPlan result = new Optimizer.PropagateJoinKeyConstraints().apply(s);

        assertEquals(Sequence.class, result.getClass());
        Sequence seq = (Sequence) result;

        List<KeyedFilter> queries = seq.queries();
        KeyedFilter query1 = queries.get(0);

        assertEquals(rule1, query1);

        KeyedFilter query2 = queries.get(1);
        // rewrite constraints for key B
        Expression keyB1RuleACondition = gtExpression(b1);
        Expression keyB2RuleACondition = new LessThan(EMPTY, b2, new Literal(EMPTY, 1, INTEGER), zd);

        assertEquals(new And(EMPTY, keyB1RuleACondition, keyB2RuleACondition), filterCondition(query2.child()));
        assertEquals(ruleBCondition, filterCondition(query2.child().children().get(0)));
    }

    /**
     * Key conditions inside a disjunction (OR) are ignored
     * <p>
     * sequence
     * 1. filter a gt 1 OR x == 1 by a
     * 2. filter x == 1 by b
     * ==
     * same
     */
    public void testSkipKeySameWithDisjunctionConstraints() {
        Attribute a = key("a");

        Expression keyCondition = gtExpression(a);
        Expression filter = equalsExpression();
        Expression cond = new Or(EMPTY, filter, keyCondition);

        KeyedFilter rule1 = keyedFilter(basicFilter(cond), a);
        KeyedFilter rule2 = keyedFilter(basicFilter(filter), a);

        Sequence s = sequence(rule1, rule2);

        LogicalPlan result = new Optimizer.PropagateJoinKeyConstraints().apply(s);

        assertEquals(Sequence.class, result.getClass());
        Sequence seq = (Sequence) result;

        List<KeyedFilter> queries = seq.queries();
        assertEquals(rule1, queries.get(0));
        assertEquals(rule2, queries.get(1));
    }

    /**
     * Key conditions inside a conjunction (AND) are picked up
     * <p>
     * sequence
     * 1. filter a gt 1 and x == 1 by a
     * 2. filter x == 1 by b
     * ==
     * sequence
     * 1. filter a gt 1 and x == 1 by a
     * 2. filter b gt 1 by b
     * \filter x == 1
     */
    public void testExtractKeySameFromDisjunction() {
        Attribute a = key("a");

        Expression keyCondition = gtExpression(a);
        Expression filter = equalsExpression();

        Expression cond = new And(EMPTY, filter, keyCondition);

        KeyedFilter rule1 = keyedFilter(basicFilter(cond), a);
        KeyedFilter rule2 = keyedFilter(basicFilter(filter), a);

        Sequence s = sequence(rule1, rule2);

        LogicalPlan result = new Optimizer.PropagateJoinKeyConstraints().apply(s);

        assertEquals(Sequence.class, result.getClass());
        Sequence seq = (Sequence) result;

        List<KeyedFilter> queries = seq.queries();
        assertEquals(rule1, queries.get(0));

        KeyedFilter query2 = queries.get(1);
        LogicalPlan child2 = query2.child();

        Expression keyRuleBCondition = gtExpression(a);

        assertEquals(keyRuleBCondition, filterCondition(child2));
        assertEquals(filter, filterCondition(child2.children().get(0)));
    }

    /**
     * Multiple key conditions inside a conjunction (AND) are picked up
     * <p>
     * sequence
     * 1. filter a gt 1 and x by a
     * 2. filter x by b
     * =
     * sequence
     * 1. filter a gt 1 and x by a
     * 2. filter b gt 1 by b
     * \filter x
     */
    public void testDifferentKeyFromDisjunction() {
        Attribute a = key("a");
        Attribute b = key("b");

        Expression keyARuleACondition = gtExpression(a);
        Expression filter = equalsExpression();

        Expression cond = new And(EMPTY, filter, new And(EMPTY, keyARuleACondition, filter));

        KeyedFilter rule1 = keyedFilter(basicFilter(cond), a);
        KeyedFilter rule2 = keyedFilter(basicFilter(filter), b);

        Sequence s = sequence(rule1, rule2);

        LogicalPlan result = new Optimizer.PropagateJoinKeyConstraints().apply(s);

        assertEquals(Sequence.class, result.getClass());
        Sequence seq = (Sequence) result;

        List<KeyedFilter> queries = seq.queries();
        assertEquals(rule1, queries.get(0));

        KeyedFilter query2 = queries.get(1);
        LogicalPlan child2 = query2.child();

        Expression keyRuleBCondition = gtExpression(b);

        assertEquals(keyRuleBCondition, filterCondition(child2));
        assertEquals(filter, filterCondition(child2.children().get(0)));
    }

    // ((a + 1) - 3) * 4 >= 16 -> a >= 6.
    public void testReduceBinaryComparisons() {
        LogicalPlan plan = accept("foo where ((pid + 1) - 3) * 4 >= 16");
        assertNotNull(plan);
        List<LogicalPlan> filters = plan.collectFirstChildren(x -> x instanceof Filter);
        assertNotNull(filters);
        assertEquals(1, filters.size());
        assertTrue(filters.get(0) instanceof Filter);
        Filter filter = (Filter) filters.get(0);

        assertTrue(filter.condition() instanceof And);
        And and = (And) filter.condition();
        assertTrue(and.right() instanceof GreaterThanOrEqual);
        GreaterThanOrEqual gte = (GreaterThanOrEqual) and.right();

        assertTrue(gte.left() instanceof FieldAttribute);
        assertEquals("pid", ((FieldAttribute) gte.left()).name());

        assertTrue(gte.right() instanceof Literal);
        assertEquals(6, ((Literal) gte.right()).value());
    }

    public void testReplaceCastOnField() {
        Attribute a = TestUtils.fieldAttribute("string", DataTypes.KEYWORD);
        ToString ts = new ToString(EMPTY, a);
        assertSame(a, new Optimizer.PruneCast().maybePruneCast(ts));
    }

    public void testReplaceCastOnLiteral() {
        Literal l = new Literal(EMPTY, "string", DataTypes.KEYWORD);
        ToString ts = new ToString(EMPTY, l);
        assertSame(l, new Optimizer.PruneCast().maybePruneCast(ts));
    }

    private static Attribute timestamp() {
        return new FieldAttribute(EMPTY, "test", new EsField("field", INTEGER, emptyMap(), true));
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

    private static KeyedFilter keyedFilter(LogicalPlan child, NamedExpression... keys) {
        return new KeyedFilter(EMPTY, child, asList(keys), timestamp(), tiebreaker());
    }

    private static Attribute key(String name) {
        return new FieldAttribute(EMPTY, name, new EsField(name, INTEGER, emptyMap(), true));
    }

    private static void assertOrder(UnaryPlan plan, OrderDirection direction) {
        assertEquals(OrderBy.class, plan.child().getClass());
        OrderBy orderBy = (OrderBy) plan.child();
        Order order = orderBy.order().get(0);
        assertEquals(direction, order.direction());
    }

    private static LogicalPlan defaultPipes(LogicalPlan plan) {
        assertTrue(plan instanceof Project);
        plan = ((Project) plan).child();
        assertTrue(plan instanceof LimitWithOffset);
        plan = ((LimitWithOffset) plan).child();
        assertTrue(plan instanceof OrderBy);
        return ((OrderBy) plan).child();
    }

    private static Sequence sequence(LogicalPlan... rules) {
        List<KeyedFilter> collect = Stream.of(rules)
            .map(r -> r instanceof KeyedFilter ? (KeyedFilter) r : keyedFilter(r))
            .collect(toList());

        return new Sequence(EMPTY, collect, keyedFilter(rel()), TimeValue.MINUS_ONE, timestamp(), tiebreaker(), OrderDirection.ASC);
    }

    private static Expression filterCondition(LogicalPlan plan) {
        assertEquals(Filter.class, plan.getClass());
        Filter f = (Filter) plan;
        return f.condition();
    }

    private static Filter basicFilter(Expression filter) {
        return new Filter(EMPTY, rel(), filter);
    }

    private static Equals equalsExpression() {
        return new Equals(EMPTY, timestamp(), TRUE);
    }

    private static GreaterThan gtExpression(Attribute b) {
        return new GreaterThan(EMPTY, b, new Literal(EMPTY, 1, INTEGER), UTC);
    }
}
