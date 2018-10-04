/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.optimizer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.Alias;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.Order;
import org.elasticsearch.xpack.sql.expression.Order.OrderDirection;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.sql.expression.function.scalar.Cast;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DayOfMonth;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DayOfYear;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.MonthOfYear;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.WeekOfYear;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.Year;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.ACos;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.ASin;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.ATan;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.E;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Floor;
import org.elasticsearch.xpack.sql.expression.predicate.And;
import org.elasticsearch.xpack.sql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.sql.expression.predicate.IsNotNull;
import org.elasticsearch.xpack.sql.expression.predicate.Not;
import org.elasticsearch.xpack.sql.expression.predicate.Or;
import org.elasticsearch.xpack.sql.expression.predicate.Range;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.sql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.sql.expression.predicate.regex.LikePattern;
import org.elasticsearch.xpack.sql.expression.predicate.regex.RLike;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.BinaryComparisonSimplification;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.BooleanLiteralsOnTheRight;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.BooleanSimplification;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.CombineBinaryComparisons;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.CombineProjections;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.ConstantFolding;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.PropagateEquals;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.PruneDuplicateFunctions;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.PruneSubqueryAliases;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.ReplaceFoldableAttributes;
import org.elasticsearch.xpack.sql.plan.logical.Filter;
import org.elasticsearch.xpack.sql.plan.logical.LocalRelation;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.plan.logical.OrderBy;
import org.elasticsearch.xpack.sql.plan.logical.Project;
import org.elasticsearch.xpack.sql.plan.logical.SubQueryAlias;
import org.elasticsearch.xpack.sql.plan.logical.command.ShowTables;
import org.elasticsearch.xpack.sql.session.EmptyExecutable;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.EsField;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.sql.tree.Location.EMPTY;

public class OptimizerTests extends ESTestCase {

    private static final Expression DUMMY_EXPRESSION = new DummyBooleanExpression(EMPTY, 0);

    private static final Literal ONE = L(1);
    private static final Literal TWO = L(2);
    private static final Literal THREE = L(3);
    private static final Literal FOUR = L(4);
    private static final Literal FIVE = L(5);
    private static final Literal SIX = L(6);


    public static class DummyBooleanExpression extends Expression {

        private final int id;

        public DummyBooleanExpression(Location location, int id) {
            super(location, Collections.emptyList());
            this.id = id;
        }

        @Override
        protected NodeInfo<? extends Expression> info() {
            return NodeInfo.create(this, DummyBooleanExpression::new, id);
        }

        @Override
        public Expression replaceChildren(List<Expression> newChildren) {
            throw new UnsupportedOperationException("this type of node doesn't have any children");
        }

        @Override
        public boolean nullable() {
            return false;
        }

        @Override
        public DataType dataType() {
            return DataType.BOOLEAN;
        }

        @Override
        public int hashCode() {
            int h = getClass().hashCode();
            h = 31 * h + id;
            return h;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            return id == ((DummyBooleanExpression) obj).id;
        }
    }

    private static LogicalPlan FROM() {
        return new LocalRelation(EMPTY, new EmptyExecutable(emptyList()));
    }

    private static Literal L(Object value) {
        return Literal.of(EMPTY, value);
    }

    public void testPruneSubqueryAliases() {
        ShowTables s = new ShowTables(EMPTY, null, null);
        SubQueryAlias plan = new SubQueryAlias(EMPTY, s, "show");
        LogicalPlan result = new PruneSubqueryAliases().apply(plan);
        assertEquals(result, s);
    }

    public void testDuplicateFunctions() {
        AggregateFunction f1 = new Count(EMPTY, Literal.TRUE, false);
        AggregateFunction f2 = new Count(EMPTY, Literal.TRUE, false);

        assertTrue(f1.functionEquals(f2));

        Project p = new Project(EMPTY, FROM(), Arrays.asList(f1, f2));
        LogicalPlan result = new PruneDuplicateFunctions().apply(p);
        assertTrue(result instanceof Project);
        List<? extends NamedExpression> projections = ((Project) result).projections();
        assertEquals(2, projections.size());
        assertSame(projections.get(0), projections.get(1));
    }

    public void testCombineProjections() {
        // a
        Alias a = new Alias(EMPTY, "a", FIVE);
        // b
        Alias b = new Alias(EMPTY, "b", L(10));
        // x -> a
        Alias x = new Alias(EMPTY, "x", a);

        Project lowerP = new Project(EMPTY, FROM(), asList(a, b));
        Project upperP = new Project(EMPTY, lowerP, singletonList(x));

        LogicalPlan result = new CombineProjections().apply(upperP);
        assertNotSame(upperP, result);

        assertTrue(result instanceof Project);
        Project p = (Project) result;
        assertEquals(1, p.projections().size());
        Alias al = (Alias) p.projections().get(0);
        assertEquals("x", al.name());
        assertTrue(al.child() instanceof Literal);
        assertEquals(5, al.child().fold());
        assertTrue(p.child() instanceof LocalRelation);
    }

    public void testReplaceFoldableAttributes() {
        // SELECT 5 a, 10 b FROM foo WHERE a < 10 ORDER BY b

        // a
        Alias a = new Alias(EMPTY, "a", FIVE);
        // b
        Alias b = new Alias(EMPTY, "b", L(10));
        // WHERE a < 10
        LogicalPlan p = new Filter(EMPTY, FROM(), new LessThan(EMPTY, a, L(10)));
        // SELECT
        p = new Project(EMPTY, p, Arrays.asList(a, b));
        // ORDER BY
        p = new OrderBy(EMPTY, p, singletonList(new Order(EMPTY, b, OrderDirection.ASC)));

        LogicalPlan result = new ReplaceFoldableAttributes().apply(p);
        assertNotSame(p, result);

        // ORDER BY b -> ORDER BY 10
        assertTrue(result instanceof OrderBy);
        OrderBy o = (OrderBy) result;
        assertEquals(1, o.order().size());
        Expression oe = o.order().get(0).child();
        assertTrue(oe instanceof Literal);
        assertEquals(10, oe.fold());

        // WHERE a < 10
        assertTrue(o.child() instanceof Project);
        Project pj = (Project) o.child();
        assertTrue(pj.child() instanceof Filter);
        Filter f = (Filter) pj.child();
        assertTrue(f.condition() instanceof LessThan);
        LessThan lt = (LessThan) f.condition();
        assertTrue(lt.left() instanceof Literal);
        assertTrue(lt.right() instanceof Literal);
        assertEquals(5, lt.left().fold());
        assertEquals(10, lt.right().fold());
    }

    //
    // Constant folding
    //

    public void testConstantFolding() {
        Expression exp = new Add(EMPTY, TWO, THREE);

        assertTrue(exp.foldable());
        Expression result = new ConstantFolding().rule(exp);
        assertTrue(result instanceof Literal);
        assertEquals(5, ((Literal) result).value());

        // check now with an alias
        result = new ConstantFolding().rule(new Alias(EMPTY, "a", exp));
        assertEquals("a", Expressions.name(result));
        assertEquals(5, ((Literal) result).value());
    }

    public void testConstantFoldingBinaryComparison() {
        assertEquals(Literal.FALSE, new ConstantFolding().rule(new GreaterThan(EMPTY, TWO, THREE)).canonical());
        assertEquals(Literal.FALSE, new ConstantFolding().rule(new GreaterThanOrEqual(EMPTY, TWO, THREE)).canonical());
        assertEquals(Literal.FALSE, new ConstantFolding().rule(new Equals(EMPTY, TWO, THREE)).canonical());
        assertEquals(Literal.TRUE, new ConstantFolding().rule(new LessThanOrEqual(EMPTY, TWO, THREE)).canonical());
        assertEquals(Literal.TRUE, new ConstantFolding().rule(new LessThan(EMPTY, TWO, THREE)).canonical());
    }

    public void testConstantFoldingBinaryLogic() {
        assertEquals(Literal.FALSE,
                new ConstantFolding().rule(new And(EMPTY, new GreaterThan(EMPTY, TWO, THREE), Literal.TRUE)).canonical());
        assertEquals(Literal.TRUE,
                new ConstantFolding().rule(new Or(EMPTY, new GreaterThanOrEqual(EMPTY, TWO, THREE), Literal.TRUE)).canonical());
    }

    public void testConstantFoldingRange() {
        assertEquals(true, new ConstantFolding().rule(new Range(EMPTY, FIVE, FIVE, true, L(10), false)).fold());
        assertEquals(false, new ConstantFolding().rule(new Range(EMPTY, FIVE, FIVE, false, L(10), false)).fold());
    }

    public void testConstantIsNotNull() {
        assertEquals(Literal.FALSE, new ConstantFolding().rule(new IsNotNull(EMPTY, L(null))));
        assertEquals(Literal.TRUE, new ConstantFolding().rule(new IsNotNull(EMPTY, FIVE)));
    }

    public void testConstantNot() {
        assertEquals(Literal.FALSE, new ConstantFolding().rule(new Not(EMPTY, Literal.TRUE)));
        assertEquals(Literal.TRUE, new ConstantFolding().rule(new Not(EMPTY, Literal.FALSE)));
    }

    public void testConstantFoldingLikes() {
        assertEquals(Literal.TRUE,
                new ConstantFolding().rule(new Like(EMPTY, Literal.of(EMPTY, "test_emp"), new LikePattern(EMPTY, "test%", (char) 0)))
                        .canonical());
        assertEquals(Literal.TRUE,
                new ConstantFolding().rule(new RLike(EMPTY, Literal.of(EMPTY, "test_emp"), Literal.of(EMPTY, "test.emp"))).canonical());
    }

    public void testConstantFoldingDatetime() {
        final TimeZone UTC = TimeZone.getTimeZone("UTC");
        Expression cast = new Cast(EMPTY, Literal.of(EMPTY, "2018-01-19T10:23:27Z"), DataType.DATE);
        assertEquals(2018, foldFunction(new Year(EMPTY, cast, UTC)));
        assertEquals(1, foldFunction(new MonthOfYear(EMPTY, cast, UTC)));
        assertEquals(19, foldFunction(new DayOfMonth(EMPTY, cast, UTC)));
        assertEquals(19, foldFunction(new DayOfYear(EMPTY, cast, UTC)));
        assertEquals(3, foldFunction(new WeekOfYear(EMPTY, cast, UTC)));
        assertNull(foldFunction(
                new WeekOfYear(EMPTY, new Literal(EMPTY, null, DataType.NULL), UTC)));
    }

    public void testArithmeticFolding() {
        assertEquals(10, foldOperator(new Add(EMPTY, L(7), THREE)));
        assertEquals(4, foldOperator(new Sub(EMPTY, L(7), THREE)));
        assertEquals(21, foldOperator(new Mul(EMPTY, L(7), THREE)));
        assertEquals(2, foldOperator(new Div(EMPTY, L(7), THREE)));
        assertEquals(1, foldOperator(new Mod(EMPTY, L(7), THREE)));
    }

    public void testMathFolding() {
        assertEquals(7, foldFunction(new Abs(EMPTY, L(7))));
        assertEquals(0d, (double) foldFunction(new ACos(EMPTY, ONE)), 0.01d);
        assertEquals(1.57076d, (double) foldFunction(new ASin(EMPTY, ONE)), 0.01d);
        assertEquals(0.78539d, (double) foldFunction(new ATan(EMPTY, ONE)), 0.01d);
        assertEquals(7, foldFunction(new Floor(EMPTY, L(7))));
        assertEquals(Math.E, foldFunction(new E(EMPTY)));
    }

    private static Object foldFunction(Function f) {
        return ((Literal) new ConstantFolding().rule(f)).value();
    }

    private static Object foldOperator(BinaryOperator b) {
        return ((Literal) new ConstantFolding().rule(b)).value();
    }

    //
    // Logical simplifications
    //

    public void testBinaryComparisonSimplification() {
        assertEquals(Literal.TRUE, new BinaryComparisonSimplification().rule(new Equals(EMPTY, FIVE, FIVE)));
        assertEquals(Literal.TRUE, new BinaryComparisonSimplification().rule(new GreaterThanOrEqual(EMPTY, FIVE, FIVE)));
        assertEquals(Literal.TRUE, new BinaryComparisonSimplification().rule(new LessThanOrEqual(EMPTY, FIVE, FIVE)));

        assertEquals(Literal.FALSE, new BinaryComparisonSimplification().rule(new GreaterThan(EMPTY, FIVE, FIVE)));
        assertEquals(Literal.FALSE, new BinaryComparisonSimplification().rule(new LessThan(EMPTY, FIVE, FIVE)));
    }

    public void testLiteralsOnTheRight() {
        Alias a = new Alias(EMPTY, "a", L(10));
        Expression result = new BooleanLiteralsOnTheRight().rule(new Equals(EMPTY, FIVE, a));
        assertTrue(result instanceof Equals);
        Equals eq = (Equals) result;
        assertEquals(a, eq.left());
        assertEquals(FIVE, eq.right());
    }

    public void testBoolSimplifyOr() {
        BooleanSimplification simplification = new BooleanSimplification();

        assertEquals(Literal.TRUE, simplification.rule(new Or(EMPTY, Literal.TRUE, Literal.TRUE)));
        assertEquals(Literal.TRUE, simplification.rule(new Or(EMPTY, Literal.TRUE, DUMMY_EXPRESSION)));
        assertEquals(Literal.TRUE, simplification.rule(new Or(EMPTY, DUMMY_EXPRESSION, Literal.TRUE)));

        assertEquals(Literal.FALSE, simplification.rule(new Or(EMPTY, Literal.FALSE, Literal.FALSE)));
        assertEquals(DUMMY_EXPRESSION, simplification.rule(new Or(EMPTY, Literal.FALSE, DUMMY_EXPRESSION)));
        assertEquals(DUMMY_EXPRESSION, simplification.rule(new Or(EMPTY, DUMMY_EXPRESSION, Literal.FALSE)));
    }

    public void testBoolSimplifyAnd() {
        BooleanSimplification simplification = new BooleanSimplification();

        assertEquals(Literal.TRUE, simplification.rule(new And(EMPTY, Literal.TRUE, Literal.TRUE)));
        assertEquals(DUMMY_EXPRESSION, simplification.rule(new And(EMPTY, Literal.TRUE, DUMMY_EXPRESSION)));
        assertEquals(DUMMY_EXPRESSION, simplification.rule(new And(EMPTY, DUMMY_EXPRESSION, Literal.TRUE)));

        assertEquals(Literal.FALSE, simplification.rule(new And(EMPTY, Literal.FALSE, Literal.FALSE)));
        assertEquals(Literal.FALSE, simplification.rule(new And(EMPTY, Literal.FALSE, DUMMY_EXPRESSION)));
        assertEquals(Literal.FALSE, simplification.rule(new And(EMPTY, DUMMY_EXPRESSION, Literal.FALSE)));
    }

    public void testBoolCommonFactorExtraction() {
        BooleanSimplification simplification = new BooleanSimplification();

        Expression a1 = new DummyBooleanExpression(EMPTY, 1);
        Expression a2 = new DummyBooleanExpression(EMPTY, 1);
        Expression b = new DummyBooleanExpression(EMPTY, 2);
        Expression c = new DummyBooleanExpression(EMPTY, 3);

        Expression actual = new Or(EMPTY, new And(EMPTY, a1, b), new And(EMPTY, a2, c));
        Expression expected = new And(EMPTY, a1, new Or(EMPTY, b, c));

        assertEquals(expected, simplification.rule(actual));
    }

    //
    // Range optimization
    //

    // 6 < a <= 5  -> FALSE
    public void testFoldExcludingRangeToFalse() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));

        Range r = new Range(EMPTY, fa, SIX, false, FIVE, true);
        assertTrue(r.foldable());
        assertEquals(Boolean.FALSE, r.fold());
    }

    // 6 < a <= 5.5 -> FALSE
    public void testFoldExcludingRangeWithDifferentTypesToFalse() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));

        Range r = new Range(EMPTY, fa, SIX, false, L(5.5d), true);
        assertTrue(r.foldable());
        assertEquals(Boolean.FALSE, r.fold());
    }

    // Conjunction

    public void testCombineBinaryComparisonsNotComparable() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));
        LessThanOrEqual lte = new LessThanOrEqual(EMPTY, fa, SIX);
        LessThan lt = new LessThan(EMPTY, fa, Literal.FALSE);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        And and = new And(EMPTY, lte, lt);
        Expression exp = rule.rule(and);
        assertEquals(exp, and);
    }

    // a <= 6 AND a < 5  -> a < 5
    public void testCombineBinaryComparisonsUpper() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));
        LessThanOrEqual lte = new LessThanOrEqual(EMPTY, fa, SIX);
        LessThan lt = new LessThan(EMPTY, fa, FIVE);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();

        Expression exp = rule.rule(new And(EMPTY, lte, lt));
        assertEquals(LessThan.class, exp.getClass());
        LessThan r = (LessThan) exp;
        assertEquals(FIVE, r.right());
    }

    // 6 <= a AND 5 < a  -> 6 <= a
    public void testCombineBinaryComparisonsLower() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));
        GreaterThanOrEqual gte = new GreaterThanOrEqual(EMPTY, fa, SIX);
        GreaterThan gt = new GreaterThan(EMPTY, fa, FIVE);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();

        Expression exp = rule.rule(new And(EMPTY, gte, gt));
        assertEquals(GreaterThanOrEqual.class, exp.getClass());
        GreaterThanOrEqual r = (GreaterThanOrEqual) exp;
        assertEquals(SIX, r.right());
    }

    // 5 <= a AND 5 < a  -> 5 < a
    public void testCombineBinaryComparisonsInclude() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));
        GreaterThanOrEqual gte = new GreaterThanOrEqual(EMPTY, fa, FIVE);
        GreaterThan gt = new GreaterThan(EMPTY, fa, FIVE);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();

        Expression exp = rule.rule(new And(EMPTY, gte, gt));
        assertEquals(GreaterThan.class, exp.getClass());
        GreaterThan r = (GreaterThan) exp;
        assertEquals(FIVE, r.right());
    }

    // 3 <= a AND 4 < a AND a <= 7 AND a < 6 -> 4 < a < 6
    public void testCombineMultipleBinaryComparisons() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));
        GreaterThanOrEqual gte = new GreaterThanOrEqual(EMPTY, fa, THREE);
        GreaterThan gt = new GreaterThan(EMPTY, fa, FOUR);
        LessThanOrEqual lte = new LessThanOrEqual(EMPTY, fa, L(7));
        LessThan lt = new LessThan(EMPTY, fa, SIX);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();

        Expression exp = rule.rule(new And(EMPTY, gte, new And(EMPTY, gt, new And(EMPTY, lt, lte))));
        assertEquals(Range.class, exp.getClass());
        Range r = (Range) exp;
        assertEquals(FOUR, r.lower());
        assertFalse(r.includeLower());
        assertEquals(SIX, r.upper());
        assertFalse(r.includeUpper());
    }

    // 3 <= a AND TRUE AND 4 < a AND a != 5 AND a <= 7 -> 4 < a <= 7 AND a != 5 AND TRUE
    public void testCombineMixedMultipleBinaryComparisons() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));
        GreaterThanOrEqual gte = new GreaterThanOrEqual(EMPTY, fa, THREE);
        GreaterThan gt = new GreaterThan(EMPTY, fa, FOUR);
        LessThanOrEqual lte = new LessThanOrEqual(EMPTY, fa, L(7));
        Expression ne = new Not(EMPTY, new Equals(EMPTY, fa, FIVE));

        CombineBinaryComparisons rule = new CombineBinaryComparisons();

        // TRUE AND a != 5 AND 4 < a <= 7
        Expression exp = rule.rule(new And(EMPTY, gte, new And(EMPTY, Literal.TRUE, new And(EMPTY, gt, new And(EMPTY, ne, lte)))));
        assertEquals(And.class, exp.getClass());
        And and = ((And) exp);
        assertEquals(Range.class, and.right().getClass());
        Range r = (Range) and.right();
        assertEquals(FOUR, r.lower());
        assertFalse(r.includeLower());
        assertEquals(L(7), r.upper());
        assertTrue(r.includeUpper());
    }

    // 1 <= a AND a < 5  -> 1 <= a < 5
    public void testCombineComparisonsIntoRange() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));
        GreaterThanOrEqual gte = new GreaterThanOrEqual(EMPTY, fa, ONE);
        LessThan lt = new LessThan(EMPTY, fa, FIVE);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(new And(EMPTY, gte, lt));
        assertEquals(Range.class, rule.rule(exp).getClass());

        Range r = (Range) exp;
        assertEquals(ONE, r.lower());
        assertTrue(r.includeLower());
        assertEquals(FIVE, r.upper());
        assertFalse(r.includeUpper());
    }

    // a != NULL AND a > 1 AND a < 5 AND a == 10  -> (a != NULL AND a == 10) AND 1 <= a < 5
    public void testCombineUnbalancedComparisonsMixedWithEqualsIntoRange() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));
        IsNotNull isn = new IsNotNull(EMPTY, fa);
        GreaterThanOrEqual gte = new GreaterThanOrEqual(EMPTY, fa, ONE);

        Equals eq = new Equals(EMPTY, fa, L(10));
        LessThan lt = new LessThan(EMPTY, fa, FIVE);

        And and = new And(EMPTY, new And(EMPTY, isn, gte), new And(EMPTY, lt, eq));

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(And.class, exp.getClass());
        And a = (And) exp;
        assertEquals(Range.class, a.right().getClass());

        Range r = (Range) a.right();
        assertEquals(ONE, r.lower());
        assertTrue(r.includeLower());
        assertEquals(FIVE, r.upper());
        assertFalse(r.includeUpper());
    }

    // (2 < a < 3) AND (1 < a < 4) -> (2 < a < 3)
    public void testCombineBinaryComparisonsConjunctionOfIncludedRange() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));

        Range r1 = new Range(EMPTY, fa, TWO, false, THREE, false);
        Range r2 = new Range(EMPTY, fa, ONE, false, FOUR, false);

        And and = new And(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(r1, exp);
    }

    // (2 < a < 3) AND a < 2 -> 2 < a < 2
    public void testCombineBinaryComparisonsConjunctionOfNonOverlappingBoundaries() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));

        Range r1 = new Range(EMPTY, fa, TWO, false, THREE, false);
        Range r2 = new Range(EMPTY, fa, ONE, false, TWO, false);

        And and = new And(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(Range.class, exp.getClass());
        Range r = (Range) exp;
        assertEquals(TWO, r.lower());
        assertFalse(r.includeLower());
        assertEquals(TWO, r.upper());
        assertFalse(r.includeUpper());
        assertEquals(Boolean.FALSE, r.fold());
    }

    // (2 < a < 3) AND (2 < a <= 3) -> 2 < a < 3
    public void testCombineBinaryComparisonsConjunctionOfUpperEqualsOverlappingBoundaries() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));

        Range r1 = new Range(EMPTY, fa, TWO, false, THREE, false);
        Range r2 = new Range(EMPTY, fa, TWO, false, THREE, true);

        And and = new And(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(r1, exp);
    }

    // (2 < a < 3) AND (1 < a < 3) -> 2 < a < 3
    public void testCombineBinaryComparisonsConjunctionOverlappingUpperBoundary() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));

        Range r2 = new Range(EMPTY, fa, TWO, false, THREE, false);
        Range r1 = new Range(EMPTY, fa, ONE, false, THREE, false);

        And and = new And(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(r2, exp);
    }

    // (2 < a <= 3) AND (1 < a < 3) -> 2 < a < 3
    public void testCombineBinaryComparisonsConjunctionWithDifferentUpperLimitInclusion() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));

        Range r1 = new Range(EMPTY, fa, ONE, false, THREE, false);
        Range r2 = new Range(EMPTY, fa, TWO, false, THREE, true);

        And and = new And(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(Range.class, exp.getClass());
        Range r = (Range) exp;
        assertEquals(TWO, r.lower());
        assertFalse(r.includeLower());
        assertEquals(THREE, r.upper());
        assertFalse(r.includeUpper());
    }

    // (0 < a <= 1) AND (0 <= a < 2) -> 0 < a <= 1
    public void testRangesOverlappingConjunctionNoLowerBoundary() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));

        Range r1 = new Range(EMPTY, fa, L(0), false, ONE, true);
        Range r2 = new Range(EMPTY, fa, L(0), true, TWO, false);

        And and = new And(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(r1, exp);
    }

    // Disjunction

    public void testCombineBinaryComparisonsDisjunctionNotComparable() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));

        GreaterThan gt1 = new GreaterThan(EMPTY, fa, ONE);
        GreaterThan gt2 = new GreaterThan(EMPTY, fa, Literal.FALSE);

        Or or = new Or(EMPTY, gt1, gt2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(exp, or);
    }


    // 2 < a OR 1 < a OR 3 < a -> 1 < a
    public void testCombineBinaryComparisonsDisjunctionLowerBound() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));

        GreaterThan gt1 = new GreaterThan(EMPTY, fa, ONE);
        GreaterThan gt2 = new GreaterThan(EMPTY, fa, TWO);
        GreaterThan gt3 = new GreaterThan(EMPTY, fa, THREE);

        Or or = new Or(EMPTY, gt1, new Or(EMPTY, gt2, gt3));

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(GreaterThan.class, exp.getClass());

        GreaterThan gt = (GreaterThan) exp;
        assertEquals(ONE, gt.right());
    }

    // 2 < a OR 1 < a OR 3 <= a -> 1 < a
    public void testCombineBinaryComparisonsDisjunctionIncludeLowerBounds() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));

        GreaterThan gt1 = new GreaterThan(EMPTY, fa, ONE);
        GreaterThan gt2 = new GreaterThan(EMPTY, fa, TWO);
        GreaterThanOrEqual gte3 = new GreaterThanOrEqual(EMPTY, fa, THREE);

        Or or = new Or(EMPTY, new Or(EMPTY, gt1, gt2), gte3);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(GreaterThan.class, exp.getClass());

        GreaterThan gt = (GreaterThan) exp;
        assertEquals(ONE, gt.right());
    }

    // a < 1 OR a < 2 OR a < 3 ->  a < 3
    public void testCombineBinaryComparisonsDisjunctionUpperBound() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));

        LessThan lt1 = new LessThan(EMPTY, fa, ONE);
        LessThan lt2 = new LessThan(EMPTY, fa, TWO);
        LessThan lt3 = new LessThan(EMPTY, fa, THREE);

        Or or = new Or(EMPTY, new Or(EMPTY, lt1, lt2), lt3);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(LessThan.class, exp.getClass());

        LessThan lt = (LessThan) exp;
        assertEquals(THREE, lt.right());
    }

    // a < 2 OR a <= 2 OR a < 1 ->  a <= 2
    public void testCombineBinaryComparisonsDisjunctionIncludeUpperBounds() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));

        LessThan lt1 = new LessThan(EMPTY, fa, ONE);
        LessThan lt2 = new LessThan(EMPTY, fa, TWO);
        LessThanOrEqual lte2 = new LessThanOrEqual(EMPTY, fa, TWO);

        Or or = new Or(EMPTY, lt2, new Or(EMPTY, lte2, lt1));

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(LessThanOrEqual.class, exp.getClass());

        LessThanOrEqual lte = (LessThanOrEqual) exp;
        assertEquals(TWO, lte.right());
    }

    // a < 2 OR 3 < a OR a < 1 OR 4 < a ->  a < 2 OR 3 < a
    public void testCombineBinaryComparisonsDisjunctionOfLowerAndUpperBounds() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));

        LessThan lt1 = new LessThan(EMPTY, fa, ONE);
        LessThan lt2 = new LessThan(EMPTY, fa, TWO);

        GreaterThan gt3 = new GreaterThan(EMPTY, fa, THREE);
        GreaterThan gt4 = new GreaterThan(EMPTY, fa, FOUR);

        Or or = new Or(EMPTY, new Or(EMPTY, lt2, gt3), new Or(EMPTY, lt1, gt4));

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(Or.class, exp.getClass());

        Or ro = (Or) exp;

        assertEquals(LessThan.class, ro.left().getClass());
        LessThan lt = (LessThan) ro.left();
        assertEquals(TWO, lt.right());
        assertEquals(GreaterThan.class, ro.right().getClass());
        GreaterThan gt = (GreaterThan) ro.right();
        assertEquals(THREE, gt.right());
    }

    // (2 < a < 3) OR (1 < a < 4) -> (1 < a < 4)
    public void testCombineBinaryComparisonsDisjunctionOfIncludedRangeNotComparable() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));

        Range r1 = new Range(EMPTY, fa, TWO, false, THREE, false);
        Range r2 = new Range(EMPTY, fa, ONE, false, Literal.FALSE, false);

        Or or = new Or(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(or, exp);
    }


    // (2 < a < 3) OR (1 < a < 4) -> (1 < a < 4)
    public void testCombineBinaryComparisonsDisjunctionOfIncludedRange() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));


        Range r1 = new Range(EMPTY, fa, TWO, false, THREE, false);
        Range r2 = new Range(EMPTY, fa, ONE, false, FOUR, false);

        Or or = new Or(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(Range.class, exp.getClass());

        Range r = (Range) exp;
        assertEquals(ONE, r.lower());
        assertFalse(r.includeLower());
        assertEquals(FOUR, r.upper());
        assertFalse(r.includeUpper());
    }

    // (2 < a < 3) OR (1 < a < 2) -> same
    public void testCombineBinaryComparisonsDisjunctionOfNonOverlappingBoundaries() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));

        Range r1 = new Range(EMPTY, fa, TWO, false, THREE, false);
        Range r2 = new Range(EMPTY, fa, ONE, false, TWO, false);

        Or or = new Or(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(or, exp);
    }

    // (2 < a < 3) OR (2 < a <= 3) -> 2 < a <= 3
    public void testCombineBinaryComparisonsDisjunctionOfUpperEqualsOverlappingBoundaries() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));

        Range r1 = new Range(EMPTY, fa, TWO, false, THREE, false);
        Range r2 = new Range(EMPTY, fa, TWO, false, THREE, true);

        Or or = new Or(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(r2, exp);
    }

    // (2 < a < 3) OR (1 < a < 3) -> 1 < a < 3
    public void testCombineBinaryComparisonsOverlappingUpperBoundary() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));

        Range r2 = new Range(EMPTY, fa, TWO, false, THREE, false);
        Range r1 = new Range(EMPTY, fa, ONE, false, THREE, false);

        Or or = new Or(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(r1, exp);
    }

    // (2 < a <= 3) OR (1 < a < 3) -> same (the <= prevents the ranges from being combined)
    public void testCombineBinaryComparisonsWithDifferentUpperLimitInclusion() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));

        Range r1 = new Range(EMPTY, fa, ONE, false, THREE, false);
        Range r2 = new Range(EMPTY, fa, TWO, false, THREE, true);

        Or or = new Or(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(or, exp);
    }

    // (0 < a <= 1) OR (0 < a < 2) -> 0 < a < 2
    public void testRangesOverlappingNoLowerBoundary() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));

        Range r2 = new Range(EMPTY, fa, L(0), false, TWO, false);
        Range r1 = new Range(EMPTY, fa, L(0), false, ONE, true);

        Or or = new Or(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(r2, exp);
    }

    // Equals

    // a == 1 AND a == 2 -> FALSE
    public void testDualEqualsConjunction() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));
        Equals eq1 = new Equals(EMPTY, fa, ONE);
        Equals eq2 = new Equals(EMPTY, fa, TWO);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq1, eq2));
        assertEquals(Literal.FALSE, rule.rule(exp));
    }

    // 1 <= a < 10 AND a == 1 -> a == 1
    public void testEliminateRangeByEqualsInInterval() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));
        Equals eq1 = new Equals(EMPTY, fa, ONE);
        Range r = new Range(EMPTY, fa, ONE, true, L(10), false);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq1, r));
        assertEquals(eq1, rule.rule(exp));
    }

    // 1 < a < 10 AND a == 10 -> FALSE
    public void testEliminateRangeByEqualsOutsideInterval() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "a", new EsField("af", DataType.INTEGER, emptyMap(), true));
        Equals eq1 = new Equals(EMPTY, fa, L(10));
        Range r = new Range(EMPTY, fa, ONE, false, L(10), false);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq1, r));
        assertEquals(Literal.FALSE, rule.rule(exp));
    }
}