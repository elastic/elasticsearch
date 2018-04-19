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
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.Order;
import org.elasticsearch.xpack.sql.expression.Order.OrderDirection;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.sql.expression.function.scalar.Cast;
import org.elasticsearch.xpack.sql.expression.function.scalar.arithmetic.Add;
import org.elasticsearch.xpack.sql.expression.function.scalar.arithmetic.Div;
import org.elasticsearch.xpack.sql.expression.function.scalar.arithmetic.Mod;
import org.elasticsearch.xpack.sql.expression.function.scalar.arithmetic.Mul;
import org.elasticsearch.xpack.sql.expression.function.scalar.arithmetic.Sub;
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
import org.elasticsearch.xpack.sql.expression.predicate.Equals;
import org.elasticsearch.xpack.sql.expression.predicate.GreaterThan;
import org.elasticsearch.xpack.sql.expression.predicate.GreaterThanOrEqual;
import org.elasticsearch.xpack.sql.expression.predicate.IsNotNull;
import org.elasticsearch.xpack.sql.expression.predicate.LessThan;
import org.elasticsearch.xpack.sql.expression.predicate.LessThanOrEqual;
import org.elasticsearch.xpack.sql.expression.predicate.Not;
import org.elasticsearch.xpack.sql.expression.predicate.Or;
import org.elasticsearch.xpack.sql.expression.predicate.Range;
import org.elasticsearch.xpack.sql.expression.regex.Like;
import org.elasticsearch.xpack.sql.expression.regex.LikePattern;
import org.elasticsearch.xpack.sql.expression.regex.RLike;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.BinaryComparisonSimplification;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.BooleanLiteralsOnTheRight;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.BooleanSimplification;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.CombineProjections;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.ConstantFolding;
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
import org.joda.time.DateTimeZone;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.sql.tree.Location.EMPTY;

public class OptimizerTests extends ESTestCase {

    private static final Expression DUMMY_EXPRESSION = new DummyBooleanExpression(EMPTY, 0);

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
        ShowTables s = new ShowTables(EMPTY, null);
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
        Alias a = new Alias(EMPTY, "a", L(5));
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
        Alias a = new Alias(EMPTY, "a", L(5));
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

    public void testConstantFolding() {
        Expression exp = new Add(EMPTY, L(2), L(3));

        assertTrue(exp.foldable());
        assertTrue(exp instanceof NamedExpression);
        String n = Expressions.name(exp);

        Expression result = new ConstantFolding().rule(exp);
        assertTrue(result instanceof Alias);
        assertEquals(n, Expressions.name(result));
        Expression c = ((Alias) result).child();
        assertTrue(c instanceof Literal);
        assertEquals(5, ((Literal) c).value());

        // check now with an alias
        result = new ConstantFolding().rule(new Alias(EMPTY, "a", exp));
        assertTrue(result instanceof Alias);
        assertEquals("a", Expressions.name(result));
        c = ((Alias) result).child();
        assertTrue(c instanceof Literal);
        assertEquals(5, ((Literal) c).value());
    }

    public void testConstantFoldingBinaryComparison() {
        assertEquals(Literal.FALSE, new ConstantFolding().rule(new GreaterThan(EMPTY, L(2), L(3))));
        assertEquals(Literal.FALSE, new ConstantFolding().rule(new GreaterThanOrEqual(EMPTY, L(2), L(3))));
        assertEquals(Literal.FALSE, new ConstantFolding().rule(new Equals(EMPTY, L(2), L(3))));
        assertEquals(Literal.TRUE, new ConstantFolding().rule(new LessThanOrEqual(EMPTY, L(2), L(3))));
        assertEquals(Literal.TRUE, new ConstantFolding().rule(new LessThan(EMPTY, L(2), L(3))));
    }

    public void testConstantFoldingBinaryLogic() {
        assertEquals(Literal.FALSE, new ConstantFolding().rule(new And(EMPTY, new GreaterThan(EMPTY, L(2), L(3)), Literal.TRUE)));
        assertEquals(Literal.TRUE, new ConstantFolding().rule(new Or(EMPTY, new GreaterThanOrEqual(EMPTY, L(2), L(3)), Literal.TRUE)));
    }

    public void testConstantFoldingRange() {
        assertEquals(Literal.TRUE, new ConstantFolding().rule(new Range(EMPTY, L(5), L(5), true, L(10), false)));
        assertEquals(Literal.FALSE, new ConstantFolding().rule(new Range(EMPTY, L(5), L(5), false, L(10), false)));
    }

    public void testConstantIsNotNull() {
        assertEquals(Literal.FALSE, new ConstantFolding().rule(new IsNotNull(EMPTY, L(null))));
        assertEquals(Literal.TRUE, new ConstantFolding().rule(new IsNotNull(EMPTY, L(5))));
    }

    public void testConstantNot() {
        assertEquals(Literal.FALSE, new ConstantFolding().rule(new Not(EMPTY, Literal.TRUE)));
        assertEquals(Literal.TRUE, new ConstantFolding().rule(new Not(EMPTY, Literal.FALSE)));
    }

    public void testConstantFoldingLikes() {
        assertEquals(Literal.TRUE,
                new ConstantFolding().rule(new Like(EMPTY, Literal.of(EMPTY, "test_emp"), new LikePattern(EMPTY, "test%", (char) 0))));
        assertEquals(Literal.TRUE,
                new ConstantFolding().rule(new RLike(EMPTY, Literal.of(EMPTY, "test_emp"), Literal.of(EMPTY, "test.emp"))));
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
        assertEquals(10, foldFunction(new Add(EMPTY, L(7), L(3))));
        assertEquals(4, foldFunction(new Sub(EMPTY, L(7), L(3))));
        assertEquals(21, foldFunction(new Mul(EMPTY, L(7), L(3))));
        assertEquals(2, foldFunction(new Div(EMPTY, L(7), L(3))));
        assertEquals(1, foldFunction(new Mod(EMPTY, L(7), L(3))));
    }

    public void testMathFolding() {
        assertEquals(7, foldFunction(new Abs(EMPTY, L(7))));
        assertEquals(0d, (double) foldFunction(new ACos(EMPTY, L(1))), 0.01d);
        assertEquals(1.57076d, (double) foldFunction(new ASin(EMPTY, L(1))), 0.01d);
        assertEquals(0.78539d, (double) foldFunction(new ATan(EMPTY, L(1))), 0.01d);
        assertEquals(7, foldFunction(new Floor(EMPTY, L(7))));
        assertEquals(Math.E, foldFunction(new E(EMPTY)));
    }

    private static Object foldFunction(Function f) {
        return unwrapAlias(new ConstantFolding().rule(f));
    }

    private static Object unwrapAlias(Expression e) {
        Alias a = (Alias) e;
        Literal l = (Literal) a.child();
        return l.value();
    }

    public void testBinaryComparisonSimplification() {
        assertEquals(Literal.TRUE, new BinaryComparisonSimplification().rule(new Equals(EMPTY, L(5), L(5))));
        assertEquals(Literal.TRUE, new BinaryComparisonSimplification().rule(new GreaterThanOrEqual(EMPTY, L(5), L(5))));
        assertEquals(Literal.TRUE, new BinaryComparisonSimplification().rule(new LessThanOrEqual(EMPTY, L(5), L(5))));

        assertEquals(Literal.FALSE, new BinaryComparisonSimplification().rule(new GreaterThan(EMPTY, L(5), L(5))));
        assertEquals(Literal.FALSE, new BinaryComparisonSimplification().rule(new LessThan(EMPTY, L(5), L(5))));
    }

    public void testLiteralsOnTheRight() {
        Alias a = new Alias(EMPTY, "a", L(10));
        Expression result = new BooleanLiteralsOnTheRight().rule(new Equals(EMPTY, L(5), a));
        assertTrue(result instanceof Equals);
        Equals eq = (Equals) result;
        assertEquals(a, eq.left());
        assertEquals(L(5), eq.right());
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
}
