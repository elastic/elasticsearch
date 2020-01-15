/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.optimizer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expression.TypeResolution;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Foldables;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.Order.OrderDirection;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.function.aggregate.InnerAggregate;
import org.elasticsearch.xpack.ql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.ql.expression.predicate.Predicates;
import org.elasticsearch.xpack.ql.expression.predicate.Range;
import org.elasticsearch.xpack.ql.expression.predicate.conditional.ArbitraryConditionalFunction;
import org.elasticsearch.xpack.ql.expression.predicate.conditional.Case;
import org.elasticsearch.xpack.ql.expression.predicate.conditional.Coalesce;
import org.elasticsearch.xpack.ql.expression.predicate.conditional.ConditionalFunction;
import org.elasticsearch.xpack.ql.expression.predicate.conditional.Greatest;
import org.elasticsearch.xpack.ql.expression.predicate.conditional.IfConditional;
import org.elasticsearch.xpack.ql.expression.predicate.conditional.IfNull;
import org.elasticsearch.xpack.ql.expression.predicate.conditional.Iif;
import org.elasticsearch.xpack.ql.expression.predicate.conditional.Least;
import org.elasticsearch.xpack.ql.expression.predicate.conditional.NullIf;
import org.elasticsearch.xpack.ql.expression.predicate.fulltext.FullTextPredicate;
import org.elasticsearch.xpack.ql.expression.predicate.fulltext.MatchQueryPredicate;
import org.elasticsearch.xpack.ql.expression.predicate.fulltext.MultiMatchQueryPredicate;
import org.elasticsearch.xpack.ql.expression.predicate.fulltext.StringQueryPredicate;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NullEquals;
import org.elasticsearch.xpack.ql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.ql.expression.predicate.regex.LikePattern;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RLike;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.util.CollectionUtils;
import org.elasticsearch.xpack.ql.util.StringUtils;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer.PruneSubqueryAliases;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.sql.expression.function.aggregate.ExtendedStats;
import org.elasticsearch.xpack.sql.expression.function.aggregate.First;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Last;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Stats;
import org.elasticsearch.xpack.sql.expression.function.aggregate.StddevPop;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.sql.expression.function.aggregate.SumOfSquares;
import org.elasticsearch.xpack.sql.expression.function.aggregate.VarPop;
import org.elasticsearch.xpack.sql.expression.function.scalar.Cast;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateAdd;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DatePart;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTrunc;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DayName;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DayOfMonth;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DayOfYear;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.IsoWeekOfYear;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.MonthOfYear;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.Year;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.StDistance;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.ACos;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.ASin;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.ATan;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Cos;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.E;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.Floor;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.Ascii;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.Repeat;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.BinaryComparisonSimplification;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.BooleanLiteralsOnTheRight;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.BooleanSimplification;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.CombineBinaryComparisons;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.CombineProjections;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.ConstantFolding;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.FoldNull;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.PropagateEquals;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.ReplaceAggsWithExtendedStats;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.ReplaceAggsWithStats;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.ReplaceFoldableAttributes;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.ReplaceMinMaxWithTopHits;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.ReplaceReferenceAttributeWithSource;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.RewritePivot;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.SimplifyCase;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.SimplifyConditional;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.SortAggregateOnOrderBy;
import org.elasticsearch.xpack.sql.plan.logical.LocalRelation;
import org.elasticsearch.xpack.sql.plan.logical.Pivot;
import org.elasticsearch.xpack.sql.plan.logical.SubQueryAlias;
import org.elasticsearch.xpack.sql.plan.logical.command.ShowTables;
import org.elasticsearch.xpack.sql.session.EmptyExecutable;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.ql.expression.Literal.FALSE;
import static org.elasticsearch.xpack.ql.expression.Literal.NULL;
import static org.elasticsearch.xpack.ql.expression.Literal.TRUE;
import static org.elasticsearch.xpack.ql.expression.Literal.of;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

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

        public DummyBooleanExpression(Source source, int id) {
            super(source, Collections.emptyList());
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
        public Nullability nullable() {
            return Nullability.FALSE;
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
        return of(EMPTY, value);
    }

    private static Alias a(String name, Expression e) {
        return new Alias(e.source(), name, e);
    }

    private static FieldAttribute getFieldAttribute() {
        return getFieldAttribute("a");
    }

    private static FieldAttribute getFieldAttribute(String name) {
        return new FieldAttribute(EMPTY, name, new EsField(name + "f", DataType.INTEGER, emptyMap(), true));
    }

    public void testPruneSubqueryAliases() {
        ShowTables s = new ShowTables(EMPTY, null, null, false);
        SubQueryAlias plan = new SubQueryAlias(EMPTY, s, "show");
        LogicalPlan result = new PruneSubqueryAliases().apply(plan);
        assertEquals(result, s);
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
        p = new OrderBy(EMPTY, p, singletonList(new Order(EMPTY, b, OrderDirection.ASC, null)));

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
        assertEquals(Alias.class, result.getClass());
    }

    public void testConstantFoldingBinaryComparison() {
        assertEquals(FALSE, new ConstantFolding().rule(new GreaterThan(EMPTY, TWO, THREE)).canonical());
        assertEquals(FALSE, new ConstantFolding().rule(new GreaterThanOrEqual(EMPTY, TWO, THREE)).canonical());
        assertEquals(FALSE, new ConstantFolding().rule(new Equals(EMPTY, TWO, THREE)).canonical());
        assertEquals(FALSE, new ConstantFolding().rule(new NullEquals(EMPTY, TWO, THREE)).canonical());
        assertEquals(FALSE, new ConstantFolding().rule(new NullEquals(EMPTY, TWO, NULL)).canonical());
        assertEquals(TRUE, new ConstantFolding().rule(new NotEquals(EMPTY, TWO, THREE)).canonical());
        assertEquals(TRUE, new ConstantFolding().rule(new LessThanOrEqual(EMPTY, TWO, THREE)).canonical());
        assertEquals(TRUE, new ConstantFolding().rule(new LessThan(EMPTY, TWO, THREE)).canonical());
    }

    public void testConstantFoldingBinaryLogic() {
        assertEquals(FALSE,
                new ConstantFolding().rule(new And(EMPTY, new GreaterThan(EMPTY, TWO, THREE), TRUE)).canonical());
        assertEquals(TRUE,
                new ConstantFolding().rule(new Or(EMPTY, new GreaterThanOrEqual(EMPTY, TWO, THREE), TRUE)).canonical());
    }

    public void testConstantFoldingBinaryLogic_WithNullHandling() {
        assertEquals(Nullability.TRUE, new ConstantFolding().rule(new And(EMPTY, NULL, TRUE)).canonical().nullable());
        assertEquals(Nullability.TRUE, new ConstantFolding().rule(new And(EMPTY, TRUE, NULL)).canonical().nullable());
        assertEquals(FALSE, new ConstantFolding().rule(new And(EMPTY, NULL, FALSE)).canonical());
        assertEquals(FALSE, new ConstantFolding().rule(new And(EMPTY, FALSE, NULL)).canonical());
        assertEquals(Nullability.TRUE, new ConstantFolding().rule(new And(EMPTY, NULL, NULL)).canonical().nullable());

        assertEquals(TRUE, new ConstantFolding().rule(new Or(EMPTY, NULL, TRUE)).canonical());
        assertEquals(TRUE, new ConstantFolding().rule(new Or(EMPTY, TRUE, NULL)).canonical());
        assertEquals(Nullability.TRUE, new ConstantFolding().rule(new Or(EMPTY, NULL, FALSE)).canonical().nullable());
        assertEquals(Nullability.TRUE, new ConstantFolding().rule(new Or(EMPTY, FALSE, NULL)).canonical().nullable());
        assertEquals(Nullability.TRUE, new ConstantFolding().rule(new Or(EMPTY, NULL, NULL)).canonical().nullable());
    }

    public void testConstantFoldingRange() {
        assertEquals(true, new ConstantFolding().rule(new Range(EMPTY, FIVE, FIVE, true, L(10), false)).fold());
        assertEquals(false, new ConstantFolding().rule(new Range(EMPTY, FIVE, FIVE, false, L(10), false)).fold());
    }

    public void testConstantIsNotNull() {
        assertEquals(FALSE, new ConstantFolding().rule(new IsNotNull(EMPTY, L(null))));
        assertEquals(TRUE, new ConstantFolding().rule(new IsNotNull(EMPTY, FIVE)));
    }

    public void testConstantNot() {
        assertEquals(FALSE, new ConstantFolding().rule(new Not(EMPTY, TRUE)));
        assertEquals(TRUE, new ConstantFolding().rule(new Not(EMPTY, FALSE)));
    }

    public void testConstantFoldingLikes() {
        assertEquals(TRUE,
                new ConstantFolding().rule(new Like(EMPTY, of(EMPTY, "test_emp"), new LikePattern("test%", (char) 0)))
                        .canonical());
        assertEquals(TRUE,
                new ConstantFolding().rule(new RLike(EMPTY, of(EMPTY, "test_emp"), "test.emp")).canonical());
    }

    public void testConstantFoldingDatetime() {
        Expression cast = new Cast(EMPTY, of(EMPTY, "2018-01-19T10:23:27Z"), DataType.DATETIME);
        assertEquals(2018, foldFunction(new Year(EMPTY, cast, UTC)));
        assertEquals(1, foldFunction(new MonthOfYear(EMPTY, cast, UTC)));
        assertEquals(19, foldFunction(new DayOfMonth(EMPTY, cast, UTC)));
        assertEquals(19, foldFunction(new DayOfYear(EMPTY, cast, UTC)));
        assertEquals(3, foldFunction(new IsoWeekOfYear(EMPTY, cast, UTC)));
        assertNull(foldFunction(
                new IsoWeekOfYear(EMPTY, new Literal(EMPTY, null, DataType.NULL), UTC)));
    }

    public void testConstantFoldingIn() {
        In in = new In(EMPTY, ONE,
            Arrays.asList(ONE, TWO, ONE, THREE, new Sub(EMPTY, THREE, ONE), ONE, FOUR, new Abs(EMPTY, new Sub(EMPTY, TWO, FIVE))));
        Literal result= (Literal) new ConstantFolding().rule(in);
        assertEquals(true, result.value());
    }

    public void testConstantFoldingIn_LeftValueNotFoldable() {
        In in = new In(EMPTY, getFieldAttribute(),
                Arrays.asList(ONE, TWO, ONE, THREE, new Sub(EMPTY, THREE, ONE), ONE, FOUR, new Abs(EMPTY, new Sub(EMPTY, TWO, FIVE))));
        Alias as = new Alias(in.source(), in.sourceText(), in);
        Project p = new Project(EMPTY, FROM(), Collections.singletonList(as));
        p = (Project) new ConstantFolding().apply(p);
        assertEquals(1, p.projections().size());
        Alias a = (Alias) p.projections().get(0);
        In i = (In) a.child();
        assertThat(Foldables.valuesOf(i.list(), DataType.INTEGER), contains(1 ,2 ,3 ,4));
    }

    public void testConstantFoldingIn_RightValueIsNull() {
        In in = new In(EMPTY, getFieldAttribute(), Arrays.asList(NULL, NULL));
        Literal result= (Literal) new ConstantFolding().rule(in);
        assertNull(result.value());
    }

    public void testConstantFoldingIn_LeftValueIsNull() {
        In in = new In(EMPTY, NULL, Arrays.asList(ONE, TWO, THREE));
        Literal result= (Literal) new ConstantFolding().rule(in);
        assertNull(result.value());
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

    private static Object foldOperator(BinaryOperator<?, ?, ?, ?> b) {
        return ((Literal) new ConstantFolding().rule(b)).value();
    }

    // Null folding

    public void testNullFoldingIsNull() {
        FoldNull foldNull = new FoldNull();
        assertEquals(true, foldNull.rule(new IsNull(EMPTY, NULL)).fold());
        assertEquals(false, foldNull.rule(new IsNull(EMPTY, TRUE)).fold());
    }

    public void testNullFoldingIsNullWithCast() {
        FoldNull foldNull = new FoldNull();

        Cast cast = new Cast(EMPTY, L("foo"), DataType.DATE);
        IsNull isNull = new IsNull(EMPTY, cast);
        final IsNull isNullOpt = (IsNull) foldNull.rule(isNull);
        assertEquals(isNull, isNullOpt);

        QlIllegalArgumentException sqlIAE = expectThrows(QlIllegalArgumentException.class,
                () -> isNullOpt.asPipe().asProcessor().process(null));
        assertEquals("cannot cast [foo] to [date]: Text 'foo' could not be parsed at index 0", sqlIAE.getMessage());

        isNull = new IsNull(EMPTY, new Cast(EMPTY, NULL, randomFrom(DataType.values())));
        assertTrue((Boolean) ((IsNull) foldNull.rule(isNull)).asPipe().asProcessor().process(null));
    }

    public void testNullFoldingIsNotNull() {
        FoldNull foldNull = new FoldNull();
        assertEquals(true, foldNull.rule(new IsNotNull(EMPTY, TRUE)).fold());
        assertEquals(false, foldNull.rule(new IsNotNull(EMPTY, NULL)).fold());

        Cast cast = new Cast(EMPTY, L("foo"), DataType.DATE);
        IsNotNull isNotNull = new IsNotNull(EMPTY, cast);
        assertEquals(isNotNull, foldNull.rule(isNotNull));
    }

    public void testNullFoldingIsNotNullWithCast() {
        FoldNull foldNull = new FoldNull();

        Cast cast = new Cast(EMPTY, L("foo"), DataType.DATE);
        IsNotNull isNotNull = new IsNotNull(EMPTY, cast);
        final IsNotNull isNotNullOpt = (IsNotNull) foldNull.rule(isNotNull);
        assertEquals(isNotNull, isNotNullOpt);

        QlIllegalArgumentException sqlIAE = expectThrows(QlIllegalArgumentException.class,
                () -> isNotNullOpt.asPipe().asProcessor().process(null));
        assertEquals("cannot cast [foo] to [date]: Text 'foo' could not be parsed at index 0", sqlIAE.getMessage());

        isNotNull = new IsNotNull(EMPTY, new Cast(EMPTY, NULL, randomFrom(DataType.values())));
        assertFalse((Boolean) ((IsNotNull) foldNull.rule(isNotNull)).asPipe().asProcessor().process(null));
    }

    public void testGenericNullableExpression() {
        FoldNull rule = new FoldNull();
        // date-time
        assertNullLiteral(rule.rule(new DayName(EMPTY, NULL, randomZone())));
        // math function
        assertNullLiteral(rule.rule(new Cos(EMPTY, NULL)));
        // string function
        assertNullLiteral(rule.rule(new Ascii(EMPTY, NULL)));
        assertNullLiteral(rule.rule(new Repeat(EMPTY, getFieldAttribute(), NULL)));
        // arithmetic
        assertNullLiteral(rule.rule(new Add(EMPTY, getFieldAttribute(), NULL)));
        // comparison
        assertNullLiteral(rule.rule(new GreaterThan(EMPTY, getFieldAttribute(), NULL)));
        // regex
        assertNullLiteral(rule.rule(new RLike(EMPTY, NULL, "123")));
    }

    public void testNullFoldingOnCast() {
        FoldNull foldNull = new FoldNull();

        Cast cast = new Cast(EMPTY, NULL, randomFrom(DataType.values()));
        assertEquals(Nullability.TRUE, cast.nullable());
        assertNull(foldNull.rule(cast).fold());

        cast = new Cast(EMPTY, L("foo"), DataType.DATE);
        assertEquals(Nullability.UNKNOWN, cast.nullable());
        assertEquals(cast, foldNull.rule(cast));
    }

    public void testNullFoldingDoesNotApplyOnLogicalExpressions() {
        FoldNull rule = new FoldNull();

        Or or = new Or(EMPTY, NULL, TRUE);
        assertEquals(or, rule.rule(or));
        or = new Or(EMPTY, NULL, NULL);
        assertEquals(or, rule.rule(or));

        And and = new And(EMPTY, NULL, TRUE);
        assertEquals(and, rule.rule(and));
        and = new And(EMPTY, NULL, NULL);
        assertEquals(and, rule.rule(and));
    }

    @SuppressWarnings("unchecked")
    public void testNullFoldingDoesNotApplyOnConditionals() throws Exception {
        FoldNull rule = new FoldNull();

        Class<ConditionalFunction> clazz =
            (Class<ConditionalFunction>) randomFrom(IfNull.class, NullIf.class);
        Constructor<ConditionalFunction> ctor = clazz.getConstructor(Source.class, Expression.class, Expression.class);
        ConditionalFunction conditionalFunction = ctor.newInstance(EMPTY, NULL, ONE);
        assertEquals(conditionalFunction, rule.rule(conditionalFunction));
        conditionalFunction = ctor.newInstance(EMPTY, ONE, NULL);
        assertEquals(conditionalFunction, rule.rule(conditionalFunction));
        conditionalFunction = ctor.newInstance(EMPTY, NULL, NULL);
        assertEquals(conditionalFunction, rule.rule(conditionalFunction));
    }

    @SuppressWarnings("unchecked")
    public void testNullFoldingDoesNotApplyOnArbitraryConditionals() throws Exception {
        FoldNull rule = new FoldNull();

        Class<ArbitraryConditionalFunction> clazz =
            (Class<ArbitraryConditionalFunction>) randomFrom(Coalesce.class, Greatest.class, Least.class);
        Constructor<ArbitraryConditionalFunction> ctor = clazz.getConstructor(Source.class, List.class);
        ArbitraryConditionalFunction conditionalFunction = ctor.newInstance(EMPTY, Arrays.asList(NULL, ONE, TWO));
        assertEquals(conditionalFunction, rule.rule(conditionalFunction));
        conditionalFunction = ctor.newInstance(EMPTY, Arrays.asList(NULL, NULL, NULL));
        assertEquals(conditionalFunction, rule.rule(conditionalFunction));
    }

    public void testSimplifyCoalesceNulls() {
        Expression e = new SimplifyConditional().rule(new Coalesce(EMPTY, asList(NULL, NULL)));
        assertEquals(Coalesce.class, e.getClass());
        assertEquals(0, e.children().size());
    }

    public void testSimplifyCoalesceRandomNulls() {
        Expression e = new SimplifyConditional().rule(new Coalesce(EMPTY, randomListOfNulls()));
        assertEquals(Coalesce.class, e.getClass());
        assertEquals(0, e.children().size());
    }

    public void testSimplifyCoalesceRandomNullsWithValue() {
        Expression e = new SimplifyConditional().rule(new Coalesce(EMPTY,
                CollectionUtils.combine(
                        CollectionUtils.combine(randomListOfNulls(), TRUE, FALSE, TRUE),
                        randomListOfNulls())));
        assertEquals(1, e.children().size());
        assertEquals(TRUE, e.children().get(0));
        assertEquals(DataType.BOOLEAN, e.dataType());
    }

    private List<Expression> randomListOfNulls() {
        return asList(randomArray(1, 10, Literal[]::new, () -> NULL));
    }

    public void testSimplifyCoalesceFirstLiteral() {
        Expression e = new SimplifyConditional()
                .rule(new Coalesce(EMPTY,
                        Arrays.asList(NULL, TRUE, FALSE, new Abs(EMPTY, getFieldAttribute()))));
        assertEquals(Coalesce.class, e.getClass());
        assertEquals(1, e.children().size());
        assertEquals(TRUE, e.children().get(0));
        assertEquals(DataType.BOOLEAN, e.dataType());
    }

    public void testSimplifyIfNullNulls() {
        Expression e = new SimplifyConditional().rule(new IfNull(EMPTY, NULL, NULL));
        assertEquals(IfNull.class, e.getClass());
        assertEquals(0, e.children().size());
    }

    public void testSimplifyIfNullWithNullAndValue() {
        Expression e = new SimplifyConditional().rule(new IfNull(EMPTY, NULL, ONE));
        assertEquals(IfNull.class, e.getClass());
        assertEquals(1, e.children().size());
        assertEquals(ONE, e.children().get(0));
        assertEquals(DataType.INTEGER, e.dataType());

        e = new SimplifyConditional().rule(new IfNull(EMPTY, ONE, NULL));
        assertEquals(IfNull.class, e.getClass());
        assertEquals(1, e.children().size());
        assertEquals(ONE, e.children().get(0));
        assertEquals(DataType.INTEGER, e.dataType());
    }

    public void testFoldNullNotAppliedOnNullIf() {
        Expression orig = new NullIf(EMPTY, ONE, NULL);
        Expression f = new FoldNull().rule(orig);
        assertEquals(orig, f);
    }

    public void testSimplifyGreatestNulls() {
        Expression e = new SimplifyConditional().rule(new Greatest(EMPTY, asList(NULL, NULL)));
        assertEquals(Greatest.class, e.getClass());
        assertEquals(0, e.children().size());
    }

    public void testSimplifyGreatestRandomNulls() {
        Expression e = new SimplifyConditional().rule(new Greatest(EMPTY, randomListOfNulls()));
        assertEquals(Greatest.class, e.getClass());
        assertEquals(0, e.children().size());
    }

    public void testSimplifyGreatestRandomNullsWithValue() {
        Expression e = new SimplifyConditional().rule(new Greatest(EMPTY,
            CollectionUtils.combine(CollectionUtils.combine(randomListOfNulls(), ONE, TWO, ONE), randomListOfNulls())));
        assertEquals(Greatest.class, e.getClass());
        assertEquals(2, e.children().size());
        assertEquals(ONE, e.children().get(0));
        assertEquals(TWO, e.children().get(1));
        assertEquals(DataType.INTEGER, e.dataType());
    }

    public void testSimplifyLeastNulls() {
        Expression e = new SimplifyConditional().rule(new Least(EMPTY, asList(NULL, NULL)));
        assertEquals(Least.class, e.getClass());
        assertEquals(0, e.children().size());
    }

    public void testSimplifyLeastRandomNulls() {
        Expression e = new SimplifyConditional().rule(new Least(EMPTY, randomListOfNulls()));
        assertEquals(Least.class, e.getClass());
        assertEquals(0, e.children().size());
    }

    public void testSimplifyLeastRandomNullsWithValue() {
        Expression e = new SimplifyConditional().rule(new Least(EMPTY,
            CollectionUtils.combine(CollectionUtils.combine(randomListOfNulls(), ONE, TWO, ONE), randomListOfNulls())));
        assertEquals(Least.class, e.getClass());
        assertEquals(2, e.children().size());
        assertEquals(ONE, e.children().get(0));
        assertEquals(TWO, e.children().get(1));
        assertEquals(DataType.INTEGER, e.dataType());
    }

    public void testConcatFoldingIsNotNull() {
        FoldNull foldNull = new FoldNull();
        assertEquals(1, foldNull.rule(new Concat(EMPTY, NULL, ONE)).fold());
        assertEquals(1, foldNull.rule(new Concat(EMPTY, ONE, NULL)).fold());
        assertEquals(StringUtils.EMPTY, foldNull.rule(new Concat(EMPTY, NULL, NULL)).fold());
    }

    public void testFoldNullDateAdd() {
        FoldNull foldNull = new FoldNull();
        assertNullLiteral(foldNull.rule(new DateAdd(EMPTY, NULL, TWO, THREE, UTC)));
        assertNullLiteral(foldNull.rule(new DateAdd(EMPTY, ONE, NULL, THREE, UTC)));
        assertNullLiteral(foldNull.rule(new DateAdd(EMPTY, ONE, TWO, NULL, UTC)));
        assertNullLiteral(foldNull.rule(new DateAdd(EMPTY, NULL, NULL, NULL, UTC)));
        assertTrue(foldNull.rule(new DateAdd(EMPTY, ONE, TWO, THREE, UTC)) instanceof DateAdd);
    }

    public void testFoldNullDatePart() {
        FoldNull foldNull = new FoldNull();
        assertNullLiteral(foldNull.rule(new DatePart(EMPTY, NULL, TWO, UTC)));
        assertNullLiteral(foldNull.rule(new DatePart(EMPTY, ONE, NULL, UTC)));
        assertNullLiteral(foldNull.rule(new DatePart(EMPTY, NULL, NULL, UTC)));
        assertTrue(foldNull.rule(new DatePart(EMPTY, ONE, TWO, UTC)) instanceof DatePart);
    }

    public void testFoldNullDateTrunc() {
        FoldNull foldNull = new FoldNull();
        assertNullLiteral(foldNull.rule(new DateTrunc(EMPTY, NULL, TWO, UTC)));
        assertNullLiteral(foldNull.rule(new DateTrunc(EMPTY, ONE, NULL, UTC)));
        assertNullLiteral(foldNull.rule(new DateTrunc(EMPTY, NULL, NULL, UTC)));
        assertTrue(foldNull.rule(new DateTrunc(EMPTY, ONE, TWO, UTC)) instanceof DateTrunc);
    }

    public void testSimplifyCaseConditionsFoldWhenFalse() {
        // CASE WHEN a = 1 THEN 'foo1'
        //      WHEN 1 = 2 THEN 'bar1'
        //      WHEN 2 = 1 THEN 'bar2'
        //      WHEN a > 1 THEN 'foo2'
        // ELSE 'default'
        // END
        //
        // ==>
        //
        // CASE WHEN a = 1 THEN 'foo1'
        //      WHEN a > 1 THEN 'foo2'
        // ELSE 'default'
        // END

        Case c = new Case(EMPTY, Arrays.asList(
            new IfConditional(EMPTY, new Equals(EMPTY, getFieldAttribute(), ONE), Literal.of(EMPTY, "foo1")),
            new IfConditional(EMPTY, new Equals(EMPTY, ONE, TWO), Literal.of(EMPTY, "bar1")),
            new IfConditional(EMPTY, new Equals(EMPTY, TWO, ONE), Literal.of(EMPTY, "bar2")),
            new IfConditional(EMPTY, new GreaterThan(EMPTY, getFieldAttribute(), ONE), Literal.of(EMPTY, "foo2")),
            Literal.of(EMPTY, "default")));
        assertFalse(c.foldable());
        Expression e = new SimplifyCase().rule(c);
        assertEquals(Case.class, e.getClass());
        c = (Case) e;
        assertEquals(2, c.conditions().size());
        assertThat(c.conditions().get(0).condition().getClass(), is(Equals.class));
        assertThat(c.conditions().get(1).condition().getClass(), is(GreaterThan.class));
        assertFalse(c.foldable());
        assertEquals(TypeResolution.TYPE_RESOLVED, c.typeResolved());
    }

    public void testSimplifyCaseConditionsFoldCompletely_FoldableElse() {
        // CASE WHEN 1 = 2 THEN 'foo1'
        //      WHEN 1 = 1 THEN 'foo2'
        // ELSE 'default'
        // END
        //
        // ==>
        //
        // 'foo2'

        Case c = new Case(EMPTY, Arrays.asList(
            new IfConditional(EMPTY, new Equals(EMPTY, ONE, TWO), Literal.of(EMPTY, "foo1")),
            new IfConditional(EMPTY, new Equals(EMPTY, ONE, ONE), Literal.of(EMPTY, "foo2")),
            Literal.of(EMPTY, "default")));
        assertFalse(c.foldable());

        SimplifyCase rule = new SimplifyCase();
        Expression e = rule.rule(c);
        assertEquals(Case.class, e.getClass());
        c = (Case) e;
        assertEquals(1, c.conditions().size());
        assertThat(c.conditions().get(0).condition().nodeString(), is("1[INTEGER] == 1[INTEGER]"));
        assertTrue(c.foldable());
        assertEquals("foo2", c.fold());
        assertEquals(TypeResolution.TYPE_RESOLVED, c.typeResolved());
    }

    public void testSimplifyCaseConditionsFoldCompletely_NonFoldableElse() {
        // CASE WHEN 1 = 2 THEN 'foo1'
        // ELSE myField
        // END
        //
        // ==>
        //
        // myField (non-foldable)

        Case c = new Case(EMPTY, Arrays.asList(
                new IfConditional(EMPTY, new Equals(EMPTY, ONE, TWO), Literal.of(EMPTY, "foo1")),
                getFieldAttribute("myField")));
        assertFalse(c.foldable());

        SimplifyCase rule = new SimplifyCase();
        Expression e = rule.rule(c);
        assertEquals(Case.class, e.getClass());
        c = (Case) e;
        assertEquals(0, c.conditions().size());
        assertFalse(c.foldable());
        assertEquals("myField", Expressions.name(c.elseResult()));
    }

    public void testSimplifyIif_ConditionTrue_FoldableResult() {
        SimplifyCase rule = new SimplifyCase();
        Iif iif = new Iif(EMPTY, new Equals(EMPTY, ONE, ONE), Literal.of(EMPTY, "foo"), Literal.of(EMPTY, "bar"));
        assertTrue(iif.foldable());

        Expression e = rule.rule(iif);
        assertEquals(Iif.class, e.getClass());
        iif = (Iif) e;
        assertEquals(1, iif.conditions().size());
        assertTrue(iif.foldable());
        assertEquals("foo", iif.fold());
        assertEquals(TypeResolution.TYPE_RESOLVED, iif.typeResolved());
    }

    public void testSimplifyIif_ConditionTrue_NonFoldableResult() {
        SimplifyCase rule = new SimplifyCase();
        Iif iif = new Iif(EMPTY, new Equals(EMPTY, ONE, ONE), getFieldAttribute("myField"), Literal.of(EMPTY, "bar"));
        assertFalse(iif.foldable());

        Expression e = rule.rule(iif);
        assertEquals(Iif.class, e.getClass());
        iif = (Iif) e;
        assertEquals(1, iif.conditions().size());
        assertFalse(iif.foldable());
        assertTrue(iif.conditions().get(0).condition().foldable());
        assertEquals(Boolean.TRUE, iif.conditions().get(0).condition().fold());
        assertEquals("myField", Expressions.name(iif.conditions().get(0).result()));
    }

    public void testSimplifyIif_ConditionFalse_FoldableResult() {
        SimplifyCase rule = new SimplifyCase();
        Iif iif = new Iif(EMPTY, new Equals(EMPTY, ONE, TWO), Literal.of(EMPTY, "foo"), Literal.of(EMPTY, "bar"));
        assertTrue(iif.foldable());

        Expression e = rule.rule(iif);
        assertEquals(Iif.class, e.getClass());
        iif = (Iif) e;
        assertEquals(0, iif.conditions().size());
        assertTrue(iif.foldable());
        assertEquals("bar", iif.fold());
        assertEquals(TypeResolution.TYPE_RESOLVED, iif.typeResolved());
    }

    public void testSimplifyIif_ConditionFalse_NonFoldableResult() {
        SimplifyCase rule = new SimplifyCase();
        Iif iif = new Iif(EMPTY, new Equals(EMPTY, ONE, TWO), Literal.of(EMPTY, "foo"), getFieldAttribute("myField"));
        assertFalse(iif.foldable());

        Expression e = rule.rule(iif);
        assertEquals(Iif.class, e.getClass());
        iif = (Iif) e;
        assertEquals(0, iif.conditions().size());
        assertFalse(iif.foldable());
        assertEquals("myField", Expressions.name(iif.elseResult()));
    }
    
    //
    // Logical simplifications
    //

    private void assertNullLiteral(Expression expression) {
        assertEquals(Literal.class, expression.getClass());
        assertNull(expression.fold());
    }

    public void testBinaryComparisonSimplification() {
        assertEquals(TRUE, new BinaryComparisonSimplification().rule(new Equals(EMPTY, FIVE, FIVE)));
        assertEquals(TRUE, new BinaryComparisonSimplification().rule(new NullEquals(EMPTY, FIVE, FIVE)));
        assertEquals(TRUE, new BinaryComparisonSimplification().rule(new NullEquals(EMPTY, NULL, NULL)));
        assertEquals(FALSE, new BinaryComparisonSimplification().rule(new NotEquals(EMPTY, FIVE, FIVE)));
        assertEquals(TRUE, new BinaryComparisonSimplification().rule(new GreaterThanOrEqual(EMPTY, FIVE, FIVE)));
        assertEquals(TRUE, new BinaryComparisonSimplification().rule(new LessThanOrEqual(EMPTY, FIVE, FIVE)));

        assertEquals(FALSE, new BinaryComparisonSimplification().rule(new GreaterThan(EMPTY, FIVE, FIVE)));
        assertEquals(FALSE, new BinaryComparisonSimplification().rule(new LessThan(EMPTY, FIVE, FIVE)));
    }

    public void testNullEqualsWithNullLiteralBecomesIsNull() {
        BooleanLiteralsOnTheRight swapLiteralsToRight = new BooleanLiteralsOnTheRight();
        BinaryComparisonSimplification bcSimpl = new BinaryComparisonSimplification();
        FieldAttribute fa = getFieldAttribute();
        Source source = new Source(1, 10, "IS_NULL(a)");

        Expression e = bcSimpl.rule(swapLiteralsToRight.rule(new NullEquals(source, fa, NULL)));
        assertEquals(IsNull.class, e.getClass());
        IsNull isNull = (IsNull) e;
        assertEquals(source, isNull.source());

        e = bcSimpl.rule(swapLiteralsToRight.rule(new NullEquals(source, NULL, fa)));
        assertEquals(IsNull.class, e.getClass());
        isNull = (IsNull) e;
        assertEquals(source, isNull.source());
    }

    public void testLiteralsOnTheRight() {
        Alias a = new Alias(EMPTY, "a", L(10));
        Expression result = new BooleanLiteralsOnTheRight().rule(new Equals(EMPTY, FIVE, a));
        assertTrue(result instanceof Equals);
        Equals eq = (Equals) result;
        assertEquals(a, eq.left());
        assertEquals(FIVE, eq.right());

        a = new Alias(EMPTY, "a", L(10));
        result = new BooleanLiteralsOnTheRight().rule(new NullEquals(EMPTY, FIVE, a));
        assertTrue(result instanceof NullEquals);
        NullEquals nullEquals= (NullEquals) result;
        assertEquals(a, nullEquals.left());
        assertEquals(FIVE, nullEquals.right());
    }

    public void testLiteralsOnTheRightInStDistance() {
        Alias a = new Alias(EMPTY, "a", L(10));
        Expression result = new BooleanLiteralsOnTheRight().rule(new StDistance(EMPTY, FIVE, a));
        assertTrue(result instanceof StDistance);
        StDistance sd = (StDistance) result;
        assertEquals(a, sd.left());
        assertEquals(FIVE, sd.right());
    }

    public void testBoolSimplifyNotIsNullAndNotIsNotNull() {
        BooleanSimplification simplification = new BooleanSimplification();
        assertTrue(simplification.rule(new Not(EMPTY, new IsNull(EMPTY, ONE))) instanceof IsNotNull);
        assertTrue(simplification.rule(new Not(EMPTY, new IsNotNull(EMPTY, ONE))) instanceof IsNull);
    }

    public void testBoolSimplifyOr() {
        BooleanSimplification simplification = new BooleanSimplification();

        assertEquals(TRUE, simplification.rule(new Or(EMPTY, TRUE, TRUE)));
        assertEquals(TRUE, simplification.rule(new Or(EMPTY, TRUE, DUMMY_EXPRESSION)));
        assertEquals(TRUE, simplification.rule(new Or(EMPTY, DUMMY_EXPRESSION, TRUE)));

        assertEquals(FALSE, simplification.rule(new Or(EMPTY, FALSE, FALSE)));
        assertEquals(DUMMY_EXPRESSION, simplification.rule(new Or(EMPTY, FALSE, DUMMY_EXPRESSION)));
        assertEquals(DUMMY_EXPRESSION, simplification.rule(new Or(EMPTY, DUMMY_EXPRESSION, FALSE)));
    }

    public void testBoolSimplifyAnd() {
        BooleanSimplification simplification = new BooleanSimplification();

        assertEquals(TRUE, simplification.rule(new And(EMPTY, TRUE, TRUE)));
        assertEquals(DUMMY_EXPRESSION, simplification.rule(new And(EMPTY, TRUE, DUMMY_EXPRESSION)));
        assertEquals(DUMMY_EXPRESSION, simplification.rule(new And(EMPTY, DUMMY_EXPRESSION, TRUE)));

        assertEquals(FALSE, simplification.rule(new And(EMPTY, FALSE, FALSE)));
        assertEquals(FALSE, simplification.rule(new And(EMPTY, FALSE, DUMMY_EXPRESSION)));
        assertEquals(FALSE, simplification.rule(new And(EMPTY, DUMMY_EXPRESSION, FALSE)));
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
        FieldAttribute fa = getFieldAttribute();

        Range r = new Range(EMPTY, fa, SIX, false, FIVE, true);
        assertTrue(r.foldable());
        assertEquals(Boolean.FALSE, r.fold());
    }

    // 6 < a <= 5.5 -> FALSE
    public void testFoldExcludingRangeWithDifferentTypesToFalse() {
        FieldAttribute fa = getFieldAttribute();

        Range r = new Range(EMPTY, fa, SIX, false, L(5.5d), true);
        assertTrue(r.foldable());
        assertEquals(Boolean.FALSE, r.fold());
    }

    // Conjunction

    public void testCombineBinaryComparisonsNotComparable() {
        FieldAttribute fa = getFieldAttribute();
        LessThanOrEqual lte = new LessThanOrEqual(EMPTY, fa, SIX);
        LessThan lt = new LessThan(EMPTY, fa, FALSE);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        And and = new And(EMPTY, lte, lt);
        Expression exp = rule.rule(and);
        assertEquals(exp, and);
    }

    // a <= 6 AND a < 5  -> a < 5
    public void testCombineBinaryComparisonsUpper() {
        FieldAttribute fa = getFieldAttribute();
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
        FieldAttribute fa = getFieldAttribute();
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
        FieldAttribute fa = getFieldAttribute();
        GreaterThanOrEqual gte = new GreaterThanOrEqual(EMPTY, fa, FIVE);
        GreaterThan gt = new GreaterThan(EMPTY, fa, FIVE);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();

        Expression exp = rule.rule(new And(EMPTY, gte, gt));
        assertEquals(GreaterThan.class, exp.getClass());
        GreaterThan r = (GreaterThan) exp;
        assertEquals(FIVE, r.right());
    }

    // 2 < a AND (2 <= a < 3) -> 2 < a < 3
    public void testCombineBinaryComparisonsAndRangeLower() {
        FieldAttribute fa = getFieldAttribute();

        GreaterThan gt = new GreaterThan(EMPTY, fa, TWO);
        Range range = new Range(EMPTY, fa, TWO, true, THREE, false);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(new And(EMPTY, gt, range));
        assertEquals(Range.class, exp.getClass());
        Range r = (Range)exp;
        assertEquals(TWO, r.lower());
        assertFalse(r.includeLower());
        assertEquals(THREE, r.upper());
        assertFalse(r.includeUpper());
    }

    // a < 4 AND (1 < a < 3) -> 1 < a < 3
    public void testCombineBinaryComparisonsAndRangeUpper() {
        FieldAttribute fa = getFieldAttribute();

        LessThan lt = new LessThan(EMPTY, fa, FOUR);
        Range range = new Range(EMPTY, fa, ONE, false, THREE, false);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(new And(EMPTY, range, lt));
        assertEquals(Range.class, exp.getClass());
        Range r = (Range)exp;
        assertEquals(ONE, r.lower());
        assertFalse(r.includeLower());
        assertEquals(THREE, r.upper());
        assertFalse(r.includeUpper());
    }

    // a <= 2 AND (1 < a < 3) -> 1 < a <= 2
    public void testCombineBinaryComparisonsAndRangeUpperEqual() {
        FieldAttribute fa = getFieldAttribute();

        LessThanOrEqual lte = new LessThanOrEqual(EMPTY, fa, TWO);
        Range range = new Range(EMPTY, fa, ONE, false, THREE, false);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(new And(EMPTY, lte, range));
        assertEquals(Range.class, exp.getClass());
        Range r = (Range)exp;
        assertEquals(ONE, r.lower());
        assertFalse(r.includeLower());
        assertEquals(TWO, r.upper());
        assertTrue(r.includeUpper());
    }

    // 3 <= a AND 4 < a AND a <= 7 AND a < 6 -> 4 < a < 6
    public void testCombineMultipleBinaryComparisons() {
        FieldAttribute fa = getFieldAttribute();
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
        FieldAttribute fa = getFieldAttribute();
        GreaterThanOrEqual gte = new GreaterThanOrEqual(EMPTY, fa, THREE);
        GreaterThan gt = new GreaterThan(EMPTY, fa, FOUR);
        LessThanOrEqual lte = new LessThanOrEqual(EMPTY, fa, L(7));
        Expression ne = new Not(EMPTY, new Equals(EMPTY, fa, FIVE));

        CombineBinaryComparisons rule = new CombineBinaryComparisons();

        // TRUE AND a != 5 AND 4 < a <= 7
        Expression exp = rule.rule(new And(EMPTY, gte, new And(EMPTY, TRUE, new And(EMPTY, gt, new And(EMPTY, ne, lte)))));
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
        FieldAttribute fa = getFieldAttribute();
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
        FieldAttribute fa = getFieldAttribute();
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
        FieldAttribute fa = getFieldAttribute();

        Range r1 = new Range(EMPTY, fa, TWO, false, THREE, false);
        Range r2 = new Range(EMPTY, fa, ONE, false, FOUR, false);

        And and = new And(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(r1, exp);
    }

    // (2 < a < 3) AND a < 2 -> 2 < a < 2
    public void testCombineBinaryComparisonsConjunctionOfNonOverlappingBoundaries() {
        FieldAttribute fa = getFieldAttribute();

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
        FieldAttribute fa = getFieldAttribute();

        Range r1 = new Range(EMPTY, fa, TWO, false, THREE, false);
        Range r2 = new Range(EMPTY, fa, TWO, false, THREE, true);

        And and = new And(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(r1, exp);
    }

    // (2 < a < 3) AND (1 < a < 3) -> 2 < a < 3
    public void testCombineBinaryComparisonsConjunctionOverlappingUpperBoundary() {
        FieldAttribute fa = getFieldAttribute();

        Range r2 = new Range(EMPTY, fa, TWO, false, THREE, false);
        Range r1 = new Range(EMPTY, fa, ONE, false, THREE, false);

        And and = new And(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(r2, exp);
    }

    // (2 < a <= 3) AND (1 < a < 3) -> 2 < a < 3
    public void testCombineBinaryComparisonsConjunctionWithDifferentUpperLimitInclusion() {
        FieldAttribute fa = getFieldAttribute();

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
        FieldAttribute fa = getFieldAttribute();

        Range r1 = new Range(EMPTY, fa, L(0), false, ONE, true);
        Range r2 = new Range(EMPTY, fa, L(0), true, TWO, false);

        And and = new And(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(and);
        assertEquals(r1, exp);
    }

    // Disjunction

    public void testCombineBinaryComparisonsDisjunctionNotComparable() {
        FieldAttribute fa = getFieldAttribute();

        GreaterThan gt1 = new GreaterThan(EMPTY, fa, ONE);
        GreaterThan gt2 = new GreaterThan(EMPTY, fa, FALSE);

        Or or = new Or(EMPTY, gt1, gt2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(exp, or);
    }


    // 2 < a OR 1 < a OR 3 < a -> 1 < a
    public void testCombineBinaryComparisonsDisjunctionLowerBound() {
        FieldAttribute fa = getFieldAttribute();

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
        FieldAttribute fa = getFieldAttribute();

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
        FieldAttribute fa = getFieldAttribute();

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
        FieldAttribute fa = getFieldAttribute();

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
        FieldAttribute fa = getFieldAttribute();

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
        FieldAttribute fa = getFieldAttribute();

        Range r1 = new Range(EMPTY, fa, TWO, false, THREE, false);
        Range r2 = new Range(EMPTY, fa, ONE, false, FALSE, false);

        Or or = new Or(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(or, exp);
    }

    // (2 < a < 3) OR (1 < a < 4) -> (1 < a < 4)
    public void testCombineBinaryComparisonsDisjunctionOfIncludedRange() {
        FieldAttribute fa = getFieldAttribute();


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
        FieldAttribute fa = getFieldAttribute();

        Range r1 = new Range(EMPTY, fa, TWO, false, THREE, false);
        Range r2 = new Range(EMPTY, fa, ONE, false, TWO, false);

        Or or = new Or(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(or, exp);
    }

    // (2 < a < 3) OR (2 < a <= 3) -> 2 < a <= 3
    public void testCombineBinaryComparisonsDisjunctionOfUpperEqualsOverlappingBoundaries() {
        FieldAttribute fa = getFieldAttribute();

        Range r1 = new Range(EMPTY, fa, TWO, false, THREE, false);
        Range r2 = new Range(EMPTY, fa, TWO, false, THREE, true);

        Or or = new Or(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(r2, exp);
    }

    // (2 < a < 3) OR (1 < a < 3) -> 1 < a < 3
    public void testCombineBinaryComparisonsOverlappingUpperBoundary() {
        FieldAttribute fa = getFieldAttribute();

        Range r2 = new Range(EMPTY, fa, TWO, false, THREE, false);
        Range r1 = new Range(EMPTY, fa, ONE, false, THREE, false);

        Or or = new Or(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(r1, exp);
    }

    // (2 < a <= 3) OR (1 < a < 3) -> same (the <= prevents the ranges from being combined)
    public void testCombineBinaryComparisonsWithDifferentUpperLimitInclusion() {
        FieldAttribute fa = getFieldAttribute();

        Range r1 = new Range(EMPTY, fa, ONE, false, THREE, false);
        Range r2 = new Range(EMPTY, fa, TWO, false, THREE, true);

        Or or = new Or(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(or, exp);
    }

    // (a = 1 AND b = 3 AND c = 4) OR (a = 2 AND b = 3 AND c = 4) -> (b = 3 AND c = 4) AND (a = 1 OR a = 2)
    public void testBooleanSimplificationCommonExpressionSubstraction() {
        FieldAttribute fa = getFieldAttribute("a");
        FieldAttribute fb = getFieldAttribute("b");
        FieldAttribute fc = getFieldAttribute("c");

        Expression a1 = new Equals(EMPTY, fa, ONE);
        Expression a2 = new Equals(EMPTY, fa, TWO);
        And common = new And(EMPTY, new Equals(EMPTY, fb, THREE), new Equals(EMPTY, fc, FOUR));
        And left = new And(EMPTY, a1, common);
        And right = new And(EMPTY, a2, common);
        Or or = new Or(EMPTY, left, right);

        Expression exp = new BooleanSimplification().rule(or);
        assertEquals(new And(EMPTY, common, new Or(EMPTY, a1, a2)), exp);
    }

    // (0 < a <= 1) OR (0 < a < 2) -> 0 < a < 2
    public void testRangesOverlappingNoLowerBoundary() {
        FieldAttribute fa = getFieldAttribute();

        Range r2 = new Range(EMPTY, fa, L(0), false, TWO, false);
        Range r1 = new Range(EMPTY, fa, L(0), false, ONE, true);

        Or or = new Or(EMPTY, r1, r2);

        CombineBinaryComparisons rule = new CombineBinaryComparisons();
        Expression exp = rule.rule(or);
        assertEquals(r2, exp);
    }


    // Equals & NullEquals

    // 1 <= a < 10 AND a == 1 -> a == 1
    public void testEliminateRangeByEqualsInInterval() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq1 = new Equals(EMPTY, fa, ONE);
        Range r = new Range(EMPTY, fa, ONE, true, L(10), false);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq1, r));
        assertEquals(eq1, rule.rule(exp));
    }

    // 1 <= a < 10 AND a <=> 1 -> a <=> 1
    public void testEliminateRangeByNullEqualsInInterval() {
        FieldAttribute fa = getFieldAttribute();
        NullEquals eq1 = new NullEquals(EMPTY, fa, ONE);
        Range r = new Range(EMPTY, fa, ONE, true, L(10), false);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq1, r));
        assertEquals(eq1, rule.rule(exp));
    }


    // The following tests should work only to simplify filters and
    // not if the expressions are part of a projection
    // See: https://github.com/elastic/elasticsearch/issues/35859

    // a == 1 AND a == 2 -> FALSE
    public void testDualEqualsConjunction() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq1 = new Equals(EMPTY, fa, ONE);
        Equals eq2 = new Equals(EMPTY, fa, TWO);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq1, eq2));
        assertEquals(FALSE, rule.rule(exp));
    }

    // a <=> 1 AND a <=> 2 -> FALSE
    public void testDualNullEqualsConjunction() {
        FieldAttribute fa = getFieldAttribute();
        NullEquals eq1 = new NullEquals(EMPTY, fa, ONE);
        NullEquals eq2 = new NullEquals(EMPTY, fa, TWO);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq1, eq2));
        assertEquals(FALSE, rule.rule(exp));
    }

    // 1 < a < 10 AND a == 10 -> FALSE
    public void testEliminateRangeByEqualsOutsideInterval() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq1 = new Equals(EMPTY, fa, L(10));
        Range r = new Range(EMPTY, fa, ONE, false, L(10), false);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq1, r));
        assertEquals(FALSE, rule.rule(exp));
    }

    // 1 < a < 10 AND a <=> 10 -> FALSE
    public void testEliminateRangeByNullEqualsOutsideInterval() {
        FieldAttribute fa = getFieldAttribute();
        NullEquals eq1 = new NullEquals(EMPTY, fa, L(10));
        Range r = new Range(EMPTY, fa, ONE, false, L(10), false);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq1, r));
        assertEquals(FALSE, rule.rule(exp));
    }

    // a != 3 AND a = 3 -> FALSE
    public void testPropagateEquals_VarNeq3AndVarEq3() {
        FieldAttribute fa = getFieldAttribute();
        NotEquals neq = new NotEquals(EMPTY, fa, THREE);
        Equals eq = new Equals(EMPTY, fa, THREE);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, neq, eq));
        assertEquals(FALSE, rule.rule(exp));
    }

    // a != 4 AND a = 3 -> a = 3
    public void testPropagateEquals_VarNeq4AndVarEq3() {
        FieldAttribute fa = getFieldAttribute();
        NotEquals neq = new NotEquals(EMPTY, fa, FOUR);
        Equals eq = new Equals(EMPTY, fa, THREE);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, neq, eq));
        assertEquals(Equals.class, exp.getClass());
        assertEquals(eq, rule.rule(exp));
    }

    // a = 2 AND a < 2 -> FALSE
    public void testPropagateEquals_VarEq2AndVarLt2() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = new Equals(EMPTY, fa, TWO);
        LessThan lt = new LessThan(EMPTY, fa, TWO);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq, lt));
        assertEquals(FALSE, exp);
    }

    // a = 2 AND a <= 2 -> a = 2
    public void testPropagateEquals_VarEq2AndVarLte2() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = new Equals(EMPTY, fa, TWO);
        LessThanOrEqual lt = new LessThanOrEqual(EMPTY, fa, TWO);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq, lt));
        assertEquals(eq, exp);
    }

    // a = 2 AND a <= 1 -> FALSE
    public void testPropagateEquals_VarEq2AndVarLte1() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = new Equals(EMPTY, fa, TWO);
        LessThanOrEqual lt = new LessThanOrEqual(EMPTY, fa, ONE);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq, lt));
        assertEquals(FALSE, exp);
    }

    // a = 2 AND a > 2 -> FALSE
    public void testPropagateEquals_VarEq2AndVarGt2() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = new Equals(EMPTY, fa, TWO);
        GreaterThan gt = new GreaterThan(EMPTY, fa, TWO);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq, gt));
        assertEquals(FALSE, exp);
    }

    // a = 2 AND a >= 2 -> a = 2
    public void testPropagateEquals_VarEq2AndVarGte2() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = new Equals(EMPTY, fa, TWO);
        GreaterThanOrEqual gte = new GreaterThanOrEqual(EMPTY, fa, TWO);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq, gte));
        assertEquals(eq, exp);
    }

    // a = 2 AND a > 3 -> FALSE
    public void testPropagateEquals_VarEq2AndVarLt3() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = new Equals(EMPTY, fa, TWO);
        GreaterThan gt = new GreaterThan(EMPTY, fa, THREE);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new And(EMPTY, eq, gt));
        assertEquals(FALSE, exp);
    }

    // a = 2 AND a < 3 AND a > 1 AND a != 4 -> a = 2
    public void testPropagateEquals_VarEq2AndVarLt3AndVarGt1AndVarNeq4() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = new Equals(EMPTY, fa, TWO);
        LessThan lt = new LessThan(EMPTY, fa, THREE);
        GreaterThan gt = new GreaterThan(EMPTY, fa, ONE);
        NotEquals neq = new NotEquals(EMPTY, fa, FOUR);

        PropagateEquals rule = new PropagateEquals();
        Expression and = Predicates.combineAnd(Arrays.asList(eq, lt, gt, neq));
        Expression exp = rule.rule(and);
        assertEquals(eq, exp);
    }

    // a = 2 AND 1 < a < 3 AND a > 0 AND a != 4 -> a = 2
    public void testPropagateEquals_VarEq2AndVarRangeGt1Lt3AndVarGt0AndVarNeq4() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = new Equals(EMPTY, fa, TWO);
        Range range = new Range(EMPTY, fa, ONE, false, THREE, false);
        GreaterThan gt = new GreaterThan(EMPTY, fa, L(0));
        NotEquals neq = new NotEquals(EMPTY, fa, FOUR);

        PropagateEquals rule = new PropagateEquals();
        Expression and = Predicates.combineAnd(Arrays.asList(eq, range, gt, neq));
        Expression exp = rule.rule(and);
        assertEquals(eq, exp);
    }

    // a = 2 OR a > 1 -> a > 1
    public void testPropagateEquals_VarEq2OrVarGt1() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = new Equals(EMPTY, fa, TWO);
        GreaterThan gt = new GreaterThan(EMPTY, fa, ONE);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new Or(EMPTY, eq, gt));
        assertEquals(gt, exp);
    }

    // a = 2 OR a > 2 -> a >= 2
    public void testPropagateEquals_VarEq2OrVarGte2() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = new Equals(EMPTY, fa, TWO);
        GreaterThan gt = new GreaterThan(EMPTY, fa, TWO);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new Or(EMPTY, eq, gt));
        assertEquals(GreaterThanOrEqual.class, exp.getClass());
        GreaterThanOrEqual gte = (GreaterThanOrEqual) exp;
        assertEquals(TWO, gte.right());
    }

    // a = 2 OR a < 3 -> a < 3
    public void testPropagateEquals_VarEq2OrVarLt3() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = new Equals(EMPTY, fa, TWO);
        LessThan lt = new LessThan(EMPTY, fa, THREE);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new Or(EMPTY, eq, lt));
        assertEquals(lt, exp);
    }

    // a = 3 OR a < 3 -> a <= 3
    public void testPropagateEquals_VarEq3OrVarLt3() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = new Equals(EMPTY, fa, THREE);
        LessThan lt = new LessThan(EMPTY, fa, THREE);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new Or(EMPTY, eq, lt));
        assertEquals(LessThanOrEqual.class, exp.getClass());
        LessThanOrEqual lte = (LessThanOrEqual) exp;
        assertEquals(THREE, lte.right());
    }

    // a = 2 OR 1 < a < 3 -> 1 < a < 3
    public void testPropagateEquals_VarEq2OrVarRangeGt1Lt3() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = new Equals(EMPTY, fa, TWO);
        Range range = new Range(EMPTY, fa, ONE, false, THREE, false);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new Or(EMPTY, eq, range));
        assertEquals(range, exp);
    }

    // a = 2 OR 2 < a < 3 -> 2 <= a < 3
    public void testPropagateEquals_VarEq2OrVarRangeGt2Lt3() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = new Equals(EMPTY, fa, TWO);
        Range range = new Range(EMPTY, fa, TWO, false, THREE, false);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new Or(EMPTY, eq, range));
        assertEquals(Range.class, exp.getClass());
        Range r = (Range) exp;
        assertEquals(TWO, r.lower());
        assertTrue(r.includeLower());
        assertEquals(THREE, r.upper());
        assertFalse(r.includeUpper());
    }

    // a = 3 OR 2 < a < 3 -> 2 < a <= 3
    public void testPropagateEquals_VarEq3OrVarRangeGt2Lt3() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = new Equals(EMPTY, fa, THREE);
        Range range = new Range(EMPTY, fa, TWO, false, THREE, false);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new Or(EMPTY, eq, range));
        assertEquals(Range.class, exp.getClass());
        Range r = (Range) exp;
        assertEquals(TWO, r.lower());
        assertFalse(r.includeLower());
        assertEquals(THREE, r.upper());
        assertTrue(r.includeUpper());
    }

    // a = 2 OR a != 2 -> TRUE
    public void testPropagateEquals_VarEq2OrVarNeq2() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = new Equals(EMPTY, fa, TWO);
        NotEquals neq = new NotEquals(EMPTY, fa, TWO);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new Or(EMPTY, eq, neq));
        assertEquals(TRUE, exp);
    }

    // a = 2 OR a != 5 -> a != 5
    public void testPropagateEquals_VarEq2OrVarNeq5() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = new Equals(EMPTY, fa, TWO);
        NotEquals neq = new NotEquals(EMPTY, fa, FIVE);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(new Or(EMPTY, eq, neq));
        assertEquals(NotEquals.class, exp.getClass());
        NotEquals ne = (NotEquals) exp;
        assertEquals(ne.right(), FIVE);
    }

    // a = 2 OR 3 < a < 4 OR a > 2 OR a!= 2 -> TRUE
    public void testPropagateEquals_VarEq2OrVarRangeGt3Lt4OrVarGt2OrVarNe2() {
        FieldAttribute fa = getFieldAttribute();
        Equals eq = new Equals(EMPTY, fa, TWO);
        Range range = new Range(EMPTY, fa, THREE, false, FOUR, false);
        GreaterThan gt = new GreaterThan(EMPTY, fa, TWO);
        NotEquals neq = new NotEquals(EMPTY, fa, TWO);

        PropagateEquals rule = new PropagateEquals();
        Expression exp = rule.rule(Predicates.combineOr(Arrays.asList(eq, range, neq, gt)));
        assertEquals(TRUE, exp);
    }

    public void testTranslateMinToFirst() {
        Min min1 =  new Min(EMPTY, new FieldAttribute(EMPTY, "str", new EsField("str", DataType.KEYWORD, emptyMap(), true)));
        Min min2 =  new Min(EMPTY, getFieldAttribute());

        OrderBy plan = new OrderBy(EMPTY, new Aggregate(EMPTY, FROM(), emptyList(),
                Arrays.asList(a("min1", min1), a("min2", min2))),
            Arrays.asList(
                new Order(EMPTY, min1, OrderDirection.ASC, Order.NullsPosition.LAST),
                new Order(EMPTY, min2, OrderDirection.ASC, Order.NullsPosition.LAST)));
        LogicalPlan result = new ReplaceMinMaxWithTopHits().apply(plan);
        assertTrue(result instanceof OrderBy);
        List<Order> order = ((OrderBy) result).order();
        assertEquals(2, order.size());
        assertEquals(First.class, order.get(0).child().getClass());
        assertEquals(min2, order.get(1).child());
        First first = (First) order.get(0).child();

        assertTrue(((OrderBy) result).child() instanceof Aggregate);
        List<? extends NamedExpression> aggregates = ((Aggregate) ((OrderBy) result).child()).aggregates();
        assertEquals(2, aggregates.size());
        assertEquals(Alias.class, aggregates.get(0).getClass());
        assertEquals(Alias.class, aggregates.get(1).getClass());
        assertSame(first, ((Alias) aggregates.get(0)).child());
        assertEquals(min2, ((Alias) aggregates.get(1)).child());
    }

    public void testTranslateMaxToLast() {
        Max max1 =  new Max(EMPTY, new FieldAttribute(EMPTY, "str", new EsField("str", DataType.KEYWORD, emptyMap(), true)));
        Max max2 =  new Max(EMPTY, getFieldAttribute());

        OrderBy plan = new OrderBy(EMPTY, new Aggregate(EMPTY, FROM(), emptyList(), Arrays.asList(a("max1", max1), a("max2", max2))),
            Arrays.asList(
                new Order(EMPTY, max1, OrderDirection.ASC, Order.NullsPosition.LAST),
                new Order(EMPTY, max2, OrderDirection.ASC, Order.NullsPosition.LAST)));
        LogicalPlan result = new ReplaceMinMaxWithTopHits().apply(plan);
        assertTrue(result instanceof OrderBy);
        List<Order> order = ((OrderBy) result).order();
        assertEquals(Last.class, order.get(0).child().getClass());
        assertEquals(max2, order.get(1).child());
        Last last = (Last) order.get(0).child();

        assertTrue(((OrderBy) result).child() instanceof Aggregate);
        List<? extends NamedExpression> aggregates = ((Aggregate) ((OrderBy) result).child()).aggregates();
        assertEquals(2, aggregates.size());
        assertEquals(Alias.class, aggregates.get(0).getClass());
        assertEquals(Alias.class, aggregates.get(1).getClass());
        assertSame(last, ((Alias) aggregates.get(0)).child());
        assertEquals(max2, ((Alias) aggregates.get(1)).child());
    }

    public void testSortAggregateOnOrderByWithTwoFields() {
        FieldAttribute firstField = new FieldAttribute(EMPTY, "first_field", new EsField("first_field", DataType.BYTE, emptyMap(), true));
        FieldAttribute secondField = new FieldAttribute(EMPTY, "second_field",
                new EsField("second_field", DataType.BYTE, emptyMap(), true));
        Alias firstAlias = new Alias(EMPTY, "first_alias", firstField);
        Alias secondAlias = new Alias(EMPTY, "second_alias", secondField);
        Order firstOrderBy = new Order(EMPTY, firstField, OrderDirection.ASC, Order.NullsPosition.LAST);
        Order secondOrderBy = new Order(EMPTY, secondField, OrderDirection.ASC, Order.NullsPosition.LAST);
        
        OrderBy orderByPlan = new OrderBy(EMPTY,
                new Aggregate(EMPTY, FROM(), Arrays.asList(secondField, firstField), Arrays.asList(secondAlias, firstAlias)),
                Arrays.asList(firstOrderBy, secondOrderBy));
        LogicalPlan result = new SortAggregateOnOrderBy().apply(orderByPlan);
        
        assertTrue(result instanceof OrderBy);
        List<Order> order = ((OrderBy) result).order();
        assertEquals(2, order.size());
        assertTrue(order.get(0).child() instanceof FieldAttribute);
        assertTrue(order.get(1).child() instanceof FieldAttribute);
        assertEquals("first_field", ((FieldAttribute) order.get(0).child()).name());
        assertEquals("second_field", ((FieldAttribute) order.get(1).child()).name());
        
        assertTrue(((OrderBy) result).child() instanceof Aggregate);
        Aggregate agg = (Aggregate) ((OrderBy) result).child();
        List<?> groupings = agg.groupings();
        assertEquals(2, groupings.size());
        assertTrue(groupings.get(0) instanceof FieldAttribute);
        assertTrue(groupings.get(1) instanceof FieldAttribute);
        assertEquals(firstField, groupings.get(0));
        assertEquals(secondField, groupings.get(1));
    }

    public void testSortAggregateOnOrderByOnlyAliases() {
        FieldAttribute firstField = new FieldAttribute(EMPTY, "first_field", new EsField("first_field", DataType.BYTE, emptyMap(), true));
        FieldAttribute secondField = new FieldAttribute(EMPTY, "second_field",
                new EsField("second_field", DataType.BYTE, emptyMap(), true));
        Alias firstAlias = new Alias(EMPTY, "first_alias", firstField);
        Alias secondAlias = new Alias(EMPTY, "second_alias", secondField);
        Order firstOrderBy = new Order(EMPTY, firstAlias, OrderDirection.ASC, Order.NullsPosition.LAST);
        Order secondOrderBy = new Order(EMPTY, secondAlias, OrderDirection.ASC, Order.NullsPosition.LAST);
        
        OrderBy orderByPlan = new OrderBy(EMPTY,
                new Aggregate(EMPTY, FROM(), Arrays.asList(secondAlias, firstAlias), Arrays.asList(secondAlias, firstAlias)),
                Arrays.asList(firstOrderBy, secondOrderBy));
        LogicalPlan result = new SortAggregateOnOrderBy().apply(orderByPlan);
        
        assertTrue(result instanceof OrderBy);
        List<Order> order = ((OrderBy) result).order();
        assertEquals(2, order.size());
        assertTrue(order.get(0).child() instanceof Alias);
        assertTrue(order.get(1).child() instanceof Alias);
        assertEquals("first_alias", ((Alias) order.get(0).child()).name());
        assertEquals("second_alias", ((Alias) order.get(1).child()).name());
        
        assertTrue(((OrderBy) result).child() instanceof Aggregate);
        Aggregate agg = (Aggregate) ((OrderBy) result).child();
        List<?> groupings = agg.groupings();
        assertEquals(2, groupings.size());
        assertTrue(groupings.get(0) instanceof Alias);
        assertTrue(groupings.get(1) instanceof Alias);
        assertEquals(firstAlias, groupings.get(0));
        assertEquals(secondAlias, groupings.get(1));
    }

    public void testPivotRewrite() {
        FieldAttribute column = getFieldAttribute("pivot");
        FieldAttribute number = getFieldAttribute("number");
        List<NamedExpression> values = Arrays.asList(new Alias(EMPTY, "ONE", L(1)), new Alias(EMPTY, "TWO", L(2)));
        List<NamedExpression> aggs = Arrays.asList(new Alias(EMPTY, "AVG", new Avg(EMPTY, number)));
        Pivot pivot = new Pivot(EMPTY, new EsRelation(EMPTY, new EsIndex("table", emptyMap()), false), column, values, aggs);

        LogicalPlan result = new RewritePivot().apply(pivot);
        assertEquals(Pivot.class, result.getClass());
        Pivot pv = (Pivot) result;
        assertEquals(pv.aggregates(), aggs);
        assertEquals(Filter.class, pv.child().getClass());
        Filter f = (Filter) pv.child();
        assertEquals(In.class, f.condition().getClass());
        In in = (In) f.condition();
        assertEquals(column, in.value());
        assertEquals(Arrays.asList(L(1), L(2)), in.list());
    }

    /**
     * Test queries like SELECT MIN(agg_field), MAX(agg_field) FROM table WHERE MATCH(match_field,'A') AND/OR QUERY('match_field:A')
     * or SELECT STDDEV_POP(agg_field), VAR_POP(agg_field) FROM table WHERE MATCH(match_field,'A') AND/OR QUERY('match_field:A')
     */
    public void testAggregatesPromoteToStats_WithFullTextPredicatesConditions() {
        FieldAttribute matchField = new FieldAttribute(EMPTY, "match_field", new EsField("match_field", DataType.TEXT, emptyMap(), true));
        FieldAttribute aggField = new FieldAttribute(EMPTY, "agg_field", new EsField("agg_field", DataType.INTEGER, emptyMap(), true));

        FullTextPredicate matchPredicate = new MatchQueryPredicate(EMPTY, matchField, "A", StringUtils.EMPTY);
        FullTextPredicate multiMatchPredicate = new MultiMatchQueryPredicate(EMPTY, "match_field", "A", StringUtils.EMPTY);
        FullTextPredicate stringQueryPredicate = new StringQueryPredicate(EMPTY, "match_field:A", StringUtils.EMPTY);
        List<FullTextPredicate> predicates = Arrays.asList(matchPredicate, multiMatchPredicate, stringQueryPredicate);

        FullTextPredicate left = randomFrom(predicates);
        FullTextPredicate right = randomFrom(predicates);

        BinaryLogic or = new Or(EMPTY, left, right);
        BinaryLogic and = new And(EMPTY, left, right);
        BinaryLogic condition = randomFrom(or, and);
        Filter filter = new Filter(EMPTY, FROM(), condition);

        List<AggregateFunction> aggregates;
        boolean isSimpleStats = randomBoolean();
        if (isSimpleStats) {
            aggregates = Arrays.asList(new Avg(EMPTY, aggField), new Sum(EMPTY, aggField), new Min(EMPTY, aggField),
                    new Max(EMPTY, aggField));
        } else {
            aggregates = Arrays.asList(new StddevPop(EMPTY, aggField), new SumOfSquares(EMPTY, aggField), new VarPop(EMPTY, aggField));
        }
        AggregateFunction firstAggregate = randomFrom(aggregates);
        AggregateFunction secondAggregate = randomValueOtherThan(firstAggregate, () -> randomFrom(aggregates));
        Aggregate aggregatePlan = new Aggregate(EMPTY, filter, singletonList(matchField),
                Arrays.asList(new Alias(EMPTY, "first", firstAggregate), new Alias(EMPTY, "second", secondAggregate)));
        LogicalPlan result;
        if (isSimpleStats) {
            result = new ReplaceAggsWithStats().apply(aggregatePlan);
        } else {
            result = new ReplaceAggsWithExtendedStats().apply(aggregatePlan);
        }

        assertTrue(result instanceof Aggregate);
        Aggregate resultAgg = (Aggregate) result;
        assertEquals(2, resultAgg.aggregates().size());

        NamedExpression one = resultAgg.aggregates().get(0);
        assertTrue(one instanceof Alias);
        assertTrue(((Alias) one).child() instanceof InnerAggregate);

        NamedExpression two = resultAgg.aggregates().get(1);
        assertTrue(two instanceof Alias);
        assertTrue(((Alias) two).child() instanceof InnerAggregate);

        InnerAggregate resultFirstAgg = (InnerAggregate) ((Alias) one).child();
        InnerAggregate resultSecondAgg = (InnerAggregate) ((Alias) two).child();
        assertEquals(resultFirstAgg.inner(), firstAggregate);
        assertEquals(resultSecondAgg.inner(), secondAggregate);
        if (isSimpleStats) {
            assertTrue(resultFirstAgg.outer() instanceof Stats);
            assertTrue(resultSecondAgg.outer() instanceof Stats);
            assertEquals(((Stats) resultFirstAgg.outer()).field(), aggField);
            assertEquals(((Stats) resultSecondAgg.outer()).field(), aggField);
        } else {
            assertTrue(resultFirstAgg.outer() instanceof ExtendedStats);
            assertTrue(resultSecondAgg.outer() instanceof ExtendedStats);
            assertEquals(((ExtendedStats) resultFirstAgg.outer()).field(), aggField);
            assertEquals(((ExtendedStats) resultSecondAgg.outer()).field(), aggField);
        }

        assertTrue(resultAgg.child() instanceof Filter);
        assertEquals(resultAgg.child(), filter);
    }

    public void testReplaceAttributesWithTarget() {
        FieldAttribute a = getFieldAttribute("a");
        FieldAttribute b = getFieldAttribute("b");
        
        Alias aAlias = new Alias(EMPTY, "aAlias", a);
        Alias bAlias = new Alias(EMPTY, "bAlias", b);
        
        Project p = new Project(EMPTY, FROM(), Arrays.asList(aAlias, bAlias));
        Filter f = new Filter(EMPTY, p,
                new And(EMPTY, new GreaterThan(EMPTY, aAlias.toAttribute(), L(1)), new GreaterThan(EMPTY, bAlias.toAttribute(), L(2))));
        
        ReplaceReferenceAttributeWithSource rule = new ReplaceReferenceAttributeWithSource();
        Expression condition = f.condition();
        assertTrue(condition instanceof And);
        And and = (And) condition;
        assertTrue(and.left() instanceof GreaterThan);
        GreaterThan gt = (GreaterThan) and.left();
        assertEquals(aAlias.toAttribute(), gt.left());

        LogicalPlan plan = rule.apply(f);

        Filter filter = (Filter) plan;
        condition = filter.condition();
        assertTrue(condition instanceof And);
        and = (And) condition;
        assertTrue(and.left() instanceof GreaterThan);
        gt = (GreaterThan) and.left();
        assertEquals(a, gt.left());
    }
}
