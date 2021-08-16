/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.optimizer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expression.TypeResolution;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.Order.OrderDirection;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.function.aggregate.InnerAggregate;
import org.elasticsearch.xpack.ql.expression.predicate.Range;
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
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RLike;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.BooleanSimplification;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.CombineBinaryComparisons;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.ConstantFolding;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules.LiteralsOnTheRight;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.util.CollectionUtils;
import org.elasticsearch.xpack.ql.util.StringUtils;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer.ReplaceSubQueryAliases;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer.PruneSubQueryAliases;
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
import org.elasticsearch.xpack.sql.expression.predicate.conditional.ArbitraryConditionalFunction;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Case;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Coalesce;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.ConditionalFunction;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Greatest;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.IfConditional;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.IfNull;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Iif;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.Least;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.NullIf;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.CombineProjections;
import org.elasticsearch.xpack.sql.optimizer.Optimizer.FoldNull;
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
import org.elasticsearch.xpack.sql.session.SingletonExecutable;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.ql.TestUtils.equalsOf;
import static org.elasticsearch.xpack.ql.TestUtils.fieldAttribute;
import static org.elasticsearch.xpack.ql.TestUtils.greaterThanOf;
import static org.elasticsearch.xpack.ql.TestUtils.greaterThanOrEqualOf;
import static org.elasticsearch.xpack.ql.TestUtils.lessThanOf;
import static org.elasticsearch.xpack.ql.expression.Literal.FALSE;
import static org.elasticsearch.xpack.ql.expression.Literal.NULL;
import static org.elasticsearch.xpack.ql.expression.Literal.TRUE;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.BYTE;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.sql.SqlTestUtils.literal;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.DATE;
import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class OptimizerTests extends ESTestCase {

    private static final Literal ONE = L(1);
    private static final Literal TWO = L(2);
    private static final Literal THREE = L(3);
    private static final Literal FOUR = L(4);
    private static final Literal FIVE = L(5);

    private static LogicalPlan FROM() {
        return new LocalRelation(EMPTY, new EmptyExecutable(emptyList()));
    }

    private static Literal L(Object value) {
        return literal(value);
    }

    private static Alias a(String name, Expression e) {
        return new Alias(e.source(), name, e);
    }

    private static FieldAttribute getFieldAttribute() {
        return getFieldAttribute("a");
    }

    private static FieldAttribute getFieldAttribute(String name) {
        return new FieldAttribute(EMPTY, name, new EsField(name + "f", INTEGER, emptyMap(), true));
    }

    public void testPruneSubQueryAliases() {
        ShowTables s = new ShowTables(EMPTY, null, null, false);
        SubQueryAlias plan = new SubQueryAlias(EMPTY, s, "show");
        LogicalPlan result = new PruneSubQueryAliases().apply(plan);
        assertEquals(result, s);
    }

    public void testReplaceSubQueryAliases() {
        FieldAttribute firstField = new FieldAttribute(EMPTY, "field", new EsField("field", BYTE, emptyMap(), true));
        EsRelation relation = new EsRelation(EMPTY, new EsIndex("table", emptyMap()), false);
        Aggregate agg = new Aggregate(EMPTY, relation, Collections.singletonList(firstField), Collections.singletonList(firstField));
        SubQueryAlias subquery = new SubQueryAlias(EMPTY, agg, "subquery");
        Project project = new Project(EMPTY, subquery, Collections.singletonList(firstField.withQualifier("subquery")));
        LogicalPlan result = new ReplaceSubQueryAliases().apply(project);
        assertThat(result, instanceOf(Project.class));
        assertThat(((Project) result).projections().get(0), instanceOf(FieldAttribute.class));
        assertEquals("table", ((FieldAttribute) ((Project) result).projections().get(0)).qualifier());
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
        LogicalPlan p = new Filter(EMPTY, FROM(), lessThanOf(a, L(10)));
        // SELECT
        p = new Project(EMPTY, p, asList(a, b));
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

    public void testConstantIsNotNull() {
        assertEquals(FALSE, new ConstantFolding().rule(new IsNotNull(EMPTY, L(null))));
        assertEquals(TRUE, new ConstantFolding().rule(new IsNotNull(EMPTY, FIVE)));
    }

    public void testConstantFoldingDatetime() {
        Expression cast = new Cast(EMPTY, literal("2018-01-19T10:23:27Z"), DATETIME, UTC);
        assertEquals(2018, foldFunction(new Year(EMPTY, cast, UTC)));
        assertEquals(1, foldFunction(new MonthOfYear(EMPTY, cast, UTC)));
        assertEquals(19, foldFunction(new DayOfMonth(EMPTY, cast, UTC)));
        assertEquals(19, foldFunction(new DayOfYear(EMPTY, cast, UTC)));
        assertEquals(3, foldFunction(new IsoWeekOfYear(EMPTY, cast, UTC)));
        assertNull(foldFunction(
                new IsoWeekOfYear(EMPTY, new Literal(EMPTY, null, DataTypes.NULL), UTC)));
    }

    public void testConstantFoldingIn() {
        In in = new In(EMPTY, ONE,
            asList(ONE, TWO, ONE, THREE, new Sub(EMPTY, THREE, ONE), ONE, FOUR, new Abs(EMPTY, new Sub(EMPTY, TWO, FIVE))));
        Literal result= (Literal) new ConstantFolding().rule(in);
        assertEquals(true, result.value());
    }

    public void testConstantFoldingIn_LeftValueNotFoldable() {
        In in = new In(EMPTY, getFieldAttribute(),
                asList(ONE, TWO, ONE, THREE, new Sub(EMPTY, THREE, ONE), ONE, FOUR, new Abs(EMPTY, new Sub(EMPTY, TWO, FIVE))));
        Alias as = new Alias(in.source(), in.sourceText(), in);
        Project p = new Project(EMPTY, FROM(), Collections.singletonList(as));
        p = (Project) new ConstantFolding().apply(p);
        assertEquals(1, p.projections().size());
        Alias a = (Alias) p.projections().get(0);
        In i = (In) a.child();
        assertThat(i.list(), contains(ONE, TWO, THREE, FOUR));
    }

    public void testConstantFoldingIn_RightValueIsNull() {
        In in = new In(EMPTY, getFieldAttribute(), asList(NULL, NULL));
        Literal result= (Literal) new ConstantFolding().rule(in);
        assertNull(result.value());
    }

    public void testConstantFoldingIn_LeftValueIsNull() {
        In in = new In(EMPTY, NULL, asList(ONE, TWO, THREE));
        Literal result= (Literal) new ConstantFolding().rule(in);
        assertNull(result.value());
    }

    public void testMathFolding() {
        assertEquals(7, foldFunction(new Abs(EMPTY, L(7))));
        assertEquals(0d, (double) foldFunction(new ACos(EMPTY, ONE)), 0.01d);
        assertEquals(1.57076d, (double) foldFunction(new ASin(EMPTY, ONE)), 0.01d);
        assertEquals(0.78539d, (double) foldFunction(new ATan(EMPTY, ONE)), 0.01d);
        assertEquals(7, foldFunction(new Floor(EMPTY, L(7))));
        assertEquals(Math.E, foldFunction(new E(EMPTY)));
    }

    public void testNullIfFolding() {
        assertEquals(null, foldFunction(new NullIf(EMPTY, ONE, ONE)));
        assertEquals(1, foldFunction(new NullIf(EMPTY, ONE, TWO)));
        assertEquals(2, foldFunction(new NullIf(EMPTY, TWO, ONE)));

        FieldAttribute fa = fieldAttribute();
        // works even if the expressions are not foldable
        assertEquals(null, foldFunction(new NullIf(EMPTY, fa, fa)));
    }

    private static Object foldFunction(Function f) {
        return ((Literal) new ConstantFolding().rule(f)).value();
    }

    // Null folding

    public void testNullFoldingIsNull() {
        FoldNull foldNull = new FoldNull();
        assertEquals(true, foldNull.rule(new IsNull(EMPTY, NULL)).fold());
        assertEquals(false, foldNull.rule(new IsNull(EMPTY, TRUE)).fold());
    }

    public void testNullFoldingIsNullWithCast() {
        FoldNull foldNull = new FoldNull();

        Cast cast = new Cast(EMPTY, L("foo"), DATE, UTC);
        IsNull isNull = new IsNull(EMPTY, cast);
        final IsNull isNullOpt = (IsNull) foldNull.rule(isNull);
        assertEquals(isNull, isNullOpt);

        QlIllegalArgumentException sqlIAE = expectThrows(QlIllegalArgumentException.class,
                () -> isNullOpt.asPipe().asProcessor().process(null));
        assertEquals("cannot cast [foo] to [date]: Text 'foo' could not be parsed at index 0", sqlIAE.getMessage());

        isNull = new IsNull(EMPTY, new Cast(EMPTY, NULL, randomFrom(DataTypes.types()), UTC));
        assertTrue((Boolean) ((IsNull) foldNull.rule(isNull)).asPipe().asProcessor().process(null));
    }

    public void testNullFoldingIsNotNull() {
        FoldNull foldNull = new FoldNull();
        assertEquals(true, foldNull.rule(new IsNotNull(EMPTY, TRUE)).fold());
        assertEquals(false, foldNull.rule(new IsNotNull(EMPTY, NULL)).fold());

        Cast cast = new Cast(EMPTY, L("foo"), DATE, UTC);
        IsNotNull isNotNull = new IsNotNull(EMPTY, cast);
        assertEquals(isNotNull, foldNull.rule(isNotNull));
    }

    public void testNullFoldingIsNotNullWithCast() {
        FoldNull foldNull = new FoldNull();

        Cast cast = new Cast(EMPTY, L("foo"), DATE, UTC);
        IsNotNull isNotNull = new IsNotNull(EMPTY, cast);
        final IsNotNull isNotNullOpt = (IsNotNull) foldNull.rule(isNotNull);
        assertEquals(isNotNull, isNotNullOpt);

        QlIllegalArgumentException sqlIAE = expectThrows(QlIllegalArgumentException.class,
                () -> isNotNullOpt.asPipe().asProcessor().process(null));
        assertEquals("cannot cast [foo] to [date]: Text 'foo' could not be parsed at index 0", sqlIAE.getMessage());

        isNotNull = new IsNotNull(EMPTY, new Cast(EMPTY, NULL, randomFrom(DataTypes.types()), UTC));
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
        assertNullLiteral(rule.rule(greaterThanOf(getFieldAttribute(), NULL)));
        // regex
        assertNullLiteral(rule.rule(new RLike(EMPTY, NULL, new RLikePattern("123"))));
    }

    public void testNullFoldingOnCast() {
        FoldNull foldNull = new FoldNull();

        Cast cast = new Cast(EMPTY, NULL, randomFrom(DataTypes.types()), UTC);
        assertEquals(Nullability.TRUE, cast.nullable());
        assertNull(foldNull.rule(cast).fold());

        cast = new Cast(EMPTY, L("foo"), DATE, UTC);
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
        ArbitraryConditionalFunction conditionalFunction = ctor.newInstance(EMPTY, asList(NULL, ONE, TWO));
        assertEquals(conditionalFunction, rule.rule(conditionalFunction));
        conditionalFunction = ctor.newInstance(EMPTY, asList(NULL, NULL, NULL));
        assertEquals(conditionalFunction, rule.rule(conditionalFunction));
    }

    public void testSimplifyCoalesceRandomNulls() {
        Expression e = new SimplifyConditional().rule(new Coalesce(EMPTY, randomListOfNulls()));
        assertEquals(NULL, e);
    }

    public void testSimplifyCoalesceWithOnlyOneChild() {
        Expression fa = fieldAttribute();
        assertSame(fa, new SimplifyConditional().rule(new Coalesce(EMPTY, singletonList(fa))));
    }

    public void testSimplifyCoalesceSameExpression() {
        Expression fa = fieldAttribute();
        assertSame(fa, new SimplifyConditional().rule(new Coalesce(EMPTY, randomList(2, 10, () -> fa))));
    }

    public void testSimplifyCoalesceRandomNullsWithValue() {
        Expression e = new SimplifyConditional().rule(new Coalesce(EMPTY,
                CollectionUtils.combine(
                        CollectionUtils.combine(randomListOfNulls(), TRUE, FALSE, TRUE),
                        randomListOfNulls())));
        assertEquals(1, e.children().size());
        assertEquals(TRUE, e.children().get(0));
        assertEquals(BOOLEAN, e.dataType());
    }

    private List<Expression> randomListOfNulls() {
        return asList(randomArray(1, 10, Literal[]::new, () -> NULL));
    }

    public void testSimplifyCoalesceFirstLiteral() {
        Expression e = new SimplifyConditional()
                .rule(new Coalesce(EMPTY,
                        asList(NULL, TRUE, FALSE, new Abs(EMPTY, getFieldAttribute()))));
        assertEquals(Coalesce.class, e.getClass());
        assertEquals(1, e.children().size());
        assertEquals(TRUE, e.children().get(0));
        assertEquals(BOOLEAN, e.dataType());
    }

    public void testSimplifyIfNullNulls() {
        Expression e = new SimplifyConditional().rule(new IfNull(EMPTY, NULL, NULL));
        assertEquals(NULL, e);
    }

    public void testSimplifyIfNullWithNullAndValue() {
        Expression e = new SimplifyConditional().rule(new IfNull(EMPTY, NULL, ONE));
        assertEquals(IfNull.class, e.getClass());
        assertEquals(1, e.children().size());
        assertEquals(ONE, e.children().get(0));
        assertEquals(INTEGER, e.dataType());

        e = new SimplifyConditional().rule(new IfNull(EMPTY, ONE, NULL));
        assertEquals(IfNull.class, e.getClass());
        assertEquals(1, e.children().size());
        assertEquals(ONE, e.children().get(0));
        assertEquals(INTEGER, e.dataType());
    }

    public void testFoldNullNotAppliedOnNullIf() {
        Expression orig = new NullIf(EMPTY, ONE, NULL);
        Expression f = new FoldNull().rule(orig);
        assertEquals(orig, f);
    }

    public void testSimplifyNullIf() {
        assertEquals(ONE, new SimplifyConditional().rule(new NullIf(EMPTY, ONE, NULL)));
        assertEquals(NULL, new SimplifyConditional().rule(new NullIf(EMPTY, NULL, ONE)));
        assertEquals(NULL, new SimplifyConditional().rule(new NullIf(EMPTY, NULL, NULL)));
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
        assertEquals(INTEGER, e.dataType());
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
        assertEquals(INTEGER, e.dataType());
    }

    public void testPropagateIsNullOnIfNull() {
        FieldAttribute fa = fieldAttribute();
        And and = new And(EMPTY, new IfNull(EMPTY, fa, ONE), new IsNull(EMPTY, fa));
        Filter f = new Filter(EMPTY, FROM(), and);
        LogicalPlan t = new Optimizer.PropagateNullable().apply(f);
        assertEquals(Filter.class, t.getClass());
        Expression e = ((Filter) t).condition();
        assertEquals(And.class, e.getClass());
        And a = (And) e;
        assertEquals(and.right(), a.right());
        assertEquals(IfNull.class, a.left().getClass());
        assertEquals(1, a.left().children().size());
        assertEquals(ONE, a.left().children().get(0));
    }

    public void testPropagateIsNullOnCoalesce() {
        FieldAttribute fa = fieldAttribute();
        And and = new And(EMPTY, new Coalesce(EMPTY, asList(fa, ONE, fa, TWO)), new IsNull(EMPTY, fa));
        Filter f = new Filter(EMPTY, FROM(), and);
        LogicalPlan t = new Optimizer.PropagateNullable().apply(f);
        assertEquals(Filter.class, t.getClass());
        Expression e = ((Filter) t).condition();
        assertEquals(And.class, e.getClass());
        And a = (And) e;
        assertEquals(and.right(), a.right());
        assertEquals(Coalesce.class, a.left().getClass());
        assertEquals(2, a.left().children().size());
        assertEquals(ONE, a.left().children().get(0));
        assertEquals(TWO, a.left().children().get(1));
    }

    public void testPropagateIsNotNullOnIfNull() {
        FieldAttribute fa = fieldAttribute();
        And and = new And(EMPTY, new IfNull(EMPTY, fa, ONE), new IsNotNull(EMPTY, fa));
        Filter f = new Filter(EMPTY, FROM(), and);
        LogicalPlan t = new Optimizer.PropagateNullable().apply(f);
        assertEquals(Filter.class, t.getClass());
        Expression e = ((Filter) t).condition();
        assertEquals(And.class, e.getClass());
        And a = (And) e;
        assertEquals(and.right(), a.right());
        assertEquals(IfNull.class, a.left().getClass());
        assertEquals(1, a.left().children().size());
        assertEquals(fa, a.left().children().get(0));
    }

    public void testPropagateIsNotNullOnCoalesce() {
        FieldAttribute fa = fieldAttribute();
        And and = new And(EMPTY, new Coalesce(EMPTY, asList(fa, ONE, fa, TWO, THREE)), new IsNotNull(EMPTY, fa));
        Filter f = new Filter(EMPTY, FROM(), and);
        LogicalPlan t = new Optimizer.PropagateNullable().apply(f);
        assertEquals(Filter.class, t.getClass());
        Expression e = ((Filter) t).condition();
        assertEquals(And.class, e.getClass());
        And a = (And) e;
        assertEquals(and.right(), a.right());
        assertEquals(Coalesce.class, a.left().getClass());
        assertEquals(1, a.left().children().size());
        assertEquals(fa, a.left().children().get(0));
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

        Case c = new Case(EMPTY, asList(
                new IfConditional(EMPTY, equalsOf(getFieldAttribute(), ONE), literal("foo1")),
                new IfConditional(EMPTY, equalsOf(ONE, TWO), literal("bar1")),
                new IfConditional(EMPTY, equalsOf(TWO, ONE), literal("bar2")),
                new IfConditional(EMPTY, greaterThanOf(getFieldAttribute(), ONE), literal("foo2")), literal("default")));
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

        Case c = new Case(EMPTY, asList(
                new IfConditional(EMPTY, equalsOf(ONE, TWO), literal("foo1")),
                new IfConditional(EMPTY, equalsOf(ONE, ONE), literal("foo2")), literal("default")));
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

        Case c = new Case(EMPTY, asList(
                new IfConditional(EMPTY, equalsOf(ONE, TWO), literal("foo1")),
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
        Iif iif = new Iif(EMPTY, equalsOf(ONE, ONE), literal("foo"), literal("bar"));
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
        Iif iif = new Iif(EMPTY, equalsOf(ONE, ONE), getFieldAttribute("myField"), literal("bar"));
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
        Iif iif = new Iif(EMPTY, equalsOf(ONE, TWO), literal("foo"), literal("bar"));
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
        Iif iif = new Iif(EMPTY, equalsOf(ONE, TWO), literal("foo"), getFieldAttribute("myField"));
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

    public void testLiteralsOnTheRightInStDistance() {
        Alias a = new Alias(EMPTY, "a", L(10));
        Expression result = new LiteralsOnTheRight().rule(new StDistance(EMPTY, FIVE, a));
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

    //
    // Range optimization
    //

    // Conjunction

    // a != NULL AND a > 1 AND a < 5 AND a == 10  -> (a != NULL AND a == 10) AND 1 <= a < 5
    public void testCombineUnbalancedComparisonsMixedWithEqualsIntoRange() {
        FieldAttribute fa = getFieldAttribute();
        IsNotNull isn = new IsNotNull(EMPTY, fa);
        GreaterThanOrEqual gte = greaterThanOrEqualOf(fa, ONE);

        Equals eq = equalsOf(fa, L(10));
        LessThan lt = lessThanOf(fa, FIVE);

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

    public void testTranslateMinToFirst() {
        Min min1 = new Min(EMPTY, new FieldAttribute(EMPTY, "str", new EsField("str", KEYWORD, emptyMap(), true)));
        Min min2 =  new Min(EMPTY, getFieldAttribute());

        OrderBy plan = new OrderBy(EMPTY, new Aggregate(EMPTY, FROM(), emptyList(),
                asList(a("min1", min1), a("min2", min2))),
            asList(
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
        Max max1 = new Max(EMPTY, new FieldAttribute(EMPTY, "str", new EsField("str", KEYWORD, emptyMap(), true)));
        Max max2 =  new Max(EMPTY, getFieldAttribute());

        OrderBy plan = new OrderBy(EMPTY, new Aggregate(EMPTY, FROM(), emptyList(), asList(a("max1", max1), a("max2", max2))),
            asList(
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
        FieldAttribute firstField = new FieldAttribute(EMPTY, "first_field", new EsField("first_field", BYTE, emptyMap(), true));
        FieldAttribute secondField = new FieldAttribute(EMPTY, "second_field",
                new EsField("second_field", BYTE, emptyMap(), true));
        Alias firstAlias = new Alias(EMPTY, "first_alias", firstField);
        Alias secondAlias = new Alias(EMPTY, "second_alias", secondField);
        Order firstOrderBy = new Order(EMPTY, firstField, OrderDirection.ASC, Order.NullsPosition.LAST);
        Order secondOrderBy = new Order(EMPTY, secondField, OrderDirection.ASC, Order.NullsPosition.LAST);

        OrderBy orderByPlan = new OrderBy(EMPTY,
                new Aggregate(EMPTY, FROM(), asList(secondField, firstField), asList(secondAlias, firstAlias)),
                asList(firstOrderBy, secondOrderBy));
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
        FieldAttribute firstField = new FieldAttribute(EMPTY, "first_field", new EsField("first_field", BYTE, emptyMap(), true));
        FieldAttribute secondField = new FieldAttribute(EMPTY, "second_field",
                new EsField("second_field", BYTE, emptyMap(), true));
        Alias firstAlias = new Alias(EMPTY, "first_alias", firstField);
        Alias secondAlias = new Alias(EMPTY, "second_alias", secondField);
        Order firstOrderBy = new Order(EMPTY, firstAlias, OrderDirection.ASC, Order.NullsPosition.LAST);
        Order secondOrderBy = new Order(EMPTY, secondAlias, OrderDirection.ASC, Order.NullsPosition.LAST);

        OrderBy orderByPlan = new OrderBy(EMPTY,
                new Aggregate(EMPTY, FROM(), asList(secondAlias, firstAlias), asList(secondAlias, firstAlias)),
                asList(firstOrderBy, secondOrderBy));
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
        List<NamedExpression> values = asList(new Alias(EMPTY, "ONE", L(1)), new Alias(EMPTY, "TWO", L(2)));
        List<NamedExpression> aggs = asList(new Alias(EMPTY, "AVG", new Avg(EMPTY, number)));
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
        assertEquals(asList(L(1), L(2)), in.list());
    }

    /**
     * Test queries like SELECT MIN(agg_field), MAX(agg_field) FROM table WHERE MATCH(match_field,'A') AND/OR QUERY('match_field:A')
     * or SELECT STDDEV_POP(agg_field), VAR_POP(agg_field) FROM table WHERE MATCH(match_field,'A') AND/OR QUERY('match_field:A')
     */
    public void testAggregatesPromoteToStats_WithFullTextPredicatesConditions() {
        FieldAttribute matchField = new FieldAttribute(EMPTY, "match_field", new EsField("match_field", TEXT, emptyMap(), true));
        FieldAttribute aggField = new FieldAttribute(EMPTY, "agg_field", new EsField("agg_field", INTEGER, emptyMap(), true));

        FullTextPredicate matchPredicate = new MatchQueryPredicate(EMPTY, matchField, "A", StringUtils.EMPTY);
        FullTextPredicate multiMatchPredicate = new MultiMatchQueryPredicate(EMPTY, "match_field", "A", StringUtils.EMPTY);
        FullTextPredicate stringQueryPredicate = new StringQueryPredicate(EMPTY, "match_field:A", StringUtils.EMPTY);
        List<FullTextPredicate> predicates = asList(matchPredicate, multiMatchPredicate, stringQueryPredicate);

        FullTextPredicate left = randomFrom(predicates);
        FullTextPredicate right = randomFrom(predicates);

        BinaryLogic or = new Or(EMPTY, left, right);
        BinaryLogic and = new And(EMPTY, left, right);
        BinaryLogic condition = randomFrom(or, and);
        Filter filter = new Filter(EMPTY, FROM(), condition);

        List<AggregateFunction> aggregates;
        boolean isSimpleStats = randomBoolean();
        if (isSimpleStats) {
            aggregates = asList(new Avg(EMPTY, aggField), new Sum(EMPTY, aggField), new Min(EMPTY, aggField),
                    new Max(EMPTY, aggField));
        } else {
            aggregates = asList(new StddevPop(EMPTY, aggField), new SumOfSquares(EMPTY, aggField), new VarPop(EMPTY, aggField));
        }
        AggregateFunction firstAggregate = randomFrom(aggregates);
        AggregateFunction secondAggregate = randomValueOtherThan(firstAggregate, () -> randomFrom(aggregates));
        Aggregate aggregatePlan = new Aggregate(EMPTY, filter, singletonList(matchField),
                asList(new Alias(EMPTY, "first", firstAggregate), new Alias(EMPTY, "second", secondAggregate)));
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

        Project p = new Project(EMPTY, FROM(), asList(aAlias, bAlias));
        Filter f = new Filter(EMPTY, p, new And(EMPTY, greaterThanOf(aAlias.toAttribute(), L(1)),
            greaterThanOf(bAlias.toAttribute(), L(2))));

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

    //
    // ReplaceSumWithStats rule
    //
    public void testSumIsReplacedWithStats() {
        FieldAttribute fa = getFieldAttribute();
        Sum sum = new Sum(EMPTY, fa);

        Alias sumAlias = new Alias(EMPTY, "sum", sum);
        EsRelation from = new EsRelation(EMPTY, new EsIndex("table", emptyMap()), false);

        Aggregate aggregate = new Aggregate(EMPTY, from, emptyList(), asList(sumAlias));
        LogicalPlan optimizedPlan = new Optimizer().optimize(aggregate);
        assertTrue(optimizedPlan instanceof Aggregate);
        Aggregate p = (Aggregate) optimizedPlan;
        assertEquals(1, p.aggregates().size());
        assertTrue(p.aggregates().get(0) instanceof Alias);
        Alias alias = (Alias) p.aggregates().get(0);
        assertTrue(alias.child() instanceof InnerAggregate);
        assertEquals(sum, ((InnerAggregate) alias.child()).inner());
    }

    public void testReplaceCast() {
        FieldAttribute a = getFieldAttribute("a");
        Cast c = new Cast(EMPTY, a, a.dataType(), UTC);
        assertSame(a, new Optimizer.PruneCast().maybePruneCast(c));
    }

    public void testReplaceCastOnLiteral() {
        Literal literal = literal("string");
        Cast c = new Cast(EMPTY, literal, literal.dataType(), UTC);
        assertSame(literal, new Optimizer.PruneCast().maybePruneCast(c));
    }

    /**
     * Once https://github.com/elastic/elasticsearch/issues/71582 is addressed (ES `sum` aggregation can differentiate between
     * <code>SUM(all zeroes)</code> and <code>SUM(all nulls)</code>), remove the {@link OptimizerTests#testSumIsReplacedWithStats()}, and
     * re-enable the following test.
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/71582")
    public void testSumIsNotReplacedWithStats() {
        FieldAttribute fa = getFieldAttribute();
        Sum sum = new Sum(EMPTY, fa);

        Alias sumAlias = new Alias(EMPTY, "sum", sum);

        Aggregate aggregate = new Aggregate(EMPTY, FROM(), emptyList(), asList(sumAlias));
        LogicalPlan optimizedPlan = new Optimizer().optimize(aggregate);
        assertTrue(optimizedPlan instanceof Aggregate);
        Aggregate p = (Aggregate) optimizedPlan;
        assertEquals(1, p.aggregates().size());
        assertEquals(sumAlias, p.aggregates().get(0));
    }

    public void testPushProjectionsIntoLocalRelations() {
        // SELECT TRUE as a
        Project plan = new Project(EMPTY,
            new LocalRelation(EMPTY, new SingletonExecutable(emptyList())),
            singletonList(new Alias(EMPTY, "a", TRUE)));

        LogicalPlan optimized = new Optimizer.PushProjectionsIntoLocalRelation().apply(plan);

        assertEquals(LocalRelation.class, optimized.getClass());
        assertEquals(plan.output(), ((LocalRelation) optimized).executable().output());
    }

    public void testSkipQueryForOnlyLiteralAggregations() {
        Aggregate plan = new Aggregate(EMPTY,
            new EsRelation(EMPTY, new EsIndex("table", emptyMap()), false),
            emptyList(),
            singletonList(new Alias(EMPTY, "a", TRUE))
        );

        LogicalPlan optimized = new Optimizer.SkipQueryForLiteralAggregations().apply(plan);

        optimized.forEachDown(LeafPlan.class, l -> {
            assertEquals(LocalRelation.class, l.getClass());
            assertEquals(SingletonExecutable.class, ((LocalRelation) l).executable().getClass());
        });
    }

    public void testDoNotSkipQueryOnEsRelationWithFilter() {
        // SELECT TRUE as a FROM table WHERE col IS NULL
        Project plan = new Project(EMPTY,
            new Filter(EMPTY,
                new EsRelation(EMPTY, new EsIndex("table", emptyMap()), false),
                new IsNull(EMPTY, getFieldAttribute("col"))),
            singletonList(new Alias(EMPTY, "a", TRUE)));

        LogicalPlan optimized = new Optimizer.SkipQueryForLiteralAggregations().apply(plan);

        optimized.forEachDown(LeafPlan.class, l -> {
            assertEquals(EsRelation.class, l.getClass());
        });
    }


}
