/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchOperator;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.LenientCast;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCounter;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.hamcrest.Matchers.equalTo;

/**
 * Parsing of the lenient cast operator {@code <value>?::<type>}, and how it coexists with the {@code ?} parameter markers
 * ({@code ?}, {@code ?name}, {@code ?1}, {@code ??}, {@code ??name}) and the plain {@code ::} cast.
 */
public class LenientCastParserTests extends AbstractStatementParserTests {

    @Before
    public void requireLenientCastOperator() {
        assumeTrue("requires the lenient cast operator", EsqlCapabilities.Cap.LENIENT_CAST_OPERATOR.isEnabled());
    }

    public void testLenientCastOnField() {
        Expression e = evalExpression("x?::long");
        LenientCast cast = as(e, LenientCast.class);
        assertThat(cast.source().text(), equalTo("x?::long"));
        ToLong toLong = as(cast.child(), ToLong.class);
        assertThat(toLong.source().text(), equalTo("x?::long"));
        assertThat(as(toLong.field(), UnresolvedAttribute.class).name(), equalTo("x"));
    }

    public void testLenientCastOnLiteral() {
        LenientCast cast = as(evalExpression("1?::long"), LenientCast.class);
        ToLong toLong = as(cast.child(), ToLong.class);
        assertThat(as(toLong.field(), Literal.class).value(), equalTo(1));
    }

    public void testLenientCastAllowsWhitespaceAroundTheOperator() {
        for (String expression : List.of("x ?:: long", "x ?::long", "x?:: long", "x   ?::   long")) {
            LenientCast cast = as(evalExpression(expression), LenientCast.class);
            ToLong toLong = as(cast.child(), ToLong.class);
            assertThat(expression, as(toLong.field(), UnresolvedAttribute.class).name(), equalTo("x"));
        }
    }

    public void testLenientCastRejectsWhitespaceInsideTheOperator() {
        for (String expression : List.of("x? ::long", "x ? :: long", "x?\n::long")) {
            expectError("row a = 1 | eval y = " + expression, "the lenient cast operator [?::] must not contain whitespace");
        }
    }

    public void testLenientCastInsideExpressions() {
        Neg neg = as(evalExpression("-x?::long"), Neg.class);
        as(as(neg.field(), LenientCast.class).child(), ToLong.class);

        Add add = as(evalExpression("x?::long + 1"), Add.class);
        as(as(add.left(), LenientCast.class).child(), ToLong.class);
        assertThat(as(add.right(), Literal.class).value(), equalTo(1));

        UnresolvedFunction toString = as(evalExpression("TO_STRING(x?::long)"), UnresolvedFunction.class);
        as(as(toString.children().getFirst(), LenientCast.class).child(), ToLong.class);
    }

    public void testLenientCastOnFunctionAndParenthesizedOperands() {
        LenientCast onFunction = as(evalExpression("f(x)?::long"), LenientCast.class);
        assertThat(as(as(onFunction.child(), ToLong.class).field(), UnresolvedFunction.class).name(), equalTo("f"));

        LenientCast onParenthesized = as(evalExpression("(a + b)?::long"), LenientCast.class);
        as(as(onParenthesized.child(), ToLong.class).field(), Add.class);
    }

    public void testNestedCasts() {
        // Only the inner cast is lenient.
        ToDouble outerStrict = as(evalExpression("x?::long::double"), ToDouble.class);
        LenientCast innerLenient = as(outerStrict.field(), LenientCast.class);
        assertThat(as(as(innerLenient.child(), ToLong.class).field(), UnresolvedAttribute.class).name(), equalTo("x"));

        // Only the outer cast is lenient.
        LenientCast outerLenient = as(evalExpression("x::long?::double"), LenientCast.class);
        ToDouble toDouble = as(outerLenient.child(), ToDouble.class);
        ToLong innerStrict = as(toDouble.field(), ToLong.class);
        assertThat(as(innerStrict.field(), UnresolvedAttribute.class).name(), equalTo("x"));

        // Both casts are lenient.
        LenientCast both = as(evalExpression("x?::long?::double"), LenientCast.class);
        ToDouble bothOuter = as(both.child(), ToDouble.class);
        LenientCast bothInner = as(bothOuter.field(), LenientCast.class);
        as(bothInner.child(), ToLong.class);
    }

    public void testLenientCastToVirtualCounterTarget() {
        assumeTrue("requires TO_COUNTER", EsqlCapabilities.Cap.TO_COUNTER.isEnabled());
        LenientCast cast = as(evalExpression("x?::" + DataType.COUNTER_CAST_NAME), LenientCast.class);
        as(cast.child(), ToCounter.class);
    }

    public void testAnonymousParamCastIsUnchanged() {
        // `?::long` is the anonymous positional parameter cast to long, not a lenient cast with a missing operand.
        Expression e = evalExpression("?::long", List.of(paramAsConstant(null, 1)));
        ToLong toLong = as(e, ToLong.class);
        assertThat(as(toLong.field(), Literal.class).value(), equalTo(1));
    }

    public void testNamedAndPositionalParamCastsAreUnchanged() {
        ToLong named = as(evalExpression("?p::long", List.of(paramAsConstant("p", 1))), ToLong.class);
        assertThat(as(named.field(), Literal.class).value(), equalTo(1));

        ToLong positional = as(evalExpression("?1::long", List.of(paramAsConstant(null, 1))), ToLong.class);
        assertThat(as(positional.field(), Literal.class).value(), equalTo(1));
    }

    public void testLenientCastOnParams() {
        LenientCast named = as(evalExpression("?p?::long", List.of(paramAsConstant("p", 1))), LenientCast.class);
        assertThat(as(as(named.child(), ToLong.class).field(), Literal.class).value(), equalTo(1));

        LenientCast positional = as(evalExpression("?1?::long", List.of(paramAsConstant(null, 1))), LenientCast.class);
        assertThat(as(as(positional.child(), ToLong.class).field(), Literal.class).value(), equalTo(1));

        LenientCast anonymous = as(evalExpression("? ?::long", List.of(paramAsConstant(null, 1))), LenientCast.class);
        assertThat(as(as(anonymous.child(), ToLong.class).field(), Literal.class).value(), equalTo(1));
    }

    public void testDoubleParamCastsAreUnchanged() {
        ToLong named = as(evalExpression("??f::long", List.of(paramAsConstant("f", "field"))), ToLong.class);
        assertThat(as(named.field(), UnresolvedAttribute.class).name(), equalTo("field"));

        // `??::long` lexes as DOUBLE_PARAMS followed by `::`.
        ToLong anonymous = as(evalExpression("??::long", List.of(paramAsConstant(null, "field"))), ToLong.class);
        assertThat(as(anonymous.field(), UnresolvedAttribute.class).name(), equalTo("field"));
    }

    public void testLenientCastOnDoubleParams() {
        LenientCast named = as(evalExpression("??f?::long", List.of(paramAsConstant("f", "field"))), LenientCast.class);
        assertThat(as(as(named.child(), ToLong.class).field(), UnresolvedAttribute.class).name(), equalTo("field"));

        LenientCast positional = as(evalExpression("??1?::long", List.of(paramAsConstant(null, "field"))), LenientCast.class);
        assertThat(as(as(positional.child(), ToLong.class).field(), UnresolvedAttribute.class).name(), equalTo("field"));
    }

    public void testParamsAsValuesAreUnaffected() {
        Equals equals = as(
            as(query("row a = 1 | where x == ?", new QueryParams(List.of(paramAsConstant(null, 1)))), Filter.class).condition(),
            Equals.class
        );
        assertThat(as(equals.right(), Literal.class).value(), equalTo(1));

        In in = as(
            as(
                query("row a = 1 | where x in (?, ?)", new QueryParams(List.of(paramAsConstant(null, 1), paramAsConstant(null, 2)))),
                Filter.class
            ).condition(),
            In.class
        );
        assertThat(in.list().size(), equalTo(2));
    }

    public void testLenientCastInCommands() {
        Filter where = as(processingCommand("where x?::long > 1"), Filter.class);
        GreaterThan gt = as(where.condition(), GreaterThan.class);
        as(gt.left(), LenientCast.class);

        Aggregate stats = as(processingCommand("stats c = count(*) by x?::long"), Aggregate.class);
        assertThat(stats.groupings().size(), equalTo(1));
        assertThat(stats.groupings().getFirst().anyMatch(LenientCast.class::isInstance), equalTo(true));

        OrderBy sort = as(processingCommand("sort x?::long"), OrderBy.class);
        Order order = sort.order().getFirst();
        as(order.child(), LenientCast.class);

        In inList = as(as(processingCommand("where x?::long in (1, 2)"), Filter.class).condition(), In.class);
        as(inList.value(), LenientCast.class);

        In inValues = as(as(processingCommand("where 1 in (x?::long, y)"), Filter.class).condition(), In.class);
        as(inValues.list().getFirst(), LenientCast.class);
    }

    public void testLenientCastInMatchOperatorField() {
        MatchOperator match = as(as(query("FROM test | WHERE x?::keyword : \"value\""), Filter.class).condition(), MatchOperator.class);
        as(match.field(), LenientCast.class);
    }

    public void testRejectedForms() {
        // `??` lexes as DOUBLE_PARAMS, which cannot follow an expression.
        expectThrows(ParsingException.class, () -> query("row a = 1 | eval y = x??::long"));
        // `?1` lexes as a positional parameter, which cannot follow an expression.
        expectThrows(ParsingException.class, () -> query("row a = 1 | eval y = x?1::long"));
        // The target type is required.
        expectThrows(ParsingException.class, () -> query("row a = 1 | eval y = x?::"));
        expectError("row a = 1 | eval y = x?::unknown_type", "Unknown data type named [unknown_type]");
    }

    public void testFromSelectorsAreUnaffected() {
        UnresolvedRelation relation = as(query("FROM foo::failures"), UnresolvedRelation.class);
        assertThat(relation.indexPattern().indexPattern(), equalTo("foo::failures"));
    }

    private Expression evalExpression(String expression) {
        return evalExpression(expression, List.of());
    }

    private Expression evalExpression(String expression, List<QueryParam> params) {
        LogicalPlan plan = query("row a = 1 | eval y = " + expression, new QueryParams(params));
        return as(plan, Eval.class).fields().getFirst().child();
    }
}
