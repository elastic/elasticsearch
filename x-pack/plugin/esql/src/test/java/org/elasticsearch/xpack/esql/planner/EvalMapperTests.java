/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.TestBlockFactory;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateFormat;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pow;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Length;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Substring;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class EvalMapperTests extends ESTestCase {
    private static final FieldAttribute DOUBLE1 = field("foo", DataType.DOUBLE);
    private static final FieldAttribute DOUBLE2 = field("bar", DataType.DOUBLE);
    private static final FieldAttribute LONG = field("long", DataType.LONG);
    private static final FieldAttribute DATE = field("date", DataType.DATETIME);

    private static final Configuration TEST_CONFIG = new Configuration(
        ZoneOffset.UTC,
        Locale.US,
        "test",
        null,
        null,
        10000000,
        10000,
        StringUtils.EMPTY,
        false,
        Map.of(),
        System.nanoTime()
    );

    @ParametersFactory(argumentFormatting = "%1$s")
    public static List<Object[]> params() {
        Literal literal = new Literal(Source.EMPTY, new BytesRef("something"), DataType.KEYWORD);
        Literal datePattern = new Literal(Source.EMPTY, new BytesRef("yyyy"), DataType.KEYWORD);
        Literal dateInterval = new Literal(Source.EMPTY, Duration.ofHours(1), DataType.TIME_DURATION);

        List<Object[]> params = new ArrayList<>();
        for (Expression e : new Expression[] {
            new Add(Source.EMPTY, DOUBLE1, DOUBLE2),
            new Sub(Source.EMPTY, DOUBLE1, DOUBLE2),
            new Mul(Source.EMPTY, DOUBLE1, DOUBLE2),
            new Div(Source.EMPTY, DOUBLE1, DOUBLE2),
            new Mod(Source.EMPTY, DOUBLE1, DOUBLE2),
            new Neg(Source.EMPTY, DOUBLE1),
            new Abs(Source.EMPTY, DOUBLE1),
            new Equals(Source.EMPTY, DOUBLE1, DOUBLE2),
            new GreaterThan(Source.EMPTY, DOUBLE1, DOUBLE2, null),
            new GreaterThanOrEqual(Source.EMPTY, DOUBLE1, DOUBLE2, null),
            new LessThan(Source.EMPTY, DOUBLE1, DOUBLE2, null),
            new LessThanOrEqual(Source.EMPTY, DOUBLE1, DOUBLE2, null),
            new And(
                Source.EMPTY,
                new LessThan(Source.EMPTY, DOUBLE1, DOUBLE2, null),
                new LessThanOrEqual(Source.EMPTY, DOUBLE1, DOUBLE2, null)
            ),
            new Or(
                Source.EMPTY,
                new LessThan(Source.EMPTY, DOUBLE1, DOUBLE2, null),
                new LessThanOrEqual(Source.EMPTY, DOUBLE1, DOUBLE2, null)
            ),
            new Not(Source.EMPTY, new LessThan(Source.EMPTY, DOUBLE1, DOUBLE2, null)),
            new Concat(Source.EMPTY, literal, Collections.emptyList()),
            new Round(Source.EMPTY, DOUBLE1, LONG),
            new Pow(Source.EMPTY, DOUBLE1, DOUBLE2),
            DOUBLE1,
            literal,
            new Length(Source.EMPTY, literal),
            new DateFormat(Source.EMPTY, datePattern, DATE, TEST_CONFIG),
            new DateFormat(Source.EMPTY, datePattern, literal, TEST_CONFIG),
            new StartsWith(Source.EMPTY, literal, literal),
            new Substring(Source.EMPTY, literal, LONG, LONG),
            new DateTrunc(Source.EMPTY, dateInterval, DATE) }) {
            params.add(new Object[] { e.nodeString(), e });
        }

        return params;
    }

    private final String nodeString;
    private final Expression expression;

    public EvalMapperTests(String nodeString, Expression expression) {
        this.nodeString = nodeString;
        this.expression = expression;
    }

    public void testEvaluatorSuppliers() {
        Layout.Builder lb = new Layout.Builder();
        lb.append(DOUBLE1);
        lb.append(DOUBLE2);
        lb.append(DATE);
        lb.append(LONG);
        Layout layout = lb.build();

        var supplier = EvalMapper.toEvaluator(expression, layout);
        EvalOperator.ExpressionEvaluator evaluator1 = supplier.get(driverContext());
        EvalOperator.ExpressionEvaluator evaluator2 = supplier.get(driverContext());
        assertNotNull(evaluator1);
        assertNotNull(evaluator2);
        assertTrue(evaluator1 != evaluator2);
    }

    // Test serialization of expressions, since we have convenient access to some expressions.
    public void testExpressionSerialization() {
        SerializationTestUtils.assertSerialization(expression, TEST_CONFIG);
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Collections.emptyMap(), false));
    }

    static DriverContext driverContext() {
        return new DriverContext(
            new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService()).withCircuitBreaking(),
            TestBlockFactory.getNonBreakingInstance()
        );
    }
}
