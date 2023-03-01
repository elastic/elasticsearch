/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateFormat;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Length;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Substring;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;

import java.time.Duration;
import java.util.Collections;
import java.util.function.Supplier;

public class EvalMapperTests extends ESTestCase {

    public void testEvaluatorSuppliers() {
        Literal literal = new Literal(Source.EMPTY, new BytesRef("something"), DataTypes.KEYWORD);
        FieldAttribute double1 = field("foo", DataTypes.DOUBLE);
        FieldAttribute double2 = field("bar", DataTypes.DOUBLE);
        FieldAttribute longField = field("long", DataTypes.LONG);
        FieldAttribute date = field("date", DataTypes.DATETIME);
        Literal datePattern = new Literal(Source.EMPTY, new BytesRef("yyyy"), DataTypes.KEYWORD);
        Literal dateInterval = new Literal(Source.EMPTY, Duration.ofHours(1), EsqlDataTypes.TIME_DURATION);

        Layout.Builder lb = new Layout.Builder();
        lb.appendChannel(double1.id());
        lb.appendChannel(double2.id());
        lb.appendChannel(date.id());
        lb.appendChannel(longField.id());
        Layout layout = lb.build();

        Expression[] expressions = {
            new Add(Source.EMPTY, double1, double2),
            new Sub(Source.EMPTY, double1, double2),
            new Mul(Source.EMPTY, double1, double2),
            new Div(Source.EMPTY, double1, double2),
            new Abs(Source.EMPTY, double1),
            new Equals(Source.EMPTY, double1, double2),
            new GreaterThan(Source.EMPTY, double1, double2, null),
            new GreaterThanOrEqual(Source.EMPTY, double1, double2, null),
            new LessThan(Source.EMPTY, double1, double2, null),
            new LessThanOrEqual(Source.EMPTY, double1, double2, null),
            new And(
                Source.EMPTY,
                new LessThan(Source.EMPTY, double1, double2, null),
                new LessThanOrEqual(Source.EMPTY, double1, double2, null)
            ),
            new Or(
                Source.EMPTY,
                new LessThan(Source.EMPTY, double1, double2, null),
                new LessThanOrEqual(Source.EMPTY, double1, double2, null)
            ),
            new Not(Source.EMPTY, new LessThan(Source.EMPTY, double1, double2, null)),
            new Concat(Source.EMPTY, literal, Collections.emptyList()),
            new Round(Source.EMPTY, double1, double2),
            double1,
            literal,
            new Length(Source.EMPTY, literal),
            new DateFormat(Source.EMPTY, date, datePattern),
            new StartsWith(Source.EMPTY, literal, literal),
            new Substring(Source.EMPTY, literal, longField, longField),
            new DateTrunc(Source.EMPTY, date, dateInterval) };

        for (Expression expression : expressions) {
            Supplier<EvalOperator.ExpressionEvaluator> supplier = EvalMapper.toEvaluator(expression, layout);
            EvalOperator.ExpressionEvaluator evaluator1 = supplier.get();
            EvalOperator.ExpressionEvaluator evaluator2 = supplier.get();
            assertNotNull(evaluator1);
            assertNotNull(evaluator2);
            assertTrue(evaluator1 != evaluator2);
        }
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Collections.emptyMap(), false));
    }
}
