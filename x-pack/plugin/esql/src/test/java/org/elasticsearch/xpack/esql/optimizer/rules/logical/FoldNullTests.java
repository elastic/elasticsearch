/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Median;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MedianAbsoluteDeviation;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialCentroid;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.Categorize;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateExtract;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateFormat;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateParse;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.CIDRMatch;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cos;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pow;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.AbstractMultivalueFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvAvg;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvCount;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvDedupe;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvFirst;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLast;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMedian;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSum;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.LTrim;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Substring;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.lang.reflect.Constructor;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.L;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.core.expression.Literal.NULL;
import static org.elasticsearch.xpack.esql.core.expression.Literal.TRUE;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;

public class FoldNullTests extends ESTestCase {
    private Expression foldNull(Expression e) {
        return new FoldNull().rule(e, unboundLogicalOptimizerContext());
    }

    public void testBasicNullFolding() {
        assertNullLiteral(foldNull(new Add(EMPTY, L(randomInt()), Literal.NULL)));
        assertNullLiteral(foldNull(new Round(EMPTY, Literal.NULL, null)));
        assertNullLiteral(foldNull(new Pow(EMPTY, Literal.NULL, Literal.NULL)));
        assertNullLiteral(foldNull(new DateFormat(EMPTY, Literal.NULL, Literal.NULL, null)));
        assertNullLiteral(foldNull(new DateParse(EMPTY, Literal.NULL, Literal.NULL)));
        assertNullLiteral(foldNull(new DateTrunc(EMPTY, Literal.NULL, Literal.NULL)));
        assertNullLiteral(foldNull(new Substring(EMPTY, Literal.NULL, Literal.NULL, Literal.NULL)));
    }

    public void testNullFoldingIsNotNull() {
        assertEquals(true, foldNull(new IsNotNull(EMPTY, TRUE)).fold(FoldContext.small()));
        assertEquals(false, foldNull(new IsNotNull(EMPTY, NULL)).fold(FoldContext.small()));
    }

    @SuppressWarnings("unchecked")
    public void testNullFoldingDoesNotApplyOnAbstractMultivalueFunction() throws Exception {
        List<Class<? extends AbstractMultivalueFunction>> items = List.of(
            MvDedupe.class,
            MvFirst.class,
            MvLast.class,
            MvMax.class,
            MvMedian.class,
            MvMin.class,
            MvSum.class
        );
        for (Class<? extends AbstractMultivalueFunction> clazz : items) {
            Constructor<? extends AbstractMultivalueFunction> ctor = clazz.getConstructor(Source.class, Expression.class);
            AbstractMultivalueFunction conditionalFunction = ctor.newInstance(EMPTY, getFieldAttribute("a"));
            assertEquals(conditionalFunction, foldNull(conditionalFunction));

            conditionalFunction = ctor.newInstance(EMPTY, NULL);
            assertEquals(NULL, foldNull(conditionalFunction));
        }

        // avg and count ar different just because they know the return type in advance (all the others infer the type from the input)
        MvAvg avg = new MvAvg(EMPTY, getFieldAttribute("a"));
        assertEquals(avg, foldNull(avg));
        avg = new MvAvg(EMPTY, NULL);
        assertEquals(new Literal(EMPTY, null, DOUBLE), foldNull(avg));

        MvCount count = new MvCount(EMPTY, getFieldAttribute("a"));
        assertEquals(count, foldNull(count));
        count = new MvCount(EMPTY, NULL);
        assertEquals(new Literal(EMPTY, null, INTEGER), foldNull(count));
    }

    public void testNullFoldingIsNull() {
        assertEquals(true, foldNull(new IsNull(EMPTY, NULL)).fold(FoldContext.small()));
        assertEquals(false, foldNull(new IsNull(EMPTY, TRUE)).fold(FoldContext.small()));
    }

    public void testGenericNullableExpression() {
        FoldNull rule = new FoldNull();
        // arithmetic
        assertNullLiteral(foldNull(new Add(EMPTY, getFieldAttribute("a"), NULL)));
        // comparison
        assertNullLiteral(foldNull(greaterThanOf(getFieldAttribute("a"), NULL)));
        // regex
        assertNullLiteral(foldNull(new RLike(EMPTY, NULL, new RLikePattern("123"))));
        // date functions
        assertNullLiteral(foldNull(new DateExtract(EMPTY, NULL, NULL, configuration(""))));
        // math functions
        assertNullLiteral(foldNull(new Cos(EMPTY, NULL)));
        // string functions
        assertNullLiteral(foldNull(new LTrim(EMPTY, NULL)));
        // spatial
        assertNullLiteral(foldNull(new SpatialCentroid(EMPTY, NULL)));
        // ip
        assertNullLiteral(foldNull(new CIDRMatch(EMPTY, NULL, List.of(NULL))));
        // conversion
        assertNullLiteral(foldNull(new ToString(EMPTY, NULL)));
    }

    public void testNullFoldingDoesNotApplyOnLogicalExpressions() {
        Or or = new Or(EMPTY, NULL, TRUE);
        assertEquals(or, foldNull(or));
        or = new Or(EMPTY, NULL, NULL);
        assertEquals(or, foldNull(or));

        And and = new And(EMPTY, NULL, TRUE);
        assertEquals(and, foldNull(and));
        and = new And(EMPTY, NULL, NULL);
        assertEquals(and, foldNull(and));
    }

    @SuppressWarnings("unchecked")
    public void testNullFoldingDoesNotApplyOnAggregate() throws Exception {
        List<Class<? extends AggregateFunction>> items = List.of(Max.class, Min.class);
        for (Class<? extends AggregateFunction> clazz : items) {
            Constructor<? extends AggregateFunction> ctor = clazz.getConstructor(Source.class, Expression.class);
            AggregateFunction conditionalFunction = ctor.newInstance(EMPTY, getFieldAttribute("a"));
            assertEquals(conditionalFunction, foldNull(conditionalFunction));

            conditionalFunction = ctor.newInstance(EMPTY, NULL);
            assertEquals(NULL, foldNull(conditionalFunction));
        }

        Avg avg = new Avg(EMPTY, getFieldAttribute("a"));
        assertEquals(avg, foldNull(avg));
        avg = new Avg(EMPTY, NULL);
        assertEquals(new Literal(EMPTY, null, DOUBLE), foldNull(avg));

        Count count = new Count(EMPTY, getFieldAttribute("a"));
        assertEquals(count, foldNull(count));
        count = new Count(EMPTY, NULL);
        assertEquals(count, foldNull(count));

        CountDistinct countd = new CountDistinct(EMPTY, getFieldAttribute("a"), getFieldAttribute("a"));
        assertEquals(countd, foldNull(countd));
        countd = new CountDistinct(EMPTY, NULL, NULL);
        assertEquals(new Literal(EMPTY, null, LONG), foldNull(countd));

        Median median = new Median(EMPTY, getFieldAttribute("a"));
        assertEquals(median, foldNull(median));
        median = new Median(EMPTY, NULL);
        assertEquals(new Literal(EMPTY, null, DOUBLE), foldNull(median));

        MedianAbsoluteDeviation medianad = new MedianAbsoluteDeviation(EMPTY, getFieldAttribute("a"));
        assertEquals(medianad, foldNull(medianad));
        medianad = new MedianAbsoluteDeviation(EMPTY, NULL);
        assertEquals(new Literal(EMPTY, null, DOUBLE), foldNull(medianad));

        Percentile percentile = new Percentile(EMPTY, getFieldAttribute("a"), getFieldAttribute("a"));
        assertEquals(percentile, foldNull(percentile));
        percentile = new Percentile(EMPTY, NULL, NULL);
        assertEquals(new Literal(EMPTY, null, DOUBLE), foldNull(percentile));

        Sum sum = new Sum(EMPTY, getFieldAttribute("a"));
        assertEquals(sum, foldNull(sum));
        sum = new Sum(EMPTY, NULL);
        assertEquals(new Literal(EMPTY, null, DOUBLE), foldNull(sum));
    }

    public void testNullFoldableDoesNotApplyToIsNullAndNotNull() {
        DataType numericType = randomFrom(INTEGER, LONG, DOUBLE);
        DataType genericType = randomFrom(INTEGER, LONG, DOUBLE, UNSIGNED_LONG, KEYWORD, TEXT, GEO_POINT, GEO_SHAPE, VERSION, IP);
        List<Expression> items = List.of(
            new Add(EMPTY, getFieldAttribute("a", numericType), getFieldAttribute("b", numericType)),
            new Add(EMPTY, new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, List.of(1, 2, 3), INTEGER)),
            new Sub(EMPTY, getFieldAttribute("a", numericType), getFieldAttribute("b", numericType)),
            new Sub(EMPTY, new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, List.of(1, 2, 3), INTEGER)),
            new Mul(EMPTY, getFieldAttribute("a", numericType), getFieldAttribute("b", numericType)),
            new Mul(EMPTY, new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, List.of(1, 2, 3), INTEGER)),
            new Div(EMPTY, getFieldAttribute("a", numericType), getFieldAttribute("b", numericType)),
            new Div(EMPTY, new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, List.of(1, 2, 3), INTEGER)),

            new GreaterThan(EMPTY, getFieldAttribute("a", numericType), getFieldAttribute("b", numericType), randomZone()),
            new GreaterThan(EMPTY, new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, List.of(1, 2, 3), INTEGER), randomZone()),
            new GreaterThanOrEqual(EMPTY, getFieldAttribute("a", numericType), getFieldAttribute("b", numericType), randomZone()),
            new GreaterThanOrEqual(EMPTY, new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, List.of(1, 2, 3), INTEGER), randomZone()),
            new LessThan(EMPTY, getFieldAttribute("a", numericType), getFieldAttribute("b", numericType), randomZone()),
            new LessThan(EMPTY, new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, List.of(1, 2, 3), INTEGER), randomZone()),
            new LessThanOrEqual(EMPTY, getFieldAttribute("a", numericType), getFieldAttribute("b", numericType), randomZone()),
            new LessThanOrEqual(EMPTY, new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, List.of(1, 2, 3), INTEGER), randomZone()),
            new NotEquals(EMPTY, getFieldAttribute("a", numericType), getFieldAttribute("b", numericType), randomZone()),
            new NotEquals(EMPTY, new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, List.of(1, 2, 3), INTEGER), randomZone()),

            new Equals(EMPTY, getFieldAttribute("a", genericType), getFieldAttribute("b", genericType)),
            new Equals(EMPTY, new Literal(EMPTY, 1, INTEGER), new Literal(EMPTY, List.of(1, 2, 3), INTEGER))
        );
        for (Expression item : items) {
            Expression isNull = new IsNull(EMPTY, item);
            Expression transformed = foldNull(isNull);
            assertEquals(isNull, transformed);

            IsNotNull isNotNull = new IsNotNull(EMPTY, item);
            transformed = foldNull(isNotNull);
            assertEquals(isNotNull, transformed);
        }
    }

    public void testNullBucketGetsFolded() {
        assertEquals(NULL, foldNull(new Bucket(EMPTY, NULL, NULL, NULL, NULL)));
    }

    public void testNullCategorizeGroupingNotFolded() {
        Categorize categorize = new Categorize(EMPTY, NULL);
        assertEquals(categorize, foldNull(categorize));
    }

    private void assertNullLiteral(Expression expression) {
        assertNull(as(expression, Literal.class).value());
    }

}
