/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNull;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Substring;
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
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOf;
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

    public void testBasicNullFolding() {
        FoldNull rule = new FoldNull();
        assertNullLiteral(rule.rule(new Add(EMPTY, L(randomInt()), Literal.NULL)));
        assertNullLiteral(rule.rule(new Round(EMPTY, Literal.NULL, null)));
        assertNullLiteral(rule.rule(new Pow(EMPTY, Literal.NULL, Literal.NULL)));
        assertNullLiteral(rule.rule(new DateFormat(EMPTY, Literal.NULL, Literal.NULL, null)));
        assertNullLiteral(rule.rule(new DateParse(EMPTY, Literal.NULL, Literal.NULL)));
        assertNullLiteral(rule.rule(new DateTrunc(EMPTY, Literal.NULL, Literal.NULL)));
        assertNullLiteral(rule.rule(new Substring(EMPTY, Literal.NULL, Literal.NULL, Literal.NULL)));
    }

    public void testNullFoldingIsNotNull() {
        FoldNull foldNull = new FoldNull();
        assertEquals(true, foldNull.rule(new IsNotNull(EMPTY, TRUE)).fold());
        assertEquals(false, foldNull.rule(new IsNotNull(EMPTY, NULL)).fold());
    }

    @SuppressWarnings("unchecked")
    public void testNullFoldingDoesNotApplyOnAbstractMultivalueFunction() throws Exception {
        FoldNull rule = new FoldNull();

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
            assertEquals(conditionalFunction, rule.rule(conditionalFunction));

            conditionalFunction = ctor.newInstance(EMPTY, NULL);
            assertEquals(NULL, rule.rule(conditionalFunction));
        }

        // avg and count ar different just because they know the return type in advance (all the others infer the type from the input)
        MvAvg avg = new MvAvg(EMPTY, getFieldAttribute("a"));
        assertEquals(avg, rule.rule(avg));
        avg = new MvAvg(EMPTY, NULL);
        assertEquals(new Literal(EMPTY, null, DOUBLE), rule.rule(avg));

        MvCount count = new MvCount(EMPTY, getFieldAttribute("a"));
        assertEquals(count, rule.rule(count));
        count = new MvCount(EMPTY, NULL);
        assertEquals(new Literal(EMPTY, null, INTEGER), rule.rule(count));
    }

    public void testNullFoldingIsNull() {
        FoldNull foldNull = new FoldNull();
        assertEquals(true, foldNull.rule(new IsNull(EMPTY, NULL)).fold());
        assertEquals(false, foldNull.rule(new IsNull(EMPTY, TRUE)).fold());
    }

    public void testGenericNullableExpression() {
        FoldNull rule = new FoldNull();
        // arithmetic
        assertNullLiteral(rule.rule(new Add(EMPTY, getFieldAttribute("a"), NULL)));
        // comparison
        assertNullLiteral(rule.rule(greaterThanOf(getFieldAttribute("a"), NULL)));
        // regex
        assertNullLiteral(rule.rule(new RLike(EMPTY, NULL, new RLikePattern("123"))));
        // date functions
        assertNullLiteral(rule.rule(new DateExtract(EMPTY, NULL, NULL, configuration(""))));
        // math functions
        assertNullLiteral(rule.rule(new Cos(EMPTY, NULL)));
        // string functions
        assertNullLiteral(rule.rule(new LTrim(EMPTY, NULL)));
        // spatial
        assertNullLiteral(rule.rule(new SpatialCentroid(EMPTY, NULL)));
        // ip
        assertNullLiteral(rule.rule(new CIDRMatch(EMPTY, NULL, List.of(NULL))));
        // conversion
        assertNullLiteral(rule.rule(new ToString(EMPTY, NULL)));
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
    public void testNullFoldingDoesNotApplyOnAggregate() throws Exception {
        FoldNull rule = new FoldNull();

        List<Class<? extends AggregateFunction>> items = List.of(Max.class, Min.class);
        for (Class<? extends AggregateFunction> clazz : items) {
            Constructor<? extends AggregateFunction> ctor = clazz.getConstructor(Source.class, Expression.class);
            AggregateFunction conditionalFunction = ctor.newInstance(EMPTY, getFieldAttribute("a"));
            assertEquals(conditionalFunction, rule.rule(conditionalFunction));

            conditionalFunction = ctor.newInstance(EMPTY, NULL);
            assertEquals(NULL, rule.rule(conditionalFunction));
        }

        Avg avg = new Avg(EMPTY, getFieldAttribute("a"));
        assertEquals(avg, rule.rule(avg));
        avg = new Avg(EMPTY, NULL);
        assertEquals(new Literal(EMPTY, null, DOUBLE), rule.rule(avg));

        Count count = new Count(EMPTY, getFieldAttribute("a"));
        assertEquals(count, rule.rule(count));
        count = new Count(EMPTY, NULL);
        assertEquals(count, rule.rule(count));

        CountDistinct countd = new CountDistinct(EMPTY, getFieldAttribute("a"), getFieldAttribute("a"));
        assertEquals(countd, rule.rule(countd));
        countd = new CountDistinct(EMPTY, NULL, NULL);
        assertEquals(new Literal(EMPTY, null, LONG), rule.rule(countd));

        Median median = new Median(EMPTY, getFieldAttribute("a"));
        assertEquals(median, rule.rule(median));
        median = new Median(EMPTY, NULL);
        assertEquals(new Literal(EMPTY, null, DOUBLE), rule.rule(median));

        MedianAbsoluteDeviation medianad = new MedianAbsoluteDeviation(EMPTY, getFieldAttribute("a"));
        assertEquals(medianad, rule.rule(medianad));
        medianad = new MedianAbsoluteDeviation(EMPTY, NULL);
        assertEquals(new Literal(EMPTY, null, DOUBLE), rule.rule(medianad));

        Percentile percentile = new Percentile(EMPTY, getFieldAttribute("a"), getFieldAttribute("a"));
        assertEquals(percentile, rule.rule(percentile));
        percentile = new Percentile(EMPTY, NULL, NULL);
        assertEquals(new Literal(EMPTY, null, DOUBLE), rule.rule(percentile));

        Sum sum = new Sum(EMPTY, getFieldAttribute("a"));
        assertEquals(sum, rule.rule(sum));
        sum = new Sum(EMPTY, NULL);
        assertEquals(new Literal(EMPTY, null, DOUBLE), rule.rule(sum));
    }

    public void testNullFoldableDoesNotApplyToIsNullAndNotNull() {
        FoldNull rule = new FoldNull();

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
            Expression transformed = rule.rule(isNull);
            assertEquals(isNull, transformed);

            IsNotNull isNotNull = new IsNotNull(EMPTY, item);
            transformed = rule.rule(isNotNull);
            assertEquals(isNotNull, transformed);
        }
    }

    public void testNullBucketGetsFolded() {
        FoldNull foldNull = new FoldNull();
        assertEquals(NULL, foldNull.rule(new Bucket(EMPTY, NULL, NULL, NULL, NULL)));
    }

    public void testNullCategorizeGroupingNotFolded() {
        FoldNull foldNull = new FoldNull();
        Categorize categorize = new Categorize(EMPTY, NULL);
        assertEquals(categorize, foldNull.rule(categorize));
    }

    private void assertNullLiteral(Expression expression) {
        assertEquals(Literal.class, expression.getClass());
        assertNull(expression.fold());
    }

}
