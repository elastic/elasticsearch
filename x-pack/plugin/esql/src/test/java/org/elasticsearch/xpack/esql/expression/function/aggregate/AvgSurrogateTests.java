/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.compute.aggregation.LossySumDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumLongAsDoubleAggregatorFunctionSupplier;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The sum that {@code AVG} of a {@code long} is replaced with depends on what the nodes that run the query support,
 * see {@link Avg#surrogate(TransportVersion)}.
 */
public class AvgSurrogateTests extends ESTestCase {
    public void testLongIsSummedAsDoubleWhenAllNodesSupportIt() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        TransportVersion version = TransportVersionUtils.randomVersionSupporting(Sum.ESQL_SUM_LONG_AS_DOUBLE);
        Sum sum = sumOf(new Avg(EMPTY, field).surrogate(version));
        assertThat(sum.field(), sameInstance(field));
        assertThat(sum.longOverflowMode(), is(Sum.LONG_OVERFLOW_DOUBLE));
        assertThat(sum.sumsLongAsDouble(), is(true));
        assertThat(sum.dataType(), is(DataType.DOUBLE));
        assertThat(sum.supplier(), instanceOf(SumLongAsDoubleAggregatorFunctionSupplier.class));
        // The mode is not one that depends on the version any more.
        assertThat(sum.forTransportVersion(version), nullValue());
    }

    public void testLongIsCastToDoubleWhenANodeDoesNotSupportTheSum() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        TransportVersion version = TransportVersionUtils.randomVersionNotSupporting(Sum.ESQL_SUM_LONG_AS_DOUBLE);
        assertCastsToDouble(field, sumOf(new Avg(EMPTY, field).surrogate(version)));
    }

    public void testLongIsCastToDoubleWithoutAVersion() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        assertCastsToDouble(field, sumOf(new Avg(EMPTY, field).surrogate()));
    }

    /**
     * The sum of longs as a double is the compensated one, so the lossy sum keeps the cast.
     */
    public void testLossySumOfLongIsCastToDouble() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        TransportVersion version = TransportVersionUtils.randomVersionSupporting(Sum.ESQL_SUM_LONG_AS_DOUBLE);
        Avg avg = new Avg(EMPTY, field, Literal.TRUE, AggregateFunction.NO_WINDOW, SummationMode.LOSSY_LITERAL);
        Sum sum = sumOf(avg.surrogate(version));
        assertThat(sum.field(), instanceOf(ToDouble.class));
        assertThat(sum.sumsLongAsDouble(), is(false));
        assertThat(sum.dataType(), is(DataType.DOUBLE));
        assertThat(sum.supplier(), instanceOf(LossySumDoubleAggregatorFunctionSupplier.class));
    }

    public void testIntIsSummedAsLong() {
        Expression field = getFieldAttribute("f", DataType.INTEGER);
        TransportVersion version = TransportVersionUtils.randomVersionSupporting(Sum.ESQL_SUM_LONG_AS_DOUBLE);
        Sum sum = sumOf(new Avg(EMPTY, field).surrogate(version));
        assertThat(sum.field(), sameInstance(field));
        assertThat(sum.sumsLongAsDouble(), is(false));
        assertThat(sum.dataType(), is(DataType.LONG));
    }

    /**
     * The optimizer merges aggregates that are equal, so the sum that reads the longs itself must not be equal to the ones
     * that don't, or an explicit {@code SUM} of the same field would end up with the wrong kind of sum, or the other way round.
     */
    public void testTheSumOfLongsAsDoublesIsNotEqualToThePlainSums() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        Sum plain = new Sum(EMPTY, field);
        Sum asDouble = sumOf(new Avg(EMPTY, field).surrogate(TransportVersionUtils.randomVersionSupporting(Sum.ESQL_SUM_LONG_AS_DOUBLE)));
        assertThat(asDouble, not(equalTo(plain)));
        assertThat(
            asDouble,
            not(equalTo(new Sum(EMPTY, field, Literal.TRUE, Literal.NULL, plain.summationMode(), Sum.LONG_OVERFLOW_WARN)))
        );
        assertThat(asDouble.hashCode() == plain.hashCode(), is(false));
        assertThat(plain.dataType(), is(DataType.LONG));
        assertThat(asDouble.dataType(), is(DataType.DOUBLE));
    }

    private static void assertCastsToDouble(Expression field, Sum sum) {
        assertThat(sum.field(), instanceOf(ToDouble.class));
        assertThat(((ToDouble) sum.field()).field(), sameInstance(field));
        assertThat(sum.sumsLongAsDouble(), is(false));
        assertThat(sum.dataType(), is(DataType.DOUBLE));
        assertThat(sum.supplier(), instanceOf(SumDoubleAggregatorFunctionSupplier.class));
    }

    private static Sum sumOf(Expression surrogate) {
        assertThat(surrogate, instanceOf(Div.class));
        Div div = (Div) surrogate;
        assertThat(div.left(), instanceOf(Sum.class));
        assertThat(div.right(), instanceOf(Count.class));
        return (Sum) div.left();
    }
}
