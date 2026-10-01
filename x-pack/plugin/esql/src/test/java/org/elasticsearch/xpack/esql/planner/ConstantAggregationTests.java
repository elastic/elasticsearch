/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Present;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SummationMode;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ConstantAggregationTests extends ESTestCase {

    public void testIdempotent() {
        var probe = probe(new Max(Source.EMPTY, referenceAttribute("x", DataType.LONG)), 5L);
        assertThat(probe.idempotent(), is(true));
        assertThat(probe.inputIgnored(), is(false));
        assertThat(probe.single(), is(5L));
        assertThat(probe.empty(), nullValue());
    }

    public void testNotIdempotent() {
        var probe = probe(new Sum(Source.EMPTY, referenceAttribute("x", DataType.LONG)), 5L);
        assertThat(probe.idempotent(), is(false));
        assertThat(probe.inputIgnored(), is(false));
        assertThat(probe.single(), is(5L));
        assertThat(probe.empty(), nullValue());
    }

    public void testRowCountingStateNotIdempotent() {
        var percentile = new Percentile(
            Source.EMPTY,
            referenceAttribute("x", DataType.DOUBLE),
            Literal.TRUE,
            AggregateFunction.NO_WINDOW,
            Literal.fromDouble(Source.EMPTY, 50.0)
        );
        var probe = probe(percentile, 1.0);
        assertThat(probe.idempotent(), is(false));
        assertThat(probe.single(), is(1.0));
    }

    public void testNonNullEmptyResult() {
        var probe = probe(new CountDistinct(Source.EMPTY, referenceAttribute("x", DataType.KEYWORD), null), new BytesRef("a"));
        assertThat(probe.idempotent(), is(true));
        assertThat(probe.single(), is(1L));
        assertThat(probe.empty(), is(0L));
    }

    public void testMultivaluedInput() {
        var probe = probe(
            new Values(Source.EMPTY, referenceAttribute("x", DataType.KEYWORD)),
            List.of(new BytesRef("a"), new BytesRef("b"))
        );
        assertThat(probe.idempotent(), is(true));
        assertThat((List<?>) probe.single(), containsInAnyOrder(new BytesRef("a"), new BytesRef("b")));
    }

    public void testNullInputIgnored() {
        var probe = probe(new Present(Source.EMPTY, referenceAttribute("x", DataType.INTEGER)), (Object) null);
        assertThat(probe.inputIgnored(), is(true));
        assertThat(probe.single(), is(false));
        assertThat(probe.empty(), is(false));
    }

    public void testEmpty() {
        var present = new Present(Source.EMPTY, referenceAttribute("x", DataType.INTEGER));
        assertThat(ConstantAggregation.empty(present, 1, Source.EMPTY, FoldContext.small()), is(false));
    }

    public void testWarningPreventsFolding() {
        var sum = new Sum(
            Source.EMPTY,
            referenceAttribute("x", DataType.LONG),
            Literal.TRUE,
            AggregateFunction.NO_WINDOW,
            SummationMode.COMPENSATED_LITERAL,
            Sum.LONG_OVERFLOW_WARN
        );
        assertThat(ConstantAggregation.probe(sum, List.of(Long.MAX_VALUE), Source.EMPTY, FoldContext.small()), nullValue());
    }

    private static ConstantAggregation.Probe probe(ToAggregator aggregation, Object... inputs) {
        var probe = ConstantAggregation.probe(aggregation, Arrays.asList(inputs), Source.EMPTY, FoldContext.small());
        assertNotNull(probe);
        return probe;
    }
}
