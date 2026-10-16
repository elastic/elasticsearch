/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetricExpression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCounter;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGauge;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLongSurrogate;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTestUtils.randomReferenceAttribute;

public class AliasTests extends AbstractNamedExpressionSerializationTests<Alias> {
    public static Alias randomAlias() {
        Source source = SourceTests.randomSource();
        String name = randomAlphaOfLength(5);
        // TODO better randomChild
        Expression child = randomReferenceAttribute(false);
        boolean synthetic = randomBoolean();
        return new Alias(source, name, child, new NameId(), synthetic);
    }

    public void testCounterCastRetainsMetricIdentity() {
        FieldAttribute counter = metricField(DataType.COUNTER_LONG);
        Expression cast = new ToLongSurrogate(Source.EMPTY, counter, null);
        assertTrue(MetricExpression.isMetric(cast));
        Alias alias = new Alias(Source.EMPTY, "numeric", cast);
        assertTrue(alias.isMetric());
        assertTrue(alias.toAttribute().isMetric());
        assertEquals(DataType.LONG, alias.dataType());
    }

    public void testMetricIdentityThroughCastChainsAndAliases() throws IOException {
        Alias numeric = new Alias(Source.EMPTY, "numeric", new ToLong(Source.EMPTY, metricField(DataType.COUNTER_LONG)));
        Alias copy = copyInstance(numeric);
        assertTrue(copy.toAttribute().isMetric());
        assertEquals(DataType.LONG, copy.dataType());
        Alias renamed = new Alias(Source.EMPTY, "renamed", copy.toAttribute());
        Alias converted = new Alias(Source.EMPTY, "converted", new ToDouble(Source.EMPTY, renamed.toAttribute()));
        assertTrue(converted.toAttribute().isMetric());
        assertEquals(DataType.DOUBLE, converted.dataType());
    }

    public void testGaugeAndCounterCastsRetainMetricIdentity() {
        Alias gauge = new Alias(Source.EMPTY, "gauge", new ToGauge(Source.EMPTY, metricField(DataType.COUNTER_LONG)));
        assertTrue(gauge.toAttribute().isMetric());
        assertEquals(DataType.LONG, gauge.dataType());
        Alias counter = new Alias(Source.EMPTY, "counter", new ToCounter(Source.EMPTY, gauge.toAttribute()));
        assertTrue(counter.toAttribute().isMetric());
        assertEquals(DataType.COUNTER_LONG, counter.dataType());
    }

    public void testCastOfNonMetricRemainsNonMetric() {
        FieldAttribute field = new FieldAttribute(
            Source.EMPTY,
            "value",
            new EsField("value", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Alias converted = new Alias(Source.EMPTY, "converted", new ToDouble(Source.EMPTY, field));
        assertFalse(converted.toAttribute().isMetric());
        Alias counter = new Alias(Source.EMPTY, "counter", new ToCounter(Source.EMPTY, field));
        assertFalse(counter.toAttribute().isMetric());
    }

    public void testCastOfComputedValueRemainsNonMetric() {
        Expression computed = new Coalesce(Source.EMPTY, metricField(DataType.LONG), List.of(new Literal(Source.EMPTY, 0L, DataType.LONG)));
        Alias alias = new Alias(Source.EMPTY, "computed", new ToDouble(Source.EMPTY, computed));
        assertFalse(alias.toAttribute().isMetric());
    }

    private static FieldAttribute metricField(DataType type) {
        return new FieldAttribute(Source.EMPTY, "metric", new EsField("metric", type, Map.of(), true, EsField.TimeSeriesFieldType.METRIC));
    }

    @Override
    protected Alias createTestInstance() {
        return randomAlias();
    }

    @Override
    protected Alias mutateInstance(Alias instance) throws IOException {
        Source source = instance.source();
        String name = instance.name();
        Expression child = instance.child();
        boolean synthetic = instance.synthetic();
        switch (between(0, 2)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> child = randomValueOtherThan(child, () -> randomReferenceAttribute(false));
            case 2 -> synthetic = false == synthetic;
        }
        return new Alias(source, name, child, instance.id(), synthetic);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    @Override
    protected Alias mutateNameId(Alias instance) {
        return instance.withId(new NameId());
    }

    @Override
    protected boolean equalityIgnoresId() {
        return false;
    }
}
