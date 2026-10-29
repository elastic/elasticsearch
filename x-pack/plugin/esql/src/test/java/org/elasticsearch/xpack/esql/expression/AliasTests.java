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
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToAggregateMetricDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToBoolean;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCounter;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDateNanos;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDegrees;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToExponentialHistogram;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGauge;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLongSurrogate;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToRadians;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToTDigest;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToUnsignedLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
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

    public void testMetricIdentityThroughCastChainsAndAliases() throws IOException {
        FieldAttribute counter = metricField(DataType.COUNTER_LONG);
        assertTrue(MetricExpression.isMetric(new ToLongSurrogate(Source.EMPTY, counter, null)));
        Alias numeric = new Alias(Source.EMPTY, "numeric", new ToLong(Source.EMPTY, counter));
        Alias copy = copyInstance(numeric);
        assertTrue(copy.toAttribute().isMetric());
        assertEquals(DataType.LONG, copy.dataType());
        Alias renamed = new Alias(Source.EMPTY, "renamed", copy.toAttribute());
        Alias converted = new Alias(Source.EMPTY, "converted", new ToDouble(Source.EMPTY, renamed.toAttribute()));
        assertTrue(converted.toAttribute().isMetric());
        assertEquals(DataType.DOUBLE, converted.dataType());
    }

    public void testExplicitCastsDeclareNonMetricInputs() throws IOException {
        Expression ordinary = new FieldAttribute(
            Source.EMPTY,
            "value",
            new EsField("value", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Expression computed = new Coalesce(Source.EMPTY, metricField(DataType.LONG), List.of(new Literal(Source.EMPTY, 0L, DataType.LONG)));
        for (Expression input : List.of(ordinary, computed)) {
            assertFalse(MetricExpression.isMetric(new ToDouble(Source.EMPTY, input)));
            for (Expression declaration : List.of(new ToGauge(Source.EMPTY, input), new ToCounter(Source.EMPTY, input))) {
                Alias copy = copyInstance(new Alias(Source.EMPTY, "declared", declaration));
                assertTrue(copy.toAttribute().isMetric());
                assertEquals(declaration instanceof ToGauge ? DataType.LONG : DataType.COUNTER_LONG, copy.dataType());
                assertTrue(MetricExpression.isMetric(new ToDouble(Source.EMPTY, copy.toAttribute())));
            }
        }
    }

    public void testNumericConversionsPreserveMetricIdentity() {
        Expression metric = metricField(DataType.LONG);
        for (Expression conversion : List.of(
            new ToDouble(Source.EMPTY, metric),
            new ToInteger(Source.EMPTY, metric),
            new ToLong(Source.EMPTY, metric),
            new ToUnsignedLong(Source.EMPTY, metric),
            new ToAggregateMetricDouble(Source.EMPTY, metric)
        )) {
            assertTrue(conversion.toString(), new Alias(Source.EMPTY, "converted", conversion).toAttribute().isMetric());
        }
    }

    public void testHistogramConversionsPreserveMetricIdentity() throws IOException {
        Expression histogram = metricField(DataType.HISTOGRAM);
        Alias tdigest = new Alias(Source.EMPTY, "tdigest", new ToTDigest(Source.EMPTY, histogram));
        Alias exponential = new Alias(Source.EMPTY, "exponential", new ToExponentialHistogram(Source.EMPTY, tdigest.toAttribute()));
        assertTrue(tdigest.toAttribute().isMetric());
        assertTrue(exponential.toAttribute().isMetric());
        assertTrue(copyInstance(exponential).toAttribute().isMetric());
        FieldAttribute ordinaryHistogram = new FieldAttribute(
            Source.EMPTY,
            "value",
            new EsField("value", DataType.HISTOGRAM, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        assertFalse(MetricExpression.isMetric(new ToTDigest(Source.EMPTY, ordinaryHistogram)));
        assertFalse(MetricExpression.isMetric(new ToExponentialHistogram(Source.EMPTY, ordinaryHistogram)));
    }

    public void testOtherConversionsAndAngleTransformationsDropMetricIdentity() {
        Expression metric = metricField(DataType.LONG);
        for (Expression conversion : List.of(
            new ToString(Source.EMPTY, metric, TEST_CFG),
            new ToBoolean(Source.EMPTY, metric),
            new ToDatetime(Source.EMPTY, metric, TEST_CFG),
            new ToDateNanos(Source.EMPTY, metric, TEST_CFG),
            new ToDegrees(Source.EMPTY, metric),
            new ToRadians(Source.EMPTY, metric)
        )) {
            Alias alias = new Alias(Source.EMPTY, "converted", conversion);
            assertFalse(conversion.toString(), alias.toAttribute().isMetric());
        }
        Expression string = new ToString(Source.EMPTY, metric, TEST_CFG);
        assertFalse(MetricExpression.isMetric(new ToDouble(Source.EMPTY, string)));
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
