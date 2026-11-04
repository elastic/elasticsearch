/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType;

import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsFieldTestUtils.randomEsField;
import static org.elasticsearch.xpack.esql.type.EsFieldTestUtils.randomProperties;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class EsFieldTests extends AbstractEsFieldTypeTests<EsField> {
    @Override
    protected EsField createTestInstance() {
        return randomEsField(4);
    }

    public void testTimeSeriesFieldTypeMergeMetricWithDimensionThrows() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TimeSeriesFieldType.METRIC.merge(TimeSeriesFieldType.DIMENSION)
        );
        assertThat(e.getMessage(), equalTo("Time Series Metadata conflict.  Cannot merge [DIMENSION] with [METRIC]."));
    }

    public void testTimeSeriesFieldTypeMergeDimensionWithMetricThrows() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TimeSeriesFieldType.DIMENSION.merge(TimeSeriesFieldType.METRIC)
        );
        assertThat(e.getMessage(), equalTo("Time Series Metadata conflict.  Cannot merge [METRIC] with [DIMENSION]."));
    }

    public void testTimeSeriesFieldTypeMergeMetricWithNonDimension() {
        for (TimeSeriesFieldType other : new TimeSeriesFieldType[] {
            TimeSeriesFieldType.UNKNOWN,
            TimeSeriesFieldType.NONE,
            TimeSeriesFieldType.METRIC }) {
            assertThat(TimeSeriesFieldType.METRIC.merge(other), equalTo(TimeSeriesFieldType.METRIC));
        }
    }

    public void testTimeSeriesFieldTypeMergeDimensionWithNonMetric() {
        for (TimeSeriesFieldType other : new TimeSeriesFieldType[] {
            TimeSeriesFieldType.UNKNOWN,
            TimeSeriesFieldType.NONE,
            TimeSeriesFieldType.DIMENSION }) {
            assertThat(TimeSeriesFieldType.DIMENSION.merge(other), equalTo(TimeSeriesFieldType.DIMENSION));
        }
    }

    public void testTimeSeriesFieldTypeMergeUnknown() {
        for (TimeSeriesFieldType other : TimeSeriesFieldType.values()) {
            assertThat(TimeSeriesFieldType.UNKNOWN.merge(other), equalTo(other));
        }
    }

    public void testTimeSeriesFieldTypeMergeNone() {
        for (TimeSeriesFieldType other : TimeSeriesFieldType.values()) {
            assertThat(TimeSeriesFieldType.NONE.merge(other), equalTo(other));
        }
    }

    public void testPassthroughSerialization() throws Exception {
        EsField field = new EsField("attributes", DataType.OBJECT, Map.of(), false, false, TimeSeriesFieldType.NONE, true);
        assertThat(copyInstance(field, EsField.ESQL_SERIALIZE_PASSTHROUGH_FIELD).isPassthrough(), is(true));
    }

    public void testNonNullableSerialization() throws Exception {
        EsField field = new EsField("age", DataType.LONG, Map.of(), true, false, TimeSeriesFieldType.NONE, false, true);
        assertThat(copyInstance(field, EsField.ESQL_SERIALIZE_NON_NULLABLE_FIELD).isStaticallyNonNullable(), is(true));
        // A node that cannot read the flag sees "unknown".
        assertThat(
            copyInstance(field, TransportVersionUtils.getPreviousVersion(EsField.ESQL_SERIALIZE_NON_NULLABLE_FIELD))
                .isStaticallyNonNullable(),
            is(false)
        );
    }

    /** Widening a small number type, as ES|QL does for short and byte fields, must not drop the guarantee. */
    public void testWidenedSmallNumericKeepsNonNullable() {
        EsField field = new EsField("age", DataType.SHORT, Map.of(), true, false, TimeSeriesFieldType.NONE, false, true);
        EsField widened = field.withWidenedSmallNumeric();
        assertThat(widened.getDataType(), equalTo(DataType.INTEGER));
        assertThat(widened.isStaticallyNonNullable(), is(true));
        assertThat(field.withProperties(Map.of()).isStaticallyNonNullable(), is(true));
    }

    @Override
    protected EsField mutateInstance(EsField instance) {
        String name = instance.getName();
        DataType esDataType = instance.getDataType();
        Map<String, EsField> properties = instance.getProperties();
        boolean aggregatable = instance.isAggregatable();
        boolean isAlias = instance.isAlias();
        EsField.TimeSeriesFieldType tsType = instance.getTimeSeriesFieldType();
        boolean isPassthrough = instance.isPassthrough();
        boolean isStaticallyNonNullable = instance.isStaticallyNonNullable();
        switch (between(0, 7)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> esDataType = randomValueOtherThan(esDataType, () -> randomFrom(DataType.types()));
            case 2 -> properties = randomValueOtherThan(properties, () -> randomProperties(4));
            case 3 -> aggregatable = false == aggregatable;
            case 4 -> isAlias = false == isAlias;
            case 5 -> tsType = randomValueOtherThan(tsType, () -> randomFrom(EsField.TimeSeriesFieldType.values()));
            case 6 -> isPassthrough = false == isPassthrough;
            case 7 -> isStaticallyNonNullable = false == isStaticallyNonNullable;
            default -> throw new IllegalArgumentException();
        }
        return new EsField(name, esDataType, properties, aggregatable, isAlias, tsType, isPassthrough, isStaticallyNonNullable);
    }
}
