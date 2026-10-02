/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.List;

public class TimeSeriesUnsetSerializationTests extends AbstractExpressionSerializationTests<TimeSeriesUnset> {
    @Override
    protected TimeSeriesUnset createTestInstance() {
        return new TimeSeriesUnset(randomSource(), randomChild(), randomList(0, 5, AbstractExpressionSerializationTests::randomChild));
    }

    @Override
    protected TimeSeriesUnset mutateInstance(TimeSeriesUnset instance) {
        Expression timeseries = instance.timeseries();
        List<Expression> dimensions = instance.dimensions();
        if (randomBoolean()) {
            timeseries = randomValueOtherThan(timeseries, AbstractExpressionSerializationTests::randomChild);
        } else {
            dimensions = randomValueOtherThan(dimensions, () -> randomList(0, 5, AbstractExpressionSerializationTests::randomChild));
        }
        return new TimeSeriesUnset(instance.source(), timeseries, dimensions);
    }

    /**
     * Nodes before {@link TimeSeriesUnset#ESQL_TIMESERIES_METADATA_UNSET} can't run it, including nodes that already load the
     * {@code _timeseries} exclusions: such a recipient is refused rather than sent an unknown writeable.
     */
    public void testRefusesRecipientsBeforeTimeSeriesMetadataUnset() throws IOException {
        TimeSeriesUnset instance = createTestInstance();
        assertTrue(instance.supportsVersion(TimeSeriesUnset.ESQL_TIMESERIES_METADATA_UNSET));
        for (TransportVersion old : List.of(
            FieldAttribute.ESQL_TIMESERIES_METADATA_ATTRIBUTE,
            TransportVersionUtils.getPreviousVersion(TimeSeriesUnset.ESQL_TIMESERIES_METADATA_UNSET)
        )) {
            assertFalse(instance.supportsVersion(old));
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setTransportVersion(old);
                expectThrows(IOException.class, () -> instance.writeTo(out));
            }
        }
    }
}
