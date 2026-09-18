/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class MetricsInfoOperatorStatusTests extends AbstractWireSerializingTestCase<MetricsInfoOperator.Status> {
    public static MetricsInfoOperator.Status simple() {
        return new MetricsInfoOperator.Status(MetricsInfoOperator.Mode.INITIAL, 5, 100, 12, 8);
    }

    public static String simpleToJson() {
        return """
            {
              "mode" : "INITIAL",
              "pages_received" : 5,
              "rows_received" : 100,
              "rows_emitted" : 12,
              "metrics_accumulated" : 8
            }""";
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple(), true, true), equalTo(simpleToJson()));
    }

    @Override
    protected Writeable.Reader<MetricsInfoOperator.Status> instanceReader() {
        return MetricsInfoOperator.Status::new;
    }

    @Override
    public MetricsInfoOperator.Status createTestInstance() {
        return new MetricsInfoOperator.Status(
            randomFrom(MetricsInfoOperator.Mode.values()),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeInt()
        );
    }

    @Override
    protected MetricsInfoOperator.Status mutateInstance(MetricsInfoOperator.Status instance) {
        MetricsInfoOperator.Mode mode = instance.mode();
        int pagesReceived = instance.pagesReceived();
        long rowsReceived = instance.rowsReceived();
        long rowsEmitted = instance.rowsEmitted();
        int metricsAccumulated = instance.metricsAccumulated();
        switch (between(0, 4)) {
            case 0 -> mode = randomValueOtherThan(mode, () -> randomFrom(MetricsInfoOperator.Mode.values()));
            case 1 -> pagesReceived = randomValueOtherThan(pagesReceived, ESTestCase::randomNonNegativeInt);
            case 2 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            case 3 -> rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
            case 4 -> metricsAccumulated = randomValueOtherThan(metricsAccumulated, ESTestCase::randomNonNegativeInt);
            default -> throw new UnsupportedOperationException();
        }
        return new MetricsInfoOperator.Status(mode, pagesReceived, rowsReceived, rowsEmitted, metricsAccumulated);
    }
}
