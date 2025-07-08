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

public class OrdinalsGroupingOperatorStatusTests extends AbstractWireSerializingTestCase<OrdinalsGroupingOperator.Status> {
    public static OrdinalsGroupingOperator.Status simple() {
        return new OrdinalsGroupingOperator.Status(200012, 100010, 100011, 600012, 300010, 300011, 123, 111, 222);
    }

    public static String simpleToJson() {
        return """
            {
              "total_process_nanos" : 200012,
              "total_process_time" : "200micros",
              "ordinals_process_nanos" : 100010,
              "ordinals_process_time" : "100micros",
              "values_process_nanos" : 100011,
              "values_process_time" : "100micros",
              "total_emit_nanos" : 600012,
              "total_emit_time" : "600micros",
              "ordinals_emit_nanos" : 300010,
              "ordinals_emit_time" : "300micros",
              "values_emit_nanos" : 300011,
              "values_emit_time" : "300micros",
              "pages_processed" : 123,
              "rows_received" : 111,
              "rows_emitted" : 222
            }""";
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple(), true, true), equalTo(simpleToJson()));
    }

    @Override
    protected Writeable.Reader<OrdinalsGroupingOperator.Status> instanceReader() {
        return OrdinalsGroupingOperator.Status::new;
    }

    @Override
    public OrdinalsGroupingOperator.Status createTestInstance() {
        return new OrdinalsGroupingOperator.Status(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected OrdinalsGroupingOperator.Status mutateInstance(OrdinalsGroupingOperator.Status instance) {
        long totalProcessNanos = instance.totalProcessNanos();
        long ordinalsProcessNanos = instance.ordinalsProcessNanos();
        long valuesProcessNanos = instance.valuesProcessNanos();
        long totalEmitNanos = instance.totalEmitNanos();
        long ordinalsEmitNanos = instance.ordinalsEmitNanos();
        long valuesEmitNanos = instance.valuesEmitNanos();
        int pagesProcessed = instance.pagesProcessed();
        long rowsReceived = instance.rowsReceived();
        long rowsEmitted = instance.rowsEmitted();
        switch (between(0, 8)) {
            case 0 -> totalProcessNanos = randomValueOtherThan(totalProcessNanos, ESTestCase::randomNonNegativeLong);
            case 1 -> ordinalsProcessNanos = randomValueOtherThan(ordinalsProcessNanos, ESTestCase::randomNonNegativeLong);
            case 2 -> valuesProcessNanos = randomValueOtherThan(valuesProcessNanos, ESTestCase::randomNonNegativeLong);
            case 3 -> totalEmitNanos = randomValueOtherThan(totalEmitNanos, ESTestCase::randomNonNegativeLong);
            case 4 -> ordinalsEmitNanos = randomValueOtherThan(ordinalsEmitNanos, ESTestCase::randomNonNegativeLong);
            case 5 -> valuesEmitNanos = randomValueOtherThan(valuesEmitNanos, ESTestCase::randomNonNegativeLong);
            case 6 -> pagesProcessed = randomValueOtherThan(pagesProcessed, ESTestCase::randomNonNegativeInt);
            case 7 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            case 8 -> rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
            default -> throw new UnsupportedOperationException();
        }
        return new OrdinalsGroupingOperator.Status(
            totalProcessNanos,
            ordinalsProcessNanos,
            valuesProcessNanos,
            totalEmitNanos,
            ordinalsEmitNanos,
            valuesEmitNanos,
            pagesProcessed,
            rowsReceived,
            rowsEmitted
        );
    }
}
