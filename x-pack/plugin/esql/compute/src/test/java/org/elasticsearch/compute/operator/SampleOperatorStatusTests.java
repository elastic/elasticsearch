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

public class SampleOperatorStatusTests extends AbstractWireSerializingTestCase<SampleOperator.Status> {
    public static SampleOperator.Status simple() {
        return new SampleOperator.Status(500012, 200012, 123, 111, 222);
    }

    public static String simpleToJson() {
        return """
            {
              "collect_nanos" : 500012,
              "collect_time" : "500micros",
              "emit_nanos" : 200012,
              "emit_time" : "200micros",
              "pages_processed" : 123,
              "rows_received" : 111,
              "rows_emitted" : 222
            }""";
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple(), true, true), equalTo(simpleToJson()));
    }

    @Override
    protected Writeable.Reader<SampleOperator.Status> instanceReader() {
        return SampleOperator.Status::new;
    }

    @Override
    public SampleOperator.Status createTestInstance() {
        return new SampleOperator.Status(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected SampleOperator.Status mutateInstance(SampleOperator.Status instance) {
        long collectNanos = instance.collectNanos();
        long emitNanos = instance.emitNanos();
        int pagesProcessed = instance.pagesProcessed();
        long rowsReceived = instance.rowsReceived();
        long rowsEmitted = instance.rowsEmitted();
        switch (between(0, 4)) {
            case 0 -> collectNanos = randomValueOtherThan(collectNanos, ESTestCase::randomNonNegativeLong);
            case 1 -> emitNanos = randomValueOtherThan(emitNanos, ESTestCase::randomNonNegativeLong);
            case 2 -> pagesProcessed = randomValueOtherThan(pagesProcessed, ESTestCase::randomNonNegativeInt);
            case 3 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            case 4 -> rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
            default -> throw new UnsupportedOperationException();
        }
        return new SampleOperator.Status(collectNanos, emitNanos, pagesProcessed, rowsReceived, rowsEmitted);
    }
}
