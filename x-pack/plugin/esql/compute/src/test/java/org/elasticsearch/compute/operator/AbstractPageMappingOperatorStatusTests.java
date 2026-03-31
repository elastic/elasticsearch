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

public class AbstractPageMappingOperatorStatusTests extends AbstractWireSerializingTestCase<AbstractPageMappingOperator.Status> {
    public static AbstractPageMappingOperator.Status simple() {
        return new AbstractPageMappingOperator.Status(200012, 123, 111, 222);
    }

    public static String simpleToJson() {
        return """
            {
              "process_nanos" : 200012,
              "process_time" : "200micros",
              "pages_processed" : 123,
              "rows_received" : 111,
              "rows_emitted" : 222
            }""";
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple(), true, true), equalTo(simpleToJson()));
    }

    @Override
    protected Writeable.Reader<AbstractPageMappingOperator.Status> instanceReader() {
        return AbstractPageMappingOperator.Status::new;
    }

    @Override
    public AbstractPageMappingOperator.Status createTestInstance() {
        return new AbstractPageMappingOperator.Status(
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected AbstractPageMappingOperator.Status mutateInstance(AbstractPageMappingOperator.Status instance) {
        long processNanos = instance.processNanos();
        int pagesProcessed = instance.pagesProcessed();
        long rowsReceived = instance.rowsReceived();
        long rowsEmitted = instance.rowsEmitted();
        switch (between(0, 3)) {
            case 0 -> processNanos = randomValueOtherThan(processNanos, ESTestCase::randomNonNegativeLong);
            case 1 -> pagesProcessed = randomValueOtherThan(pagesProcessed, ESTestCase::randomNonNegativeInt);
            case 2 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            case 3 -> rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
            default -> throw new UnsupportedOperationException();
        }
        return new AbstractPageMappingOperator.Status(processNanos, pagesProcessed, rowsReceived, rowsEmitted);
    }
}
