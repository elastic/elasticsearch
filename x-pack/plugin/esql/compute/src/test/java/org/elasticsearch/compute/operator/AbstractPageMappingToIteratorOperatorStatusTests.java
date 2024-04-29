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

public class AbstractPageMappingToIteratorOperatorStatusTests extends AbstractWireSerializingTestCase<
    AbstractPageMappingToIteratorOperator.Status> {
    public static AbstractPageMappingToIteratorOperator.Status simple() {
        return new AbstractPageMappingToIteratorOperator.Status(200012, 123, 204);
    }

    public static String simpleToJson() {
        return """
            {
              "process_nanos" : 200012,
              "process_time" : "200micros",
              "pages_received" : 123,
              "pages_emitted" : 204
            }""";
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple(), true, true), equalTo(simpleToJson()));
    }

    @Override
    protected Writeable.Reader<AbstractPageMappingToIteratorOperator.Status> instanceReader() {
        return AbstractPageMappingToIteratorOperator.Status::new;
    }

    @Override
    public AbstractPageMappingToIteratorOperator.Status createTestInstance() {
        return new AbstractPageMappingToIteratorOperator.Status(randomNonNegativeLong(), randomNonNegativeInt(), randomNonNegativeInt());
    }

    @Override
    protected AbstractPageMappingToIteratorOperator.Status mutateInstance(AbstractPageMappingToIteratorOperator.Status instance) {
        long processNanos = instance.processNanos();
        int pagesReceived = instance.pagesReceived();
        int pagesEmitted = instance.pagesEmitted();
        switch (between(0, 2)) {
            case 0 -> processNanos = randomValueOtherThan(processNanos, ESTestCase::randomNonNegativeLong);
            case 1 -> pagesReceived = randomValueOtherThan(pagesReceived, ESTestCase::randomNonNegativeInt);
            case 2 -> pagesEmitted = randomValueOtherThan(pagesEmitted, ESTestCase::randomNonNegativeInt);
            default -> throw new UnsupportedOperationException();
        }
        return new AbstractPageMappingToIteratorOperator.Status(processNanos, pagesReceived, pagesEmitted);
    }
}
