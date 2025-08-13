/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ExchangeSinkOperatorStatusTests extends AbstractWireSerializingTestCase<ExchangeSinkOperator.Status> {
    public void testToXContent() {
        assertThat(Strings.toString(simple(), true, true), equalTo(simpleToJson()));
    }

    public static ExchangeSinkOperator.Status simple() {
        return new ExchangeSinkOperator.Status(10, 111);
    }

    public static String simpleToJson() {
        return """
            {
              "pages_received" : 10,
              "rows_received" : 111
            }""";
    }

    @Override
    protected Writeable.Reader<ExchangeSinkOperator.Status> instanceReader() {
        return ExchangeSinkOperator.Status::new;
    }

    @Override
    public ExchangeSinkOperator.Status createTestInstance() {
        return new ExchangeSinkOperator.Status(randomNonNegativeInt(), randomNonNegativeLong());
    }

    @Override
    protected ExchangeSinkOperator.Status mutateInstance(ExchangeSinkOperator.Status instance) throws IOException {
        int pagesReceived = instance.pagesReceived();
        long rowsReceived = instance.rowsReceived();
        switch (between(0, 1)) {
            case 0 -> pagesReceived = randomValueOtherThan(pagesReceived, ESTestCase::randomNonNegativeInt);
            case 1 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            default -> throw new UnsupportedOperationException();
        }
        return new ExchangeSinkOperator.Status(pagesReceived, rowsReceived);
    }
}
