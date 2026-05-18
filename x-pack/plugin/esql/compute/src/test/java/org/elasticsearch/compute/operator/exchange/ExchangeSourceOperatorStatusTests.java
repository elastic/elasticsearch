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

public class ExchangeSourceOperatorStatusTests extends AbstractWireSerializingTestCase<ExchangeSourceOperator.Status> {
    public void testToXContent() {
        assertThat(Strings.toString(new ExchangeSourceOperator.Status(0, 10, 111)), equalTo("""
            {"pages_waiting":0,"pages_emitted":10,"rows_emitted":111}"""));
    }

    @Override
    protected Writeable.Reader<ExchangeSourceOperator.Status> instanceReader() {
        return ExchangeSourceOperator.Status::new;
    }

    @Override
    protected ExchangeSourceOperator.Status createTestInstance() {
        return new ExchangeSourceOperator.Status(randomNonNegativeInt(), randomNonNegativeInt(), randomNonNegativeLong());
    }

    @Override
    protected ExchangeSourceOperator.Status mutateInstance(ExchangeSourceOperator.Status instance) throws IOException {
        int pagesWaiting = instance.pagesWaiting();
        int pagesEmitted = instance.pagesEmitted();
        long rowsEmitted = instance.rowsEmitted();
        switch (between(0, 2)) {
            case 0 -> pagesWaiting = randomValueOtherThan(pagesWaiting, ESTestCase::randomNonNegativeInt);
            case 1 -> pagesEmitted = randomValueOtherThan(pagesEmitted, ESTestCase::randomNonNegativeInt);
            case 2 -> rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
            default -> throw new UnsupportedOperationException();
        }
        return new ExchangeSourceOperator.Status(pagesWaiting, pagesEmitted, rowsEmitted);
    }
}
