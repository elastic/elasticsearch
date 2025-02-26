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

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class LimitStatusTests extends AbstractWireSerializingTestCase<LimitOperator.Status> {
    public void testToXContent() {
        assertThat(Strings.toString(new LimitOperator.Status(10, 1, 1, 111, 222)), equalTo("""
            {"limit":10,"limit_remaining":1,"pages_processed":1,"rows_received":111,"rows_emitted":222}"""));
    }

    @Override
    protected Writeable.Reader<LimitOperator.Status> instanceReader() {
        return LimitOperator.Status::new;
    }

    @Override
    protected LimitOperator.Status createTestInstance() {
        return new LimitOperator.Status(
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected LimitOperator.Status mutateInstance(LimitOperator.Status instance) throws IOException {
        int limit = instance.limit();
        int limitRemaining = instance.limitRemaining();
        int pagesProcessed = instance.pagesProcessed();
        long rowsReceived = instance.rowsReceived();
        long rowsEmitted = instance.rowsEmitted();
        switch (between(0, 4)) {
            case 0:
                limit = randomValueOtherThan(limit, ESTestCase::randomNonNegativeInt);
                break;
            case 1:
                limitRemaining = randomValueOtherThan(limitRemaining, ESTestCase::randomNonNegativeInt);
                break;
            case 2:
                pagesProcessed = randomValueOtherThan(pagesProcessed, ESTestCase::randomNonNegativeInt);
                break;
            case 3:
                rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
                break;
            case 4:
                rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
                break;
            default:
                throw new IllegalArgumentException();
        }
        return new LimitOperator.Status(limit, limitRemaining, pagesProcessed, rowsReceived, rowsEmitted);
    }
}
