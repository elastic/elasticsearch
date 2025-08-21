/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class AsyncOperatorStatusTests extends AbstractWireSerializingTestCase<AsyncOperator.Status> {
    @Override
    protected Writeable.Reader<AsyncOperator.Status> instanceReader() {
        return AsyncOperator.Status::new;
    }

    @Override
    protected AsyncOperator.Status createTestInstance() {
        return new AsyncOperator.Status(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomLongBetween(1, TimeValue.timeValueHours(1).millis())
        );
    }

    @Override
    protected AsyncOperator.Status mutateInstance(AsyncOperator.Status in) throws IOException {
        int field = randomIntBetween(0, 2);
        return switch (field) {
            case 0 -> new AsyncOperator.Status(
                randomValueOtherThan(in.receivedPages(), ESTestCase::randomNonNegativeLong),
                in.completedPages(),
                in.procesNanos()
            );
            case 1 -> new AsyncOperator.Status(
                in.receivedPages(),
                randomValueOtherThan(in.completedPages(), ESTestCase::randomNonNegativeLong),
                in.procesNanos()
            );
            case 2 -> new AsyncOperator.Status(
                in.receivedPages(),
                in.completedPages(),
                randomValueOtherThan(in.procesNanos(), ESTestCase::randomNonNegativeLong)
            );
            default -> throw new AssertionError("unknown ");
        };
    }

    public void testToXContent() {
        var status = new AsyncOperator.Status(100, 50, TimeValue.timeValueNanos(10).nanos());
        String json = Strings.toString(status, true, true);
        assertThat(json, equalTo("""
            {
              "process_nanos" : 10,
              "process_time" : "10nanos",
              "received_pages" : 100,
              "completed_pages" : 50
            }"""));
    }
}
