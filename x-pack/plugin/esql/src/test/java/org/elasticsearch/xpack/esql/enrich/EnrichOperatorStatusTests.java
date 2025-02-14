/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class EnrichOperatorStatusTests extends AbstractWireSerializingTestCase<EnrichLookupOperator.Status> {
    @Override
    protected Writeable.Reader<EnrichLookupOperator.Status> instanceReader() {
        return EnrichLookupOperator.Status::new;
    }

    @Override
    protected EnrichLookupOperator.Status createTestInstance() {
        return new EnrichLookupOperator.Status(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomLongBetween(1, TimeValue.timeValueHours(1).millis())
        );
    }

    @Override
    protected EnrichLookupOperator.Status mutateInstance(EnrichLookupOperator.Status in) throws IOException {
        int field = randomIntBetween(0, 3);
        return switch (field) {
            case 0 -> new EnrichLookupOperator.Status(
                randomValueOtherThan(in.receivedPages(), ESTestCase::randomNonNegativeLong),
                in.completedPages(),
                in.totalTerms,
                in.procesNanos()
            );
            case 1 -> new EnrichLookupOperator.Status(
                in.receivedPages(),
                randomValueOtherThan(in.completedPages(), ESTestCase::randomNonNegativeLong),
                in.totalTerms,
                in.procesNanos()
            );
            case 2 -> new EnrichLookupOperator.Status(
                in.receivedPages(),
                in.completedPages(),
                randomValueOtherThan(in.totalTerms, ESTestCase::randomNonNegativeLong),
                in.procesNanos()
            );
            case 3 -> new EnrichLookupOperator.Status(
                in.receivedPages(),
                in.completedPages(),
                in.totalTerms,
                randomValueOtherThan(in.procesNanos(), ESTestCase::randomNonNegativeLong)
            );
            default -> throw new AssertionError("unknown ");
        };
    }

    public void testToXContent() {
        var status = new EnrichLookupOperator.Status(100, 50, TimeValue.timeValueSeconds(10).millis(), 120);
        String json = Strings.toString(status, true, true);
        assertThat(json, equalTo("""
            {
              "process_nanos" : 10000,
              "process_time" : "10micros",
              "received_pages" : 100,
              "completed_pages" : 50,
              "total_terms" : 120
            }"""));
    }
}
