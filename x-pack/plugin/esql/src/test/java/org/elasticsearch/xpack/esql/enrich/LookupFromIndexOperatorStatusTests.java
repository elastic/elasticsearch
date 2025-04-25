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

public class LookupFromIndexOperatorStatusTests extends AbstractWireSerializingTestCase<LookupFromIndexOperator.Status> {
    @Override
    protected Writeable.Reader<LookupFromIndexOperator.Status> instanceReader() {
        return LookupFromIndexOperator.Status::new;
    }

    @Override
    protected LookupFromIndexOperator.Status createTestInstance() {
        return new LookupFromIndexOperator.Status(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomLongBetween(0, TimeValue.timeValueHours(1).millis()),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected LookupFromIndexOperator.Status mutateInstance(LookupFromIndexOperator.Status in) throws IOException {
        long receivedPages = in.receivedPages();
        long completedPages = in.completedPages();
        long procesNanos = in.procesNanos();
        long totalTerms = in.totalTerms();
        long emittedPages = in.emittedPages();
        switch (randomIntBetween(0, 4)) {
            case 0 -> receivedPages = randomValueOtherThan(receivedPages, ESTestCase::randomNonNegativeLong);
            case 1 -> completedPages = randomValueOtherThan(completedPages, ESTestCase::randomNonNegativeLong);
            case 2 -> procesNanos = randomValueOtherThan(procesNanos, ESTestCase::randomNonNegativeLong);
            case 3 -> totalTerms = randomValueOtherThan(totalTerms, ESTestCase::randomNonNegativeLong);
            case 4 -> emittedPages = randomValueOtherThan(emittedPages, ESTestCase::randomNonNegativeLong);
            default -> throw new UnsupportedOperationException();
        }
        return new LookupFromIndexOperator.Status(receivedPages, completedPages, procesNanos, totalTerms, emittedPages);
    }

    public void testToXContent() {
        var status = new LookupFromIndexOperator.Status(100, 50, TimeValue.timeValueSeconds(10).millis(), 120, 88);
        String json = Strings.toString(status, true, true);
        assertThat(json, equalTo("""
            {
              "process_nanos" : 10000,
              "process_time" : "10micros",
              "received_pages" : 100,
              "completed_pages" : 50,
              "emitted_pages" : 88,
              "total_terms" : 120
            }"""));
    }
}
