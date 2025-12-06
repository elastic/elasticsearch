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
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected LookupFromIndexOperator.Status mutateInstance(LookupFromIndexOperator.Status in) throws IOException {
        long receivedPages = in.receivedPages();
        long completedPages = in.completedPages();
        long procesNanos = in.processNanos();
        long totalRows = in.totalRows();
        long emittedPages = in.emittedPages();
        long emittedRows = in.emittedRows();
        switch (randomIntBetween(0, 5)) {
            case 0 -> receivedPages = randomValueOtherThan(receivedPages, ESTestCase::randomNonNegativeLong);
            case 1 -> completedPages = randomValueOtherThan(completedPages, ESTestCase::randomNonNegativeLong);
            case 2 -> procesNanos = randomValueOtherThan(procesNanos, ESTestCase::randomNonNegativeLong);
            case 3 -> totalRows = randomValueOtherThan(totalRows, ESTestCase::randomNonNegativeLong);
            case 4 -> emittedPages = randomValueOtherThan(emittedPages, ESTestCase::randomNonNegativeLong);
            case 5 -> emittedRows = randomValueOtherThan(emittedRows, ESTestCase::randomNonNegativeLong);
            default -> throw new UnsupportedOperationException();
        }
        return new LookupFromIndexOperator.Status(receivedPages, completedPages, procesNanos, totalRows, emittedPages, emittedRows);
    }

    public void testToXContent() {
        var status = new LookupFromIndexOperator.Status(100, 50, TimeValue.timeValueNanos(10).nanos(), 120, 88, 800);
        String json = Strings.toString(status, true, true);
        assertThat(json, equalTo("""
            {
              "process_nanos" : 10,
              "process_time" : "10nanos",
              "pages_received" : 100,
              "pages_completed" : 50,
              "pages_emitted" : 88,
              "rows_emitted" : 800,
              "total_rows" : 120
            }"""));
    }
}
