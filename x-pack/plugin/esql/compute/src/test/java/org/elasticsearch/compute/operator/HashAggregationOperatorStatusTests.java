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

public class HashAggregationOperatorStatusTests extends AbstractWireSerializingTestCase<HashAggregationOperator.Status> {
    public static HashAggregationOperator.Status simple() {
        return new HashAggregationOperator.Status(500012, 200012, 123, 111, 222);
    }

    public static String simpleToJson() {
        return """
            {
              "hash_nanos" : 500012,
              "hash_time" : "500micros",
              "aggregation_nanos" : 200012,
              "aggregation_time" : "200micros",
              "pages_processed" : 123,
              "rows_received" : 111,
              "rows_emitted" : 222
            }""";
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple(), true, true), equalTo(simpleToJson()));
    }

    @Override
    protected Writeable.Reader<HashAggregationOperator.Status> instanceReader() {
        return HashAggregationOperator.Status::new;
    }

    @Override
    public HashAggregationOperator.Status createTestInstance() {
        return new HashAggregationOperator.Status(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected HashAggregationOperator.Status mutateInstance(HashAggregationOperator.Status instance) {
        long hashNanos = instance.hashNanos();
        long aggregationNanos = instance.aggregationNanos();
        int pagesProcessed = instance.pagesProcessed();
        long rowsReceived = instance.rowsReceived();
        long rowsEmitted = instance.rowsEmitted();
        switch (between(0, 4)) {
            case 0 -> hashNanos = randomValueOtherThan(hashNanos, ESTestCase::randomNonNegativeLong);
            case 1 -> aggregationNanos = randomValueOtherThan(aggregationNanos, ESTestCase::randomNonNegativeLong);
            case 2 -> pagesProcessed = randomValueOtherThan(pagesProcessed, ESTestCase::randomNonNegativeInt);
            case 3 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            case 4 -> rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
            default -> throw new UnsupportedOperationException();
        }
        return new HashAggregationOperator.Status(hashNanos, aggregationNanos, pagesProcessed, rowsReceived, rowsEmitted);
    }
}
