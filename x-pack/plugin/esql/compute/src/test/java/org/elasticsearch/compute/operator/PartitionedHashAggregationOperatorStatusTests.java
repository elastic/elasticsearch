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

public class PartitionedHashAggregationOperatorStatusTests extends AbstractWireSerializingTestCase<
    PartitionedHashAggregationOperator.Status> {

    public static PartitionedHashAggregationOperator.Status simple() {
        return new PartitionedHashAggregationOperator.Status(100012L, 50006L, 300024L, 200018L, 10, 1000L, 5L, true);
    }

    public static String simpleToJson() {
        return """
            {
              "partitioned_mode" : true,
              "routing_nanos" : 100012,
              "routing_time" : "100micros",
              "conversion_nanos" : 50006,
              "conversion_time" : "50micros",
              "hash_nanos" : 300024,
              "hash_time" : "300micros",
              "aggregation_nanos" : 200018,
              "aggregation_time" : "200micros",
              "pages_processed" : 10,
              "rows_received" : 1000,
              "rows_emitted" : 5
            }""";
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple(), true, true), equalTo(simpleToJson()));
    }

    @Override
    protected Writeable.Reader<PartitionedHashAggregationOperator.Status> instanceReader() {
        return PartitionedHashAggregationOperator.Status::new;
    }

    @Override
    public PartitionedHashAggregationOperator.Status createTestInstance() {
        return new PartitionedHashAggregationOperator.Status(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomBoolean()
        );
    }

    @Override
    protected PartitionedHashAggregationOperator.Status mutateInstance(PartitionedHashAggregationOperator.Status instance) {
        long routingNanos = instance.routingNanos();
        long conversionNanos = instance.conversionNanos();
        long hashNanos = instance.hashNanos();
        long aggregationNanos = instance.aggregationNanos();
        int pagesProcessed = instance.pagesProcessed();
        long rowsReceived = instance.rowsReceived();
        long rowsEmitted = instance.rowsEmitted();
        boolean partitionedMode = instance.partitionedMode();
        switch (between(0, 7)) {
            case 0 -> routingNanos = randomValueOtherThan(routingNanos, ESTestCase::randomNonNegativeLong);
            case 1 -> conversionNanos = randomValueOtherThan(conversionNanos, ESTestCase::randomNonNegativeLong);
            case 2 -> hashNanos = randomValueOtherThan(hashNanos, ESTestCase::randomNonNegativeLong);
            case 3 -> aggregationNanos = randomValueOtherThan(aggregationNanos, ESTestCase::randomNonNegativeLong);
            case 4 -> pagesProcessed = randomValueOtherThan(pagesProcessed, ESTestCase::randomNonNegativeInt);
            case 5 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            case 6 -> rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
            case 7 -> partitionedMode = partitionedMode == false;
            default -> throw new UnsupportedOperationException();
        }
        return new PartitionedHashAggregationOperator.Status(
            routingNanos,
            conversionNanos,
            hashNanos,
            aggregationNanos,
            pagesProcessed,
            rowsReceived,
            rowsEmitted,
            partitionedMode
        );
    }
}
