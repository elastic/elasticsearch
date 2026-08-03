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

public class PartitionedHashMergeOperatorStatusTests extends AbstractWireSerializingTestCase<PartitionedHashMergeOperator.Status> {

    public static PartitionedHashMergeOperator.Status simple() {
        return new PartitionedHashMergeOperator.Status(80007L, 400032L, 250020L, 15, 2000L);
    }

    public static String simpleToJson() {
        return """
            {
              "reconcile_nanos" : 80007,
              "reconcile_time" : "80micros",
              "hash_nanos" : 400032,
              "hash_time" : "400micros",
              "aggregation_nanos" : 250020,
              "aggregation_time" : "250micros",
              "pages_processed" : 15,
              "rows_received" : 2000
            }""";
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple(), true, true), equalTo(simpleToJson()));
    }

    @Override
    protected Writeable.Reader<PartitionedHashMergeOperator.Status> instanceReader() {
        return PartitionedHashMergeOperator.Status::new;
    }

    @Override
    public PartitionedHashMergeOperator.Status createTestInstance() {
        return new PartitionedHashMergeOperator.Status(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected PartitionedHashMergeOperator.Status mutateInstance(PartitionedHashMergeOperator.Status instance) {
        long reconcileNanos = instance.reconcileNanos();
        long hashNanos = instance.hashNanos();
        long aggregationNanos = instance.aggregationNanos();
        int pagesProcessed = instance.pagesProcessed();
        long rowsReceived = instance.rowsReceived();
        switch (between(0, 4)) {
            case 0 -> reconcileNanos = randomValueOtherThan(reconcileNanos, ESTestCase::randomNonNegativeLong);
            case 1 -> hashNanos = randomValueOtherThan(hashNanos, ESTestCase::randomNonNegativeLong);
            case 2 -> aggregationNanos = randomValueOtherThan(aggregationNanos, ESTestCase::randomNonNegativeLong);
            case 3 -> pagesProcessed = randomValueOtherThan(pagesProcessed, ESTestCase::randomNonNegativeInt);
            case 4 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            default -> throw new UnsupportedOperationException();
        }
        return new PartitionedHashMergeOperator.Status(reconcileNanos, hashNanos, aggregationNanos, pagesProcessed, rowsReceived);
    }
}
