/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class HashAggregationOperatorStatusTests extends AbstractWireSerializingTestCase<HashAggregationOperator.Status> {
    public static HashAggregationOperator.Status simple() {
        var partitioning = new ParallelHashAggregationOperator.PartitioningStatus(
            100011,
            3,
            1001,
            50012,
            40013,
            4,
            90014,
            5,
            2002,
            60016,
            7
        );
        return new HashAggregationOperator.Status(500012, 200012, 123, 111, 222, 180017, 2, List.of(partitioning));
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
              "rows_emitted" : 222,
              "emit_count" : 2,
              "emit_nanos" : 180017,
              "emit_time" : "180micros",
              "partitioning" : {
                "add_input_nanos" : 100011,
                "add_input_time" : "100micros",
                "add_input_inline_count" : 3,
                "add_input_inline_rows" : 1001,
                "add_input_inline_nanos" : 50012,
                "add_input_inline_time" : "50micros",
                "finish_nanos" : 40013,
                "finish_time" : "40micros",
                "split_count" : 4,
                "split_nanos" : 90014,
                "split_time" : "90micros",
                "inline_emit_count" : 5,
                "inline_emit_rows" : 2002,
                "inline_emit_nanos" : 60016,
                "inline_emit_time" : "60micros",
                "worker_tasks" : 7
              }
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
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(List.of(ParallelHashAggregationOperator.PartitioningStatus.ENTRY));
    }

    static List<Operator.Status.ExtraStatus> randomExtraFields() {
        return randomBoolean()
            ? List.of()
            : List.of(
                new ParallelHashAggregationOperator.PartitioningStatus(
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                )
            );
    }

    @Override
    public HashAggregationOperator.Status createTestInstance() {
        return new HashAggregationOperator.Status(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomExtraFields()
        );
    }

    @Override
    protected HashAggregationOperator.Status mutateInstance(HashAggregationOperator.Status instance) {
        long hashNanos = instance.hashNanos();
        long aggregationNanos = instance.aggregationNanos();
        int pagesProcessed = instance.pagesProcessed();
        long rowsReceived = instance.rowsReceived();
        long rowsEmitted = instance.rowsEmitted();
        long emitNanos = instance.emitNanos();
        long emitCount = instance.emitCount();
        List<Operator.Status.ExtraStatus> extraFields = instance.extraFields();
        switch (between(0, 7)) {
            case 0 -> hashNanos = randomValueOtherThan(hashNanos, ESTestCase::randomNonNegativeLong);
            case 1 -> aggregationNanos = randomValueOtherThan(aggregationNanos, ESTestCase::randomNonNegativeLong);
            case 2 -> pagesProcessed = randomValueOtherThan(pagesProcessed, ESTestCase::randomNonNegativeInt);
            case 3 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            case 4 -> rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
            case 5 -> emitNanos = randomValueOtherThan(emitNanos, ESTestCase::randomNonNegativeLong);
            case 6 -> emitCount = randomValueOtherThan(emitCount, ESTestCase::randomNonNegativeLong);
            case 7 -> extraFields = randomValueOtherThan(extraFields, HashAggregationOperatorStatusTests::randomExtraFields);
            default -> throw new UnsupportedOperationException();
        }
        return new HashAggregationOperator.Status(
            hashNanos,
            aggregationNanos,
            pagesProcessed,
            rowsReceived,
            rowsEmitted,
            emitNanos,
            emitCount,
            extraFields
        );
    }
}
