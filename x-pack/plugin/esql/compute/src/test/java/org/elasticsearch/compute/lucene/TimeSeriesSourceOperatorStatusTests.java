/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesSourceOperatorStatusTests extends AbstractWireSerializingTestCase<TimeSeriesSourceOperator.Status> {
    public static TimeSeriesSourceOperator.Status simple() {
        return new TimeSeriesSourceOperator.Status(
            2,
            Set.of("*:*"),
            new TreeSet<>(List.of("a:0", "a:1")),
            1002,
            0,
            1,
            5,
            123,
            99990,
            8000,
            222,
            Map.of("b:0", LuceneSliceQueue.PartitioningStrategy.SHARD, "a:1", LuceneSliceQueue.PartitioningStrategy.DOC),
            250,
            28000
        );
    }

    public static String simpleToJson() {
        return """
            {
              "processed_slices" : 2,
              "processed_queries" : [
                "*:*"
              ],
              "processed_shards" : [
                "a:0",
                "a:1"
              ],
              "process_nanos" : 1002,
              "process_time" : "1micros",
              "slice_index" : 0,
              "total_slices" : 1,
              "pages_emitted" : 5,
              "slice_min" : 123,
              "slice_max" : 99990,
              "current" : 8000,
              "rows_emitted" : 222,
              "partitioning_strategies" : {
                "a:1" : "DOC",
                "b:0" : "SHARD"
              },
              "tsid_loaded" : 250,
              "values_loaded" : 28000
            }""";
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple(), true, true), equalTo(simpleToJson()));
    }

    @Override
    protected Writeable.Reader<TimeSeriesSourceOperator.Status> instanceReader() {
        return TimeSeriesSourceOperator.Status::new;
    }

    @Override
    public TimeSeriesSourceOperator.Status createTestInstance() {
        return new TimeSeriesSourceOperator.Status(
            randomNonNegativeInt(),
            randomProcessedQueries(),
            randomProcessedShards(),
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomPartitioningStrategies(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    private static Set<String> randomProcessedQueries() {
        int size = between(0, 10);
        Set<String> set = new TreeSet<>();
        while (set.size() < size) {
            set.add(randomAlphaOfLength(5));
        }
        return set;
    }

    private static Set<String> randomProcessedShards() {
        int size = between(0, 10);
        Set<String> set = new TreeSet<>();
        while (set.size() < size) {
            set.add(randomAlphaOfLength(3) + ":" + between(0, 10));
        }
        return set;
    }

    private static Map<String, LuceneSliceQueue.PartitioningStrategy> randomPartitioningStrategies() {
        int size = between(0, 10);
        Map<String, LuceneSliceQueue.PartitioningStrategy> partitioningStrategies = new HashMap<>();
        while (partitioningStrategies.size() < size) {
            partitioningStrategies.put(
                randomAlphaOfLength(3) + ":" + between(0, 10),
                randomFrom(LuceneSliceQueue.PartitioningStrategy.values())
            );
        }
        return partitioningStrategies;
    }

    @Override
    protected TimeSeriesSourceOperator.Status mutateInstance(TimeSeriesSourceOperator.Status instance) {
        int processedSlices = instance.processedSlices();
        Set<String> processedQueries = instance.processedQueries();
        Set<String> processedShards = instance.processedShards();
        long processNanos = instance.processNanos();
        int sliceIndex = instance.sliceIndex();
        int totalSlices = instance.totalSlices();
        int pagesEmitted = instance.pagesEmitted();
        int sliceMin = instance.sliceMin();
        int sliceMax = instance.sliceMax();
        int current = instance.current();
        long rowsEmitted = instance.rowsEmitted();
        long tsidLoaded = instance.tsidLoaded();
        long valuesLoaded = instance.valuesLoaded();
        Map<String, LuceneSliceQueue.PartitioningStrategy> partitioningStrategies = instance.partitioningStrategies();
        switch (between(0, 13)) {
            case 0 -> processedSlices = randomValueOtherThan(processedSlices, ESTestCase::randomNonNegativeInt);
            case 1 -> processedQueries = randomValueOtherThan(
                processedQueries,
                TimeSeriesSourceOperatorStatusTests::randomProcessedQueries
            );
            case 2 -> processedShards = randomValueOtherThan(processedShards, TimeSeriesSourceOperatorStatusTests::randomProcessedShards);
            case 3 -> processNanos = randomValueOtherThan(processNanos, ESTestCase::randomNonNegativeLong);
            case 4 -> sliceIndex = randomValueOtherThan(sliceIndex, ESTestCase::randomNonNegativeInt);
            case 5 -> totalSlices = randomValueOtherThan(totalSlices, ESTestCase::randomNonNegativeInt);
            case 6 -> pagesEmitted = randomValueOtherThan(pagesEmitted, ESTestCase::randomNonNegativeInt);
            case 7 -> sliceMin = randomValueOtherThan(sliceMin, ESTestCase::randomNonNegativeInt);
            case 8 -> sliceMax = randomValueOtherThan(sliceMax, ESTestCase::randomNonNegativeInt);
            case 9 -> current = randomValueOtherThan(current, ESTestCase::randomNonNegativeInt);
            case 10 -> rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
            case 11 -> partitioningStrategies = randomValueOtherThan(
                partitioningStrategies,
                TimeSeriesSourceOperatorStatusTests::randomPartitioningStrategies
            );
            case 12 -> tsidLoaded = randomValueOtherThan(tsidLoaded, ESTestCase::randomNonNegativeLong);
            case 13 -> valuesLoaded = randomValueOtherThan(valuesLoaded, ESTestCase::randomNonNegativeLong);
            default -> throw new UnsupportedOperationException();
        }
        return new TimeSeriesSourceOperator.Status(
            processedSlices,
            processedQueries,
            processedShards,
            processNanos,
            sliceIndex,
            totalSlices,
            pagesEmitted,
            sliceMin,
            sliceMax,
            current,
            rowsEmitted,
            partitioningStrategies,
            tsidLoaded,
            valuesLoaded
        );
    }
}
