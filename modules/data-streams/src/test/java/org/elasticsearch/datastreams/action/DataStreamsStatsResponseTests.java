/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.datastreams.DataStreamsStatsAction;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class DataStreamsStatsResponseTests extends AbstractWireSerializingTestCase<DataStreamsStatsAction.Response> {
    @Override
    protected Writeable.Reader<DataStreamsStatsAction.Response> instanceReader() {
        return DataStreamsStatsAction.Response::new;
    }

    @Override
    protected DataStreamsStatsAction.Response createTestInstance() {
        return randomStatsResponse();
    }

    @Override
    protected DataStreamsStatsAction.Response mutateInstance(DataStreamsStatsAction.Response instance) {
        int dataStreamCount = instance.getDataStreamCount();
        int backingIndices = instance.getBackingIndices();
        ByteSizeValue totalStoreSize = instance.getTotalStoreSize();
        DataStreamsStatsAction.DataStreamStats[] dataStreams = instance.getDataStreams();
        switch (between(0, 3)) {
            case 0 -> dataStreamCount = randomValueOtherThan(dataStreamCount, () -> randomInt(10));
            case 1 -> backingIndices = randomValueOtherThan(backingIndices, () -> randomInt(50));
            case 2 -> totalStoreSize = randomValueOtherThan(
                totalStoreSize,
                () -> ByteSizeValue.ofBytes(randomLongBetween(0, 1_000_000_000L))
            );
            case 3 -> dataStreams = mutateDataStreams(dataStreams);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        // The inherited broadcast shard fields are not part of Response#equals, so changing one would
        // not produce an unequal instance. Carry them over unchanged.
        return new DataStreamsStatsAction.Response(
            instance.getTotalShards(),
            instance.getSuccessfulShards(),
            instance.getFailedShards(),
            Arrays.asList(instance.getShardFailures()),
            dataStreamCount,
            backingIndices,
            totalStoreSize,
            dataStreams
        );
    }

    /**
     * Returns a changed copy of {@code dataStreams}, never modifying the input array (which is the live array of the
     * instance being mutated). Appends a new entry when the array is empty, otherwise either appends one or replaces
     * one entry with a different one.
     */
    private static DataStreamsStatsAction.DataStreamStats[] mutateDataStreams(DataStreamsStatsAction.DataStreamStats[] dataStreams) {
        if (dataStreams.length == 0 || randomBoolean()) {
            DataStreamsStatsAction.DataStreamStats[] mutated = Arrays.copyOf(dataStreams, dataStreams.length + 1);
            mutated[dataStreams.length] = randomDataStreamStats();
            return mutated;
        }
        DataStreamsStatsAction.DataStreamStats[] mutated = dataStreams.clone();
        int index = between(0, mutated.length - 1);
        mutated[index] = randomValueOtherThan(mutated[index], DataStreamsStatsResponseTests::randomDataStreamStats);
        return mutated;
    }

    public static DataStreamsStatsAction.Response randomStatsResponse() {
        int dataStreamCount = randomInt(10);
        int backingIndicesTotal = 0;
        long totalStoreSize = 0L;
        ArrayList<DataStreamsStatsAction.DataStreamStats> dataStreamStats = new ArrayList<>();
        for (int i = 0; i < dataStreamCount; i++) {
            DataStreamsStatsAction.DataStreamStats stats = randomDataStreamStats();
            backingIndicesTotal += stats.getBackingIndices();
            totalStoreSize += stats.getStoreSize().getBytes();
            dataStreamStats.add(stats);
        }
        int totalShards = randomIntBetween(backingIndicesTotal, backingIndicesTotal * 3);
        int successfulShards = randomInt(totalShards);
        int failedShards = totalShards - successfulShards;
        List<DefaultShardOperationFailedException> exceptions = new ArrayList<>();
        for (int i = 0; i < failedShards; i++) {
            exceptions.add(
                new DefaultShardOperationFailedException(
                    randomAlphaOfLength(8).toLowerCase(Locale.getDefault()),
                    randomInt(totalShards),
                    new ElasticsearchException("boom")
                )
            );
        }
        return new DataStreamsStatsAction.Response(
            totalShards,
            successfulShards,
            failedShards,
            exceptions,
            dataStreamCount,
            backingIndicesTotal,
            ByteSizeValue.ofBytes(totalStoreSize),
            dataStreamStats.toArray(DataStreamsStatsAction.DataStreamStats[]::new)
        );
    }

    private static DataStreamsStatsAction.DataStreamStats randomDataStreamStats() {
        String dataStreamName = randomAlphaOfLength(8).toLowerCase(Locale.getDefault());
        int backingIndices = randomInt(5);
        long storeSize = randomLongBetween(250, 1000000000);
        long maximumTimestamp = randomRecentTimestamp();
        return new DataStreamsStatsAction.DataStreamStats(
            dataStreamName,
            backingIndices,
            ByteSizeValue.ofBytes(storeSize),
            maximumTimestamp
        );
    }

    private static long randomRecentTimestamp() {
        long base = System.currentTimeMillis();
        return randomLongBetween(base - TimeUnit.HOURS.toMillis(1), base);
    }
}
