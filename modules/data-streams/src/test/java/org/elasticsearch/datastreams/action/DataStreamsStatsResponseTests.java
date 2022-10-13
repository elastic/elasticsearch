/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.datastreams.DataStreamsStatsAction;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
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

    public static DataStreamsStatsAction.Response randomStatsResponse() {
        int dataStreamCount = randomInt(10);
        int backingIndicesTotal = 0;
        long totalStoreSize = 0L;
        ArrayList<DataStreamsStatsAction.DataStreamStats> dataStreamStats = new ArrayList<>();
        for (int i = 0; i < dataStreamCount; i++) {
            String dataStreamName = randomAlphaOfLength(8).toLowerCase(Locale.getDefault());
            int backingIndices = randomInt(5);
            backingIndicesTotal += backingIndices;
            long storeSize = randomLongBetween(250, 1000000000);
            totalStoreSize += storeSize;
            long maximumTimestamp = randomRecentTimestamp();
            dataStreamStats.add(
                new DataStreamsStatsAction.DataStreamStats(dataStreamName, backingIndices, new ByteSizeValue(storeSize), maximumTimestamp)
            );
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
            new ByteSizeValue(totalStoreSize),
            dataStreamStats.toArray(DataStreamsStatsAction.DataStreamStats[]::new)
        );
    }

    private static long randomRecentTimestamp() {
        long base = System.currentTimeMillis();
        return randomLongBetween(base - TimeUnit.HOURS.toMillis(1), base);
    }
}
