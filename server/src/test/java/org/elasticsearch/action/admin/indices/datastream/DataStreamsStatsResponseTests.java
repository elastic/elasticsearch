/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.datastream;

import org.elasticsearch.ElasticsearchException;
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
            dataStreamStats.add(new DataStreamsStatsAction.DataStreamStats(dataStreamName, backingIndices,
                new ByteSizeValue(storeSize), maximumTimestamp));
        }
        int totalShards = randomIntBetween(backingIndicesTotal, backingIndicesTotal * 3);
        int successfulShards = randomInt(totalShards);
        int failedShards = totalShards - successfulShards;
        List<DefaultShardOperationFailedException> exceptions = new ArrayList<>();
        for (int i = 0; i < failedShards; i++) {
            exceptions.add(new DefaultShardOperationFailedException(randomAlphaOfLength(8).toLowerCase(Locale.getDefault()),
                randomInt(totalShards), new ElasticsearchException("boom")));
        }
        return new DataStreamsStatsAction.Response(totalShards, successfulShards, failedShards, exceptions,
            dataStreamCount, backingIndicesTotal, new ByteSizeValue(totalStoreSize),
            dataStreamStats.toArray(new DataStreamsStatsAction.DataStreamStats[0]));
    }

    private static long randomRecentTimestamp() {
        long base = System.currentTimeMillis();
        return randomLongBetween(base - TimeUnit.HOURS.toMillis(1), base);
    }
}
