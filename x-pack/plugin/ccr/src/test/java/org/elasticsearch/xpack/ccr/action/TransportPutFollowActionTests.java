/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStream.TimestampField;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class TransportPutFollowActionTests extends ESTestCase {

    public void testCreateNewLocalDataStream() {
        DataStream remoteDataStream = generateDataSteam("logs-foobar", 3);
        Index backingIndexToFollow = remoteDataStream.getIndices().get(remoteDataStream.getIndices().size() - 1);
        DataStream result = TransportPutFollowAction.updateLocalDataStream(backingIndexToFollow, null, remoteDataStream);
        assertThat(result.getName(), equalTo(remoteDataStream.getName()));
        assertThat(result.getTimeStampField(), equalTo(remoteDataStream.getTimeStampField()));
        assertThat(result.getGeneration(), equalTo(remoteDataStream.getGeneration()));
        assertThat(result.getIndices().size(), equalTo(1));
        assertThat(result.getIndices().get(0), equalTo(backingIndexToFollow));
    }

    public void testUpdateLocalDataStream_followNewBackingIndex() {
        DataStream remoteDataStream = generateDataSteam("logs-foobar", 3);
        DataStream localDataStream = generateDataSteam("logs-foobar", 2);
        Index backingIndexToFollow = remoteDataStream.getIndices().get(remoteDataStream.getIndices().size() - 1);
        DataStream result = TransportPutFollowAction.updateLocalDataStream(backingIndexToFollow, localDataStream, remoteDataStream);
        assertThat(result.getName(), equalTo(remoteDataStream.getName()));
        assertThat(result.getTimeStampField(), equalTo(remoteDataStream.getTimeStampField()));
        assertThat(result.getGeneration(), equalTo(remoteDataStream.getGeneration()));
        assertThat(result.getIndices().size(), equalTo(3));
        assertThat(result.getIndices().get(0).getName(), equalTo(DataStream.getDefaultBackingIndexName("logs-foobar", 1)));
        assertThat(result.getIndices().get(1).getName(), equalTo(DataStream.getDefaultBackingIndexName("logs-foobar", 2)));
        assertThat(result.getIndices().get(2).getName(), equalTo(DataStream.getDefaultBackingIndexName("logs-foobar", 3)));
    }

    public void testUpdateLocalDataStream_followOlderBackingIndex() {
        // follow first backing index:
        DataStream remoteDataStream = generateDataSteam("logs-foobar", 5);
        DataStream localDataStream = generateDataSteam("logs-foobar", 5, DataStream.getDefaultBackingIndexName("logs-foobar", 5));
        Index backingIndexToFollow = remoteDataStream.getIndices().get(0);
        DataStream result = TransportPutFollowAction.updateLocalDataStream(backingIndexToFollow, localDataStream, remoteDataStream);
        assertThat(result.getName(), equalTo(remoteDataStream.getName()));
        assertThat(result.getTimeStampField(), equalTo(remoteDataStream.getTimeStampField()));
        assertThat(result.getGeneration(), equalTo(remoteDataStream.getGeneration()));
        assertThat(result.getIndices().size(), equalTo(2));
        assertThat(result.getIndices().get(0).getName(), equalTo(DataStream.getDefaultBackingIndexName("logs-foobar", 1)));
        assertThat(result.getIndices().get(1).getName(), equalTo(DataStream.getDefaultBackingIndexName("logs-foobar", 5)));

        // follow second last backing index:
        localDataStream = result;
        backingIndexToFollow = remoteDataStream.getIndices().get(remoteDataStream.getIndices().size() - 2);
        result = TransportPutFollowAction.updateLocalDataStream(backingIndexToFollow, localDataStream, remoteDataStream);
        assertThat(result.getName(), equalTo(remoteDataStream.getName()));
        assertThat(result.getTimeStampField(), equalTo(remoteDataStream.getTimeStampField()));
        assertThat(result.getGeneration(), equalTo(remoteDataStream.getGeneration()));
        assertThat(result.getIndices().size(), equalTo(3));
        assertThat(result.getIndices().get(0).getName(), equalTo(DataStream.getDefaultBackingIndexName("logs-foobar", 1)));
        assertThat(result.getIndices().get(1).getName(), equalTo(DataStream.getDefaultBackingIndexName("logs-foobar", 4)));
        assertThat(result.getIndices().get(2).getName(), equalTo(DataStream.getDefaultBackingIndexName("logs-foobar", 5)));
    }

    static DataStream generateDataSteam(String name, int numBackingIndices) {
        List<Index> backingIndices = IntStream.range(1, numBackingIndices + 1)
            .mapToObj(value -> DataStream.getDefaultBackingIndexName(name, value))
            .map(value -> new Index(value, "uuid"))
            .collect(Collectors.toList());
        return new DataStream(name, new TimestampField("@timestamp"), backingIndices);
    }

    static DataStream generateDataSteam(String name, int generation, String... backingIndexNames) {
        List<Index> backingIndices = Arrays.stream(backingIndexNames)
            .map(value -> new Index(value, "uuid"))
            .collect(Collectors.toList());
        return new DataStream(name, new TimestampField("@timestamp"), backingIndices, generation, Map.of());
    }

}
