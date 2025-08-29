/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

public class TransportPutFollowActionTests extends ESTestCase {

    public void testCreateNewLocalDataStream() {
        DataStream remoteDataStream = generateDataSteam("logs-foobar", 3, false);
        Index backingIndexToFollow = remoteDataStream.getIndices().get(remoteDataStream.getIndices().size() - 1);
        DataStream result = TransportPutFollowAction.updateLocalDataStream(
            backingIndexToFollow,
            null,
            remoteDataStream.getName(),
            remoteDataStream
        );
        assertThat(result.getName(), equalTo(remoteDataStream.getName()));
        assertThat(result.getGeneration(), equalTo(remoteDataStream.getGeneration()));
        assertThat(result.getIndices().size(), equalTo(1));
        assertThat(result.getIndices().get(0), equalTo(backingIndexToFollow));
    }

    public void testUpdateLocalDataStream_followNewBackingIndex() {
        DataStream remoteDataStream = generateDataSteam("logs-foobar", 3, false);
        DataStream localDataStream = generateDataSteam("logs-foobar", 2, true);
        Index backingIndexToFollow = remoteDataStream.getIndices().get(remoteDataStream.getIndices().size() - 1);
        DataStream result = TransportPutFollowAction.updateLocalDataStream(
            backingIndexToFollow,
            localDataStream,
            remoteDataStream.getName(),
            remoteDataStream
        );
        assertThat(result.getName(), equalTo(remoteDataStream.getName()));
        assertThat(result.getGeneration(), equalTo(remoteDataStream.getGeneration()));
        assertThat(result.getIndices().size(), equalTo(3));
        assertThat(result.getIndices().get(0).getName(), DataStreamTestHelper.backingIndexEqualTo("logs-foobar", 1));
        assertThat(result.getIndices().get(1).getName(), DataStreamTestHelper.backingIndexEqualTo("logs-foobar", 2));
        assertThat(result.getIndices().get(2).getName(), DataStreamTestHelper.backingIndexEqualTo("logs-foobar", 3));
    }

    public void testUpdateLocalDataStream_followOlderBackingIndex() {
        // follow first backing index:
        DataStream remoteDataStream = generateDataSteam("logs-foobar", 5, false);
        DataStream localDataStream = generateDataSteam("logs-foobar", 5, true, DataStream.getDefaultBackingIndexName("logs-foobar", 5));
        Index backingIndexToFollow = remoteDataStream.getIndices().get(0);
        DataStream result = TransportPutFollowAction.updateLocalDataStream(
            backingIndexToFollow,
            localDataStream,
            remoteDataStream.getName(),
            remoteDataStream
        );
        assertThat(result.getName(), equalTo(remoteDataStream.getName()));
        assertThat(result.getGeneration(), equalTo(remoteDataStream.getGeneration()));
        assertThat(result.getIndices().size(), equalTo(2));
        assertThat(result.getIndices().get(0).getName(), DataStreamTestHelper.backingIndexEqualTo("logs-foobar", 1));
        assertThat(result.getIndices().get(1).getName(), DataStreamTestHelper.backingIndexEqualTo("logs-foobar", 5));

        // follow second last backing index:
        localDataStream = result;
        backingIndexToFollow = remoteDataStream.getIndices().get(remoteDataStream.getIndices().size() - 2);
        result = TransportPutFollowAction.updateLocalDataStream(
            backingIndexToFollow,
            localDataStream,
            remoteDataStream.getName(),
            remoteDataStream
        );
        assertThat(result.getName(), equalTo(remoteDataStream.getName()));
        assertThat(result.getGeneration(), equalTo(remoteDataStream.getGeneration()));
        assertThat(result.getIndices().size(), equalTo(3));
        assertThat(result.getIndices().get(0).getName(), DataStreamTestHelper.backingIndexEqualTo("logs-foobar", 1));
        assertThat(result.getIndices().get(1).getName(), DataStreamTestHelper.backingIndexEqualTo("logs-foobar", 4));
        assertThat(result.getIndices().get(2).getName(), DataStreamTestHelper.backingIndexEqualTo("logs-foobar", 5));
    }

    public void testLocalDataStreamBackingIndicesOrder() {
        DataStream remoteDataStream = generateDataSteam("logs-foobar", 8, false);

        List<Index> initialLocalBackingIndices = new ArrayList<>();
        initialLocalBackingIndices.add(new Index("random-name", UUID.randomUUID().toString()));
        String dsFirstGeneration = remoteDataStream.getIndices().getFirst().getName();
        initialLocalBackingIndices.add(new Index(dsFirstGeneration, UUID.randomUUID().toString()));
        String shrunkDsSecondGeneration = "shrink-" + remoteDataStream.getIndices().get(1).getName();
        initialLocalBackingIndices.add(new Index(shrunkDsSecondGeneration, UUID.randomUUID().toString()));
        String partialThirdGeneration = "partial-" + remoteDataStream.getIndices().get(2).getName();
        initialLocalBackingIndices.add(new Index(partialThirdGeneration, UUID.randomUUID().toString()));
        String forthGeneration = remoteDataStream.getIndices().get(3).getName();
        initialLocalBackingIndices.add(new Index(forthGeneration, UUID.randomUUID().toString()));
        String sixthGeneration = remoteDataStream.getIndices().get(5).getName();
        initialLocalBackingIndices.add(new Index(sixthGeneration, UUID.randomUUID().toString()));
        initialLocalBackingIndices.add(new Index("absolute-name", UUID.randomUUID().toString()));
        initialLocalBackingIndices.add(new Index("persistent-name", UUID.randomUUID().toString()));
        String restoredFifthGeneration = "restored-" + remoteDataStream.getIndices().get(4).getName();
        initialLocalBackingIndices.add(new Index(restoredFifthGeneration, UUID.randomUUID().toString()));
        String differentDSBackingIndex = DataStream.getDefaultBackingIndexName("different-datastream", 2);
        initialLocalBackingIndices.add(new Index(differentDSBackingIndex, UUID.randomUUID().toString()));

        DataStream localDataStream = DataStream.builder("logs-foobar", initialLocalBackingIndices)
            .setGeneration(initialLocalBackingIndices.size())
            .setMetadata(Map.of())
            .setReplicated(true)
            .build();

        // follow backing index 7
        Index backingIndexToFollow = remoteDataStream.getIndices().get(6);
        DataStream result = TransportPutFollowAction.updateLocalDataStream(
            backingIndexToFollow,
            localDataStream,
            remoteDataStream.getName(),
            remoteDataStream
        );

        assertThat(result.getName(), equalTo(remoteDataStream.getName()));
        assertThat(result.getGeneration(), equalTo(remoteDataStream.getGeneration()));
        assertThat(result.getIndices().size(), equalTo(initialLocalBackingIndices.size() + 1));
        // the later generation we just followed became the local data stream write index
        assertThat(result.getWriteIndex().getName(), is(remoteDataStream.getIndices().get(6).getName()));

        List<String> localIndicesNames = result.getIndices().stream().map(Index::getName).collect(Collectors.toList());
        assertThat(
            localIndicesNames,
            is(
                List.of(
                    differentDSBackingIndex,
                    "absolute-name",
                    "persistent-name",
                    "random-name",
                    dsFirstGeneration,
                    shrunkDsSecondGeneration,
                    partialThirdGeneration,
                    forthGeneration,
                    restoredFifthGeneration,
                    sixthGeneration,
                    remoteDataStream.getIndices().get(6).getName()
                )
            )
        );
    }

    static DataStream generateDataSteam(String name, int numBackingIndices, boolean replicate) {
        List<Index> backingIndices = IntStream.range(1, numBackingIndices + 1)
            .mapToObj(value -> DataStream.getDefaultBackingIndexName(name, value))
            .map(value -> new Index(value, "uuid"))
            .collect(Collectors.toList());
        long generation = backingIndices.size();
        return DataStream.builder(name, backingIndices).setGeneration(generation).setMetadata(Map.of()).setReplicated(replicate).build();
    }

    static DataStream generateDataSteam(String name, int generation, boolean replicate, String... backingIndexNames) {
        List<Index> backingIndices = Arrays.stream(backingIndexNames).map(value -> new Index(value, "uuid")).collect(Collectors.toList());
        return DataStream.builder(name, backingIndices).setGeneration(generation).setMetadata(Map.of()).setReplicated(replicate).build();
    }

}
