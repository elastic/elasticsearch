/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createTimestampField;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.generateMapping;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class MetadataDataStreamsServiceTests extends MapperServiceTestCase {

    public void testAddBackingIndex() {
        final long epochMillis = System.currentTimeMillis();
        final int numBackingIndices = randomIntBetween(1, 4);
        final String dataStreamName = randomAlphaOfLength(5);
        IndexMetadata[] backingIndices = new IndexMetadata[numBackingIndices];
        Metadata.Builder mb = Metadata.builder();
        for (int k = 0; k < numBackingIndices; k++) {
            backingIndices[k] = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, k + 1, epochMillis))
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putMapping(generateMapping("@timestamp"))
                .build();
            mb.put(backingIndices[k], false);
        }

        mb.put(
            DataStreamTestHelper.newInstance(
                dataStreamName,
                createTimestampField("@timestamp"),
                Arrays.stream(backingIndices).map(IndexMetadata::getIndex).collect(Collectors.toList())
            )
        );

        final IndexMetadata indexToAdd = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(generateMapping("@timestamp"))
            .build();
        mb.put(indexToAdd, false);

        ClusterState originalState = ClusterState.builder(new ClusterName("dummy")).metadata(mb.build()).build();
        ClusterState newState = MetadataDataStreamsService.modifyDataStream(
            originalState,
            List.of(DataStreamAction.addBackingIndex(dataStreamName, indexToAdd.getIndex().getName())),
            this::getMapperService
        );

        IndexAbstraction ds = newState.metadata().getIndicesLookup().get(dataStreamName);
        assertThat(ds, notNullValue());
        assertThat(ds.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
        assertThat(ds.getIndices().size(), equalTo(numBackingIndices + 1));
        List<String> backingIndexNames = ds.getIndices()
            .stream()
            .filter(x -> x.getName().startsWith(".ds-"))
            .map(Index::getName)
            .collect(Collectors.toList());
        assertThat(
            backingIndexNames,
            containsInAnyOrder(
                Arrays.stream(backingIndices)
                    .map(IndexMetadata::getIndex)
                    .map(Index::getName)
                    .collect(Collectors.toList())
                    .toArray(Strings.EMPTY_ARRAY)
            )
        );
        IndexMetadata zeroIndex = newState.metadata().index(ds.getIndices().get(0));
        assertThat(zeroIndex.getIndex(), equalTo(indexToAdd.getIndex()));
        assertThat(zeroIndex.getSettings().get("index.hidden"), equalTo("true"));
        assertThat(zeroIndex.getAliases().size(), equalTo(0));
    }

    public void testRemoveBackingIndex() {
        final long epochMillis = System.currentTimeMillis();
        final int numBackingIndices = randomIntBetween(2, 4);
        final String dataStreamName = randomAlphaOfLength(5);
        IndexMetadata[] backingIndices = new IndexMetadata[numBackingIndices];
        Metadata.Builder mb = Metadata.builder();
        for (int k = 0; k < numBackingIndices; k++) {
            backingIndices[k] = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, k + 1, epochMillis))
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putMapping(generateMapping("@timestamp"))
                .build();
            mb.put(backingIndices[k], false);
        }

        mb.put(
            DataStreamTestHelper.newInstance(
                dataStreamName,
                createTimestampField("@timestamp"),
                Arrays.stream(backingIndices).map(IndexMetadata::getIndex).collect(Collectors.toList())
            )
        );

        final IndexMetadata indexToRemove = backingIndices[randomIntBetween(0, numBackingIndices - 2)];
        ClusterState originalState = ClusterState.builder(new ClusterName("dummy")).metadata(mb.build()).build();
        ClusterState newState = MetadataDataStreamsService.modifyDataStream(
            originalState,
            List.of(DataStreamAction.removeBackingIndex(dataStreamName, indexToRemove.getIndex().getName())),
            this::getMapperService
        );

        IndexAbstraction ds = newState.metadata().getIndicesLookup().get(dataStreamName);
        assertThat(ds, notNullValue());
        assertThat(ds.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
        assertThat(ds.getIndices().size(), equalTo(numBackingIndices - 1));

        List<Index> expectedBackingIndices = ds.getIndices()
            .stream()
            .filter(x -> x.getName().equals(indexToRemove.getIndex().getName()) == false)
            .collect(Collectors.toList());
        assertThat(expectedBackingIndices, containsInAnyOrder(ds.getIndices().toArray()));

        IndexMetadata removedIndex = newState.metadata().getIndices().get(indexToRemove.getIndex().getName());
        assertThat(removedIndex, notNullValue());
        assertThat(removedIndex.getSettings().get("index.hidden"), equalTo("false"));
        assertNull(newState.metadata().getIndicesLookup().get(indexToRemove.getIndex().getName()).getParentDataStream());
    }

    public void testRemoveWriteIndexIsProhibited() {
        final long epochMillis = System.currentTimeMillis();
        final int numBackingIndices = randomIntBetween(1, 4);
        final String dataStreamName = randomAlphaOfLength(5);
        IndexMetadata[] backingIndices = new IndexMetadata[numBackingIndices];
        Metadata.Builder mb = Metadata.builder();
        for (int k = 0; k < numBackingIndices; k++) {
            backingIndices[k] = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, k + 1, epochMillis))
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putMapping(generateMapping("@timestamp"))
                .build();
            mb.put(backingIndices[k], false);
        }

        mb.put(
            DataStreamTestHelper.newInstance(
                dataStreamName,
                createTimestampField("@timestamp"),
                Arrays.stream(backingIndices).map(IndexMetadata::getIndex).collect(Collectors.toList())
            )
        );

        final IndexMetadata indexToRemove = backingIndices[numBackingIndices - 1];
        ClusterState originalState = ClusterState.builder(new ClusterName("dummy")).metadata(mb.build()).build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(
                originalState,
                List.of(DataStreamAction.removeBackingIndex(dataStreamName, indexToRemove.getIndex().getName())),
                this::getMapperService
            )
        );

        assertThat(
            e.getMessage(),
            containsString(
                String.format(
                    Locale.ROOT,
                    "cannot remove backing index [%s] of data stream [%s] because it is the write index",
                    indexToRemove.getIndex().getName(),
                    dataStreamName
                )
            )
        );
    }

    public void testAddRemoveAddRoundtripInSingleRequest() {
        final long epochMillis = System.currentTimeMillis();
        final int numBackingIndices = randomIntBetween(1, 4);
        final String dataStreamName = randomAlphaOfLength(5);
        IndexMetadata[] backingIndices = new IndexMetadata[numBackingIndices];
        Metadata.Builder mb = Metadata.builder();
        for (int k = 0; k < numBackingIndices; k++) {
            backingIndices[k] = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, k + 1, epochMillis))
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putMapping(generateMapping("@timestamp"))
                .build();
            mb.put(backingIndices[k], false);
        }

        mb.put(
            DataStreamTestHelper.newInstance(
                dataStreamName,
                createTimestampField("@timestamp"),
                Arrays.stream(backingIndices).map(IndexMetadata::getIndex).collect(Collectors.toList())
            )
        );

        final IndexMetadata indexToAdd = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(generateMapping("@timestamp"))
            .build();
        mb.put(indexToAdd, false);

        ClusterState originalState = ClusterState.builder(new ClusterName("dummy")).metadata(mb.build()).build();
        ClusterState newState = MetadataDataStreamsService.modifyDataStream(
            originalState,
            List.of(
                DataStreamAction.addBackingIndex(dataStreamName, indexToAdd.getIndex().getName()),
                DataStreamAction.removeBackingIndex(dataStreamName, indexToAdd.getIndex().getName()),
                DataStreamAction.addBackingIndex(dataStreamName, indexToAdd.getIndex().getName())
            ),
            this::getMapperService
        );

        IndexAbstraction ds = newState.metadata().getIndicesLookup().get(dataStreamName);
        assertThat(ds, notNullValue());
        assertThat(ds.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
        assertThat(ds.getIndices().size(), equalTo(numBackingIndices + 1));
        List<String> backingIndexNames = ds.getIndices()
            .stream()
            .map(Index::getName)
            .filter(name -> name.startsWith(".ds-"))
            .collect(Collectors.toList());
        assertThat(
            backingIndexNames,
            containsInAnyOrder(
                Arrays.stream(backingIndices)
                    .map(IndexMetadata::getIndex)
                    .map(Index::getName)
                    .collect(Collectors.toList())
                    .toArray(Strings.EMPTY_ARRAY)
            )
        );
        IndexMetadata zeroIndex = newState.metadata().index(ds.getIndices().get(0));
        assertThat(zeroIndex.getIndex(), equalTo(indexToAdd.getIndex()));
        assertThat(zeroIndex.getSettings().get("index.hidden"), equalTo("true"));
        assertThat(zeroIndex.getAliases().size(), equalTo(0));
    }

    public void testAddRemoveAddRoundtripInSeparateRequests() {
        final long epochMillis = System.currentTimeMillis();
        final int numBackingIndices = randomIntBetween(1, 4);
        final String dataStreamName = randomAlphaOfLength(5);
        IndexMetadata[] backingIndices = new IndexMetadata[numBackingIndices];
        Metadata.Builder mb = Metadata.builder();
        for (int k = 0; k < numBackingIndices; k++) {
            backingIndices[k] = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, k + 1, epochMillis))
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putMapping(generateMapping("@timestamp"))
                .build();
            mb.put(backingIndices[k], false);
        }

        mb.put(
            DataStreamTestHelper.newInstance(
                dataStreamName,
                createTimestampField("@timestamp"),
                Arrays.stream(backingIndices).map(IndexMetadata::getIndex).collect(Collectors.toList())
            )
        );

        final IndexMetadata indexToAdd = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(generateMapping("@timestamp"))
            .build();
        mb.put(indexToAdd, false);

        ClusterState originalState = ClusterState.builder(new ClusterName("dummy")).metadata(mb.build()).build();
        ClusterState newState = MetadataDataStreamsService.modifyDataStream(
            originalState,
            List.of(DataStreamAction.addBackingIndex(dataStreamName, indexToAdd.getIndex().getName())),
            this::getMapperService
        );
        newState = MetadataDataStreamsService.modifyDataStream(
            newState,
            List.of(DataStreamAction.removeBackingIndex(dataStreamName, indexToAdd.getIndex().getName())),
            this::getMapperService
        );
        newState = MetadataDataStreamsService.modifyDataStream(
            newState,
            List.of(DataStreamAction.addBackingIndex(dataStreamName, indexToAdd.getIndex().getName())),
            this::getMapperService
        );

        IndexAbstraction ds = newState.metadata().getIndicesLookup().get(dataStreamName);
        assertThat(ds, notNullValue());
        assertThat(ds.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
        assertThat(ds.getIndices().size(), equalTo(numBackingIndices + 1));
        List<String> backingIndexNames = ds.getIndices()
            .stream()
            .map(Index::getName)
            .filter(x -> x.startsWith(".ds-"))
            .collect(Collectors.toList());
        assertThat(
            backingIndexNames,
            containsInAnyOrder(
                Arrays.stream(backingIndices)
                    .map(IndexMetadata::getIndex)
                    .map(Index::getName)
                    .collect(Collectors.toList())
                    .toArray(Strings.EMPTY_ARRAY)
            )
        );
        IndexMetadata zeroIndex = newState.metadata().index(ds.getIndices().get(0));
        assertThat(zeroIndex.getIndex(), equalTo(indexToAdd.getIndex()));
        assertThat(zeroIndex.getSettings().get("index.hidden"), equalTo("true"));
        assertThat(zeroIndex.getAliases().size(), equalTo(0));
    }

    public void testMissingDataStream() {
        Metadata.Builder mb = Metadata.builder();
        final IndexMetadata indexToAdd = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(generateMapping("@timestamp"))
            .build();
        mb.put(indexToAdd, false);
        final String missingDataStream = randomAlphaOfLength(5);

        ClusterState originalState = ClusterState.builder(new ClusterName("dummy")).metadata(mb.build()).build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(
                originalState,
                List.of(DataStreamAction.addBackingIndex(missingDataStream, indexToAdd.getIndex().getName())),
                this::getMapperService
            )
        );

        assertThat(e.getMessage(), equalTo("data stream [" + missingDataStream + "] not found"));
    }

    public void testMissingIndex() {
        final long epochMillis = System.currentTimeMillis();
        final int numBackingIndices = randomIntBetween(1, 4);
        final String dataStreamName = randomAlphaOfLength(5);
        IndexMetadata[] backingIndices = new IndexMetadata[numBackingIndices];
        Metadata.Builder mb = Metadata.builder();
        for (int k = 0; k < numBackingIndices; k++) {
            backingIndices[k] = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, k + 1, epochMillis))
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putMapping(generateMapping("@timestamp"))
                .build();
            mb.put(backingIndices[k], false);
        }

        mb.put(
            DataStreamTestHelper.newInstance(
                dataStreamName,
                createTimestampField("@timestamp"),
                Arrays.stream(backingIndices).map(IndexMetadata::getIndex).collect(Collectors.toList())
            )
        );

        final String missingIndex = randomAlphaOfLength(5);
        ClusterState originalState = ClusterState.builder(new ClusterName("dummy")).metadata(mb.build()).build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(
                originalState,
                List.of(DataStreamAction.addBackingIndex(dataStreamName, missingIndex)),
                this::getMapperService
            )
        );

        assertThat(e.getMessage(), equalTo("index [" + missingIndex + "] not found"));
    }

    private MapperService getMapperService(IndexMetadata im) {
        try {
            String mapping = im.mapping().source().toString();
            return createMapperService(mapping);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
