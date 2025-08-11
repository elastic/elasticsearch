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
import org.elasticsearch.cluster.metadata.DataStream.TimestampField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createTimestampField;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.generateMapping;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class MetadataDataStreamsServiceTests extends MapperServiceTestCase {

    public void testAddBackingIndex() throws Exception {
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
                .putMapping("_doc", generateMapping("@timestamp"))
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
            .putMapping("_doc", generateMapping("@timestamp"))
            .build();
        mb.put(indexToAdd, false);

        ClusterState originalState = ClusterState.builder(new ClusterName("dummy")).metadata(mb.build()).build();
        ClusterState newState = MetadataDataStreamsService.modifyDataStream(
            originalState,
            org.elasticsearch.core.List.of(DataStreamAction.addBackingIndex(dataStreamName, indexToAdd.getIndex().getName())),
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

    public void testRemoveBackingIndex() throws Exception {
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
                .putMapping("_doc", generateMapping("@timestamp"))
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
            org.elasticsearch.core.List.of(DataStreamAction.removeBackingIndex(dataStreamName, indexToRemove.getIndex().getName())),
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

    public void testRemoveWriteIndexIsProhibited() throws Exception {
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
                .putMapping("_doc", generateMapping("@timestamp"))
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
                org.elasticsearch.core.List.of(DataStreamAction.removeBackingIndex(dataStreamName, indexToRemove.getIndex().getName())),
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

    public void testAddRemoveAddRoundtripInSingleRequest() throws Exception {
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
                .putMapping("_doc", generateMapping("@timestamp"))
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
            .putMapping("_doc", generateMapping("@timestamp"))
            .build();
        mb.put(indexToAdd, false);

        ClusterState originalState = ClusterState.builder(new ClusterName("dummy")).metadata(mb.build()).build();
        ClusterState newState = MetadataDataStreamsService.modifyDataStream(
            originalState,
            org.elasticsearch.core.List.of(
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

    public void testAddRemoveAddRoundtripInSeparateRequests() throws Exception {
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
                .putMapping("_doc", generateMapping("@timestamp"))
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
            .putMapping("_doc", generateMapping("@timestamp"))
            .build();
        mb.put(indexToAdd, false);

        ClusterState originalState = ClusterState.builder(new ClusterName("dummy")).metadata(mb.build()).build();
        ClusterState newState = MetadataDataStreamsService.modifyDataStream(
            originalState,
            org.elasticsearch.core.List.of(DataStreamAction.addBackingIndex(dataStreamName, indexToAdd.getIndex().getName())),
            this::getMapperService
        );
        newState = MetadataDataStreamsService.modifyDataStream(
            newState,
            org.elasticsearch.core.List.of(DataStreamAction.removeBackingIndex(dataStreamName, indexToAdd.getIndex().getName())),
            this::getMapperService
        );
        newState = MetadataDataStreamsService.modifyDataStream(
            newState,
            org.elasticsearch.core.List.of(DataStreamAction.addBackingIndex(dataStreamName, indexToAdd.getIndex().getName())),
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

    public void testMissingDataStream() throws Exception {
        Metadata.Builder mb = Metadata.builder();
        final IndexMetadata indexToAdd = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping("_doc", generateMapping("@timestamp"))
            .build();
        mb.put(indexToAdd, false);
        final String missingDataStream = randomAlphaOfLength(5);

        ClusterState originalState = ClusterState.builder(new ClusterName("dummy")).metadata(mb.build()).build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(
                originalState,
                org.elasticsearch.core.List.of(DataStreamAction.addBackingIndex(missingDataStream, indexToAdd.getIndex().getName())),
                this::getMapperService
            )
        );

        assertThat(e.getMessage(), equalTo("data stream [" + missingDataStream + "] not found"));
    }

    public void testMissingIndex() throws Exception {
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
                .putMapping("_doc", generateMapping("@timestamp"))
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
                org.elasticsearch.core.List.of(DataStreamAction.addBackingIndex(dataStreamName, missingIndex)),
                this::getMapperService
            )
        );

        assertThat(e.getMessage(), equalTo("index [" + missingIndex + "] not found"));
    }

    public void testRemoveBrokenBackingIndexReference() {
        String dataStreamName = "my-logs";
        ClusterState state = DataStreamTestHelper.getClusterStateWithDataStreams(
            org.elasticsearch.core.List.of(new Tuple<>(dataStreamName, 2)),
            org.elasticsearch.core.List.of()
        );
        DataStream original = state.getMetadata().dataStreams().get(dataStreamName);
        DataStream broken = new DataStream(
            original.getName(),
            new TimestampField("@timestamp"),
            org.elasticsearch.core.List.of(new Index(original.getIndices().get(0).getName(), "broken"), original.getIndices().get(1)),
            original.getGeneration(),
            original.getMetadata(),
            original.isHidden(),
            original.isReplicated(),
            original.isSystem()
        );
        ClusterState brokenState = ClusterState.builder(state).metadata(Metadata.builder(state.getMetadata()).put(broken).build()).build();

        ClusterState result = MetadataDataStreamsService.modifyDataStream(
            brokenState,
            org.elasticsearch.core.List.of(DataStreamAction.removeBackingIndex(dataStreamName, broken.getIndices().get(0).getName())),
            this::getMapperService
        );
        assertThat(result.getMetadata().dataStreams().get(dataStreamName).getIndices(), hasSize(1));
        assertThat(result.getMetadata().dataStreams().get(dataStreamName).getIndices().get(0), equalTo(original.getIndices().get(1)));
    }

    public void testRemoveBackingIndexThatDoesntExist() {
        String dataStreamName = "my-logs";
        ClusterState state = DataStreamTestHelper.getClusterStateWithDataStreams(
            org.elasticsearch.core.List.of(new Tuple<>(dataStreamName, 2)),
            org.elasticsearch.core.List.of()
        );

        String indexToRemove = DataStream.getDefaultBackingIndexName(dataStreamName, 3);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(
                state,
                org.elasticsearch.core.List.of(DataStreamAction.removeBackingIndex(dataStreamName, indexToRemove)),
                this::getMapperService
            )
        );
        assertThat(e.getMessage(), equalTo("index [" + indexToRemove + "] not found"));
    }

    private MapperService getMapperService(IndexMetadata im) {
        try {
            String mapping = im.mapping().source().toString();
            return createMapperService("_doc", mapping);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return org.elasticsearch.core.List.of(new MetadataIndexTemplateServiceTests.DummyPlugin());
    }
}
