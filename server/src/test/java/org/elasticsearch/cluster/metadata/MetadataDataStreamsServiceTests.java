/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.indices.IndicesService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.generateMapping;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class MetadataDataStreamsServiceTests extends MapperServiceTestCase {

    public void testAddBackingIndex() {
        final long epochMillis = System.currentTimeMillis();
        final int numBackingIndices = randomIntBetween(1, 4);
        final String dataStreamName = randomAlphaOfLength(5);
        IndexMetadata[] backingIndices = new IndexMetadata[numBackingIndices];
        Metadata.Builder mb = Metadata.builder();
        for (int k = 0; k < numBackingIndices; k++) {
            backingIndices[k] = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, k + 1, epochMillis))
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putMapping(generateMapping("@timestamp"))
                .build();
            mb.put(backingIndices[k], false);
        }

        mb.put(DataStreamTestHelper.newInstance(dataStreamName, Arrays.stream(backingIndices).map(IndexMetadata::getIndex).toList()));

        final IndexMetadata indexToAdd = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(generateMapping("@timestamp"))
            .build();
        mb.put(indexToAdd, false);

        ClusterState originalState = ClusterState.builder(new ClusterName("dummy")).metadata(mb.build()).build();
        ClusterState newState = MetadataDataStreamsService.modifyDataStream(
            originalState,
            List.of(DataStreamAction.addBackingIndex(dataStreamName, indexToAdd.getIndex().getName())),
            this::getMapperService,
            Settings.EMPTY
        );

        IndexAbstraction ds = newState.metadata().getIndicesLookup().get(dataStreamName);
        assertThat(ds, notNullValue());
        assertThat(ds.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
        assertThat(ds.getIndices().size(), equalTo(numBackingIndices + 1));
        List<String> backingIndexNames = ds.getIndices().stream().filter(x -> x.getName().startsWith(".ds-")).map(Index::getName).toList();
        assertThat(
            backingIndexNames,
            containsInAnyOrder(
                Arrays.stream(backingIndices).map(IndexMetadata::getIndex).map(Index::getName).toList().toArray(Strings.EMPTY_ARRAY)
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
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putMapping(generateMapping("@timestamp"))
                .build();
            mb.put(backingIndices[k], false);
        }

        mb.put(DataStreamTestHelper.newInstance(dataStreamName, Arrays.stream(backingIndices).map(IndexMetadata::getIndex).toList()));

        final IndexMetadata indexToRemove = backingIndices[randomIntBetween(0, numBackingIndices - 2)];
        ClusterState originalState = ClusterState.builder(new ClusterName("dummy")).metadata(mb.build()).build();
        ClusterState newState = MetadataDataStreamsService.modifyDataStream(
            originalState,
            List.of(DataStreamAction.removeBackingIndex(dataStreamName, indexToRemove.getIndex().getName())),
            this::getMapperService,
            Settings.EMPTY
        );

        IndexAbstraction ds = newState.metadata().getIndicesLookup().get(dataStreamName);
        assertThat(ds, notNullValue());
        assertThat(ds.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
        assertThat(ds.getIndices().size(), equalTo(numBackingIndices - 1));

        List<Index> expectedBackingIndices = ds.getIndices()
            .stream()
            .filter(x -> x.getName().equals(indexToRemove.getIndex().getName()) == false)
            .toList();
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
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putMapping(generateMapping("@timestamp"))
                .build();
            mb.put(backingIndices[k], false);
        }

        mb.put(DataStreamTestHelper.newInstance(dataStreamName, Arrays.stream(backingIndices).map(IndexMetadata::getIndex).toList()));

        final IndexMetadata indexToRemove = backingIndices[numBackingIndices - 1];
        ClusterState originalState = ClusterState.builder(new ClusterName("dummy")).metadata(mb.build()).build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(
                originalState,
                List.of(DataStreamAction.removeBackingIndex(dataStreamName, indexToRemove.getIndex().getName())),
                this::getMapperService,
                Settings.EMPTY
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
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putMapping(generateMapping("@timestamp"))
                .build();
            mb.put(backingIndices[k], false);
        }

        mb.put(DataStreamTestHelper.newInstance(dataStreamName, Arrays.stream(backingIndices).map(IndexMetadata::getIndex).toList()));

        final IndexMetadata indexToAdd = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
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
            this::getMapperService,
            Settings.EMPTY
        );

        IndexAbstraction ds = newState.metadata().getIndicesLookup().get(dataStreamName);
        assertThat(ds, notNullValue());
        assertThat(ds.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
        assertThat(ds.getIndices().size(), equalTo(numBackingIndices + 1));
        List<String> backingIndexNames = ds.getIndices().stream().map(Index::getName).filter(name -> name.startsWith(".ds-")).toList();
        assertThat(
            backingIndexNames,
            containsInAnyOrder(
                Arrays.stream(backingIndices).map(IndexMetadata::getIndex).map(Index::getName).toList().toArray(Strings.EMPTY_ARRAY)
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
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putMapping(generateMapping("@timestamp"))
                .build();
            mb.put(backingIndices[k], false);
        }

        mb.put(DataStreamTestHelper.newInstance(dataStreamName, Arrays.stream(backingIndices).map(IndexMetadata::getIndex).toList()));

        final IndexMetadata indexToAdd = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(generateMapping("@timestamp"))
            .build();
        mb.put(indexToAdd, false);

        ClusterState originalState = ClusterState.builder(new ClusterName("dummy")).metadata(mb.build()).build();
        ClusterState newState = MetadataDataStreamsService.modifyDataStream(
            originalState,
            List.of(DataStreamAction.addBackingIndex(dataStreamName, indexToAdd.getIndex().getName())),
            this::getMapperService,
            Settings.EMPTY
        );
        newState = MetadataDataStreamsService.modifyDataStream(
            newState,
            List.of(DataStreamAction.removeBackingIndex(dataStreamName, indexToAdd.getIndex().getName())),
            this::getMapperService,
            Settings.EMPTY
        );
        newState = MetadataDataStreamsService.modifyDataStream(
            newState,
            List.of(DataStreamAction.addBackingIndex(dataStreamName, indexToAdd.getIndex().getName())),
            this::getMapperService,
            Settings.EMPTY
        );

        IndexAbstraction ds = newState.metadata().getIndicesLookup().get(dataStreamName);
        assertThat(ds, notNullValue());
        assertThat(ds.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
        assertThat(ds.getIndices().size(), equalTo(numBackingIndices + 1));
        List<String> backingIndexNames = ds.getIndices().stream().map(Index::getName).filter(x -> x.startsWith(".ds-")).toList();
        assertThat(
            backingIndexNames,
            containsInAnyOrder(
                Arrays.stream(backingIndices).map(IndexMetadata::getIndex).map(Index::getName).toList().toArray(Strings.EMPTY_ARRAY)
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
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
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
                this::getMapperService,
                Settings.EMPTY
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
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putMapping(generateMapping("@timestamp"))
                .build();
            mb.put(backingIndices[k], false);
        }

        mb.put(DataStreamTestHelper.newInstance(dataStreamName, Arrays.stream(backingIndices).map(IndexMetadata::getIndex).toList()));

        final String missingIndex = randomAlphaOfLength(5);
        ClusterState originalState = ClusterState.builder(new ClusterName("dummy")).metadata(mb.build()).build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(
                originalState,
                List.of(DataStreamAction.addBackingIndex(dataStreamName, missingIndex)),
                this::getMapperService,
                Settings.EMPTY
            )
        );

        assertThat(e.getMessage(), equalTo("index [" + missingIndex + "] not found"));
    }

    public void testRemoveBrokenBackingIndexReference() {
        var dataStreamName = "my-logs";
        var state = DataStreamTestHelper.getClusterStateWithDataStreams(List.of(new Tuple<>(dataStreamName, 2)), List.of());
        var original = state.getMetadata().dataStreams().get(dataStreamName);
        var broken = original.copy()
            .setBackingIndices(
                original.getBackingIndices()
                    .copy()
                    .setIndices(List.of(new Index(original.getIndices().get(0).getName(), "broken"), original.getIndices().get(1)))
                    .build()
            )
            .build();
        var brokenState = ClusterState.builder(state).metadata(Metadata.builder(state.getMetadata()).put(broken).build()).build();

        var result = MetadataDataStreamsService.modifyDataStream(
            brokenState,
            List.of(DataStreamAction.removeBackingIndex(dataStreamName, broken.getIndices().get(0).getName())),
            this::getMapperService,
            Settings.EMPTY
        );
        assertThat(result.getMetadata().dataStreams().get(dataStreamName).getIndices(), hasSize(1));
        assertThat(result.getMetadata().dataStreams().get(dataStreamName).getIndices().get(0), equalTo(original.getIndices().get(1)));
    }

    public void testRemoveBackingIndexThatDoesntExist() {
        var dataStreamName = "my-logs";
        var state = DataStreamTestHelper.getClusterStateWithDataStreams(List.of(new Tuple<>(dataStreamName, 2)), List.of());

        String indexToRemove = DataStream.getDefaultBackingIndexName(dataStreamName, 3);
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(
                state,
                List.of(DataStreamAction.removeBackingIndex(dataStreamName, indexToRemove)),
                this::getMapperService,
                Settings.EMPTY
            )
        );
        assertThat(e.getMessage(), equalTo("index [" + indexToRemove + "] not found"));
    }

    public void testUpdateLifecycle() {
        String dataStream = randomAlphaOfLength(5);
        DataStreamLifecycle lifecycle = DataStreamLifecycle.newBuilder().dataRetention(randomMillisUpToYear9999()).build();
        ClusterState before = DataStreamTestHelper.getClusterStateWithDataStreams(List.of(new Tuple<>(dataStream, 2)), List.of());
        MetadataDataStreamsService service = new MetadataDataStreamsService(
            mock(ClusterService.class),
            mock(IndicesService.class),
            DataStreamGlobalRetentionSettings.create(
                ClusterSettings.createBuiltInClusterSettings(),
                DataStreamFactoryRetention.emptyFactoryRetention()
            )
        );
        {
            // Remove lifecycle
            ClusterState after = service.updateDataLifecycle(before, List.of(dataStream), null);
            DataStream updatedDataStream = after.metadata().dataStreams().get(dataStream);
            assertNotNull(updatedDataStream);
            assertThat(updatedDataStream.getLifecycle(), nullValue());
            before = after;
        }

        {
            // Set lifecycle
            ClusterState after = service.updateDataLifecycle(before, List.of(dataStream), lifecycle);
            DataStream updatedDataStream = after.metadata().dataStreams().get(dataStream);
            assertNotNull(updatedDataStream);
            assertThat(updatedDataStream.getLifecycle(), equalTo(lifecycle));
        }
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
