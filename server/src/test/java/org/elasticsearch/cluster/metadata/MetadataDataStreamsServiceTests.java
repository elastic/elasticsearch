/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.SnapshotsInProgress;
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
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotInfoTestUtils;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

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
        ProjectMetadata.Builder mb = ProjectMetadata.builder(randomProjectIdOrDefault());
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

        ProjectMetadata originalProject = mb.build();
        ProjectMetadata newProject = MetadataDataStreamsService.modifyDataStream(
            originalProject,
            List.of(DataStreamAction.addBackingIndex(dataStreamName, indexToAdd.getIndex().getName())),
            this::getMapperService,
            Settings.EMPTY
        );

        IndexAbstraction ds = newProject.getIndicesLookup().get(dataStreamName);
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
        IndexMetadata zeroIndex = newProject.index(ds.getIndices().get(0));
        assertThat(zeroIndex.getIndex(), equalTo(indexToAdd.getIndex()));
        assertThat(zeroIndex.getSettings().get("index.hidden"), equalTo("true"));
        assertThat(zeroIndex.isSystem(), equalTo(false));
        assertThat(zeroIndex.getAliases().size(), equalTo(0));
    }

    public void testAddBackingIndexToSystemDataStream() {
        final long epochMillis = System.currentTimeMillis();
        final int numBackingIndices = randomIntBetween(1, 4);
        final String dataStreamName = randomAlphaOfLength(5);
        IndexMetadata[] backingIndices = new IndexMetadata[numBackingIndices];
        ProjectMetadata.Builder mb = ProjectMetadata.builder(randomProjectIdOrDefault());
        for (int k = 0; k < numBackingIndices; k++) {
            backingIndices[k] = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, k + 1, epochMillis))
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putMapping(generateMapping("@timestamp"))
                .system(true)
                .build();
            mb.put(backingIndices[k], false);
        }

        DataStream dataStream = DataStream.builder(dataStreamName, Arrays.stream(backingIndices).map(IndexMetadata::getIndex).toList())
            .setSystem(true)
            .setHidden(true)
            .build();
        mb.put(dataStream);

        final IndexMetadata indexToAdd = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(generateMapping("@timestamp"))
            .system(false)
            .build();
        mb.put(indexToAdd, false);

        ProjectMetadata projectMetadata = mb.build();
        ProjectMetadata newState = MetadataDataStreamsService.modifyDataStream(
            projectMetadata,
            List.of(DataStreamAction.addBackingIndex(dataStreamName, indexToAdd.getIndex().getName())),
            this::getMapperService,
            Settings.EMPTY
        );

        IndexAbstraction ds = newState.getIndicesLookup().get(dataStreamName);
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
        IndexMetadata zeroIndex = newState.index(ds.getIndices().get(0));
        assertThat(zeroIndex.getIndex(), equalTo(indexToAdd.getIndex()));
        assertThat(zeroIndex.getSettings().get("index.hidden"), equalTo("true"));
        assertThat(zeroIndex.isSystem(), equalTo(true));
        assertThat(zeroIndex.getAliases().size(), equalTo(0));
    }

    public void testRemoveBackingIndex() {
        final long epochMillis = System.currentTimeMillis();
        final int numBackingIndices = randomIntBetween(2, 4);
        final String dataStreamName = randomAlphaOfLength(5);
        IndexMetadata[] backingIndices = new IndexMetadata[numBackingIndices];
        ProjectMetadata.Builder mb = ProjectMetadata.builder(randomProjectIdOrDefault());
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
        ProjectMetadata originalProject = mb.build();
        ProjectMetadata newProject = MetadataDataStreamsService.modifyDataStream(
            originalProject,
            List.of(DataStreamAction.removeBackingIndex(dataStreamName, indexToRemove.getIndex().getName())),
            this::getMapperService,
            Settings.EMPTY
        );

        IndexAbstraction ds = newProject.getIndicesLookup().get(dataStreamName);
        assertThat(ds, notNullValue());
        assertThat(ds.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
        assertThat(ds.getIndices().size(), equalTo(numBackingIndices - 1));

        List<Index> expectedBackingIndices = ds.getIndices()
            .stream()
            .filter(x -> x.getName().equals(indexToRemove.getIndex().getName()) == false)
            .toList();
        assertThat(expectedBackingIndices, containsInAnyOrder(ds.getIndices().toArray()));

        IndexMetadata removedIndex = newProject.indices().get(indexToRemove.getIndex().getName());
        assertThat(removedIndex, notNullValue());
        assertThat(removedIndex.getSettings().get("index.hidden"), equalTo("false"));
        assertNull(newProject.getIndicesLookup().get(indexToRemove.getIndex().getName()).getParentDataStream());
    }

    public void testRemoveWriteIndexIsProhibited() {
        final long epochMillis = System.currentTimeMillis();
        final int numBackingIndices = randomIntBetween(1, 4);
        final String dataStreamName = randomAlphaOfLength(5);
        IndexMetadata[] backingIndices = new IndexMetadata[numBackingIndices];
        ProjectMetadata.Builder mb = ProjectMetadata.builder(randomProjectIdOrDefault());
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
        ProjectMetadata originalProject = mb.build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(
                originalProject,
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
        ProjectMetadata.Builder mb = ProjectMetadata.builder(randomProjectIdOrDefault());
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

        ProjectMetadata originalProject = mb.build();
        ProjectMetadata newProject = MetadataDataStreamsService.modifyDataStream(
            originalProject,
            List.of(
                DataStreamAction.addBackingIndex(dataStreamName, indexToAdd.getIndex().getName()),
                DataStreamAction.removeBackingIndex(dataStreamName, indexToAdd.getIndex().getName()),
                DataStreamAction.addBackingIndex(dataStreamName, indexToAdd.getIndex().getName())
            ),
            this::getMapperService,
            Settings.EMPTY
        );

        IndexAbstraction ds = newProject.getIndicesLookup().get(dataStreamName);
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
        IndexMetadata zeroIndex = newProject.index(ds.getIndices().get(0));
        assertThat(zeroIndex.getIndex(), equalTo(indexToAdd.getIndex()));
        assertThat(zeroIndex.getSettings().get("index.hidden"), equalTo("true"));
        assertThat(zeroIndex.getAliases().size(), equalTo(0));
    }

    public void testAddRemoveAddRoundtripInSeparateRequests() {
        final long epochMillis = System.currentTimeMillis();
        final int numBackingIndices = randomIntBetween(1, 4);
        final String dataStreamName = randomAlphaOfLength(5);
        IndexMetadata[] backingIndices = new IndexMetadata[numBackingIndices];
        ProjectMetadata.Builder mb = ProjectMetadata.builder(randomProjectIdOrDefault());
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

        ProjectMetadata originalProject = mb.build();
        ProjectMetadata newProject = MetadataDataStreamsService.modifyDataStream(
            originalProject,
            List.of(DataStreamAction.addBackingIndex(dataStreamName, indexToAdd.getIndex().getName())),
            this::getMapperService,
            Settings.EMPTY
        );
        newProject = MetadataDataStreamsService.modifyDataStream(
            newProject,
            List.of(DataStreamAction.removeBackingIndex(dataStreamName, indexToAdd.getIndex().getName())),
            this::getMapperService,
            Settings.EMPTY
        );
        newProject = MetadataDataStreamsService.modifyDataStream(
            newProject,
            List.of(DataStreamAction.addBackingIndex(dataStreamName, indexToAdd.getIndex().getName())),
            this::getMapperService,
            Settings.EMPTY
        );

        IndexAbstraction ds = newProject.getIndicesLookup().get(dataStreamName);
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
        IndexMetadata zeroIndex = newProject.index(ds.getIndices().get(0));
        assertThat(zeroIndex.getIndex(), equalTo(indexToAdd.getIndex()));
        assertThat(zeroIndex.getSettings().get("index.hidden"), equalTo("true"));
        assertThat(zeroIndex.getAliases().size(), equalTo(0));
    }

    public void testMissingDataStream() {
        ProjectMetadata.Builder mb = ProjectMetadata.builder(randomProjectIdOrDefault());
        final IndexMetadata indexToAdd = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping(generateMapping("@timestamp"))
            .build();
        mb.put(indexToAdd, false);
        final String missingDataStream = randomAlphaOfLength(5);

        ProjectMetadata originalProject = mb.build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(
                originalProject,
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
        ProjectMetadata.Builder mb = ProjectMetadata.builder(randomProjectIdOrDefault());
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
        ProjectMetadata originalProject = mb.build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(
                originalProject,
                List.of(DataStreamAction.addBackingIndex(dataStreamName, missingIndex)),
                this::getMapperService,
                Settings.EMPTY
            )
        );

        assertThat(e.getMessage(), equalTo("index [" + missingIndex + "] not found"));
    }

    public void testRemoveBrokenBackingIndexReference() {
        var dataStreamName = "my-logs";
        var project = DataStreamTestHelper.getProjectWithDataStreams(List.of(new Tuple<>(dataStreamName, 2)), List.of());
        var originalDs = project.dataStreams().get(dataStreamName);
        var broken = originalDs.copy()
            .setBackingIndices(
                originalDs.getDataComponent()
                    .copy()
                    .setIndices(List.of(new Index(originalDs.getIndices().get(0).getName(), "broken"), originalDs.getIndices().get(1)))
                    .build()
            )
            .build();
        var brokenProject = ProjectMetadata.builder(project).put(broken).build();

        var result = MetadataDataStreamsService.modifyDataStream(
            brokenProject,
            List.of(DataStreamAction.removeBackingIndex(dataStreamName, broken.getIndices().get(0).getName())),
            this::getMapperService,
            Settings.EMPTY
        );
        assertThat(result.dataStreams().get(dataStreamName).getIndices(), hasSize(1));
        assertThat(result.dataStreams().get(dataStreamName).getIndices().get(0), equalTo(originalDs.getIndices().get(1)));
    }

    public void testRemoveBackingIndexThatDoesntExist() {
        var dataStreamName = "my-logs";
        var project = DataStreamTestHelper.getProjectWithDataStreams(List.of(new Tuple<>(dataStreamName, 2)), List.of());

        String indexToRemove = DataStream.getDefaultBackingIndexName(dataStreamName, 3);
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataDataStreamsService.modifyDataStream(
                project,
                List.of(DataStreamAction.removeBackingIndex(dataStreamName, indexToRemove)),
                this::getMapperService,
                Settings.EMPTY
            )
        );
        assertThat(e.getMessage(), equalTo("index [" + indexToRemove + "] not found"));
    }

    public void testUpdateLifecycle() {
        String dataStream = randomAlphaOfLength(5);
        DataStreamLifecycle lifecycle = DataStreamLifecycle.dataLifecycleBuilder().dataRetention(randomPositiveTimeValue()).build();
        ProjectMetadata before = DataStreamTestHelper.getProjectWithDataStreams(List.of(new Tuple<>(dataStream, 2)), List.of());
        MetadataDataStreamsService service = new MetadataDataStreamsService(
            mock(ClusterService.class),
            mock(IndicesService.class),
            DataStreamGlobalRetentionSettings.create(ClusterSettings.createBuiltInClusterSettings())
        );
        {
            // Remove lifecycle
            ProjectMetadata after = service.updateDataLifecycle(before, List.of(dataStream), null);
            DataStream updatedDataStream = after.dataStreams().get(dataStream);
            assertNotNull(updatedDataStream);
            assertThat(updatedDataStream.getDataLifecycle(), nullValue());
            before = after;
        }

        {
            // Set lifecycle
            ProjectMetadata after = service.updateDataLifecycle(before, List.of(dataStream), lifecycle);
            DataStream updatedDataStream = after.dataStreams().get(dataStream);
            assertNotNull(updatedDataStream);
            assertThat(updatedDataStream.getDataLifecycle(), equalTo(lifecycle));
        }
    }

    public void testUpdateDataStreamOptions() {
        String dataStream = randomAlphaOfLength(5);
        // we want the data stream options to be non-empty, so we can see the removal in action
        DataStreamOptions dataStreamOptions = randomValueOtherThan(
            DataStreamOptions.EMPTY,
            DataStreamOptionsTests::randomDataStreamOptions
        );
        ProjectMetadata before = DataStreamTestHelper.getProjectWithDataStreams(List.of(new Tuple<>(dataStream, 2)), List.of());
        MetadataDataStreamsService service = new MetadataDataStreamsService(
            mock(ClusterService.class),
            mock(IndicesService.class),
            DataStreamGlobalRetentionSettings.create(ClusterSettings.createBuiltInClusterSettings())
        );

        // Ensure no data stream options are stored
        DataStream updatedDataStream = before.dataStreams().get(dataStream);
        assertNotNull(updatedDataStream);
        assertThat(updatedDataStream.getDataStreamOptions(), equalTo(DataStreamOptions.EMPTY));

        // Set non-empty data stream options
        ProjectMetadata after = service.updateDataStreamOptions(before, List.of(dataStream), dataStreamOptions);
        updatedDataStream = after.dataStreams().get(dataStream);
        assertNotNull(updatedDataStream);
        assertThat(updatedDataStream.getDataStreamOptions(), equalTo(dataStreamOptions));
        before = after;

        // Remove data stream options
        after = service.updateDataStreamOptions(before, List.of(dataStream), null);
        updatedDataStream = after.dataStreams().get(dataStream);
        assertNotNull(updatedDataStream);
        assertThat(updatedDataStream.getDataStreamOptions(), equalTo(DataStreamOptions.EMPTY));
    }

    public void testDeleteMissing() {
        DataStream dataStream = DataStreamTestHelper.randomInstance();
        final var projectId = randomProjectIdOrDefault();
        ProjectState state = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(projectId))
            .build()
            .projectState(projectId);

        ResourceNotFoundException e = expectThrows(
            ResourceNotFoundException.class,
            () -> MetadataDataStreamsService.deleteDataStreams(state, Set.of(dataStream), Settings.EMPTY)
        );
        assertThat(e.getMessage(), containsString(dataStream.getName()));
    }

    public void testDeleteSnapshotting() {
        String dataStreamName = randomAlphaOfLength(5);
        Snapshot snapshot = new Snapshot("doesn't matter", new SnapshotId("snapshot name", "snapshot uuid"));
        SnapshotsInProgress snaps = SnapshotsInProgress.EMPTY.withAddedEntry(
            SnapshotsInProgress.Entry.snapshot(
                snapshot,
                true,
                false,
                SnapshotsInProgress.State.INIT,
                Collections.emptyMap(),
                List.of(dataStreamName),
                Collections.emptyList(),
                System.currentTimeMillis(),
                (long) randomIntBetween(0, 1000),
                Map.of(),
                null,
                SnapshotInfoTestUtils.randomUserMetadata(),
                IndexVersionUtils.randomVersion()
            )
        );
        final DataStream dataStream = DataStreamTestHelper.randomInstance(dataStreamName);
        var projectId = randomProjectIdOrDefault();
        ProjectState state = ClusterState.builder(ClusterName.DEFAULT)
            .putCustom(SnapshotsInProgress.TYPE, snaps)
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(dataStream))
            .build()
            .projectState(projectId);
        Exception e = expectThrows(
            SnapshotInProgressException.class,
            () -> MetadataDataStreamsService.deleteDataStreams(state, Set.of(dataStream), Settings.EMPTY)
        );
        assertEquals(
            "Cannot delete data streams that are being snapshotted: ["
                + dataStreamName
                + "]. Try again after snapshot finishes "
                + "or cancel the currently running snapshot.",
            e.getMessage()
        );
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
