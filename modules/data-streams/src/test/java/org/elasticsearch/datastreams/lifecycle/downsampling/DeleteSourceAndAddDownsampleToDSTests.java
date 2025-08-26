/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.downsampling;

import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.datastreams.DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleFixtures.createDataStream;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DeleteSourceAndAddDownsampleToDSTests extends ESTestCase {

    private long now;

    @Before
    public void refreshNow() {
        now = System.currentTimeMillis();
    }

    public void testDownsampleIndexMissingIsNoOp() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.MAX_VALUE).build(),
            now
        );
        builder.put(dataStream);
        ClusterState previousState = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();

        String firstGeneration = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        ClusterState newState = new DeleteSourceAndAddDownsampleToDS(
            Settings.EMPTY,
            dataStreamName,
            firstGeneration,
            "downsample-1s-" + firstGeneration,
            null
        ).execute(previousState);

        assertThat(newState, is(previousState));
    }

    public void testDownsampleIsAddedToDSEvenIfSourceDeleted() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.MAX_VALUE).build(),
            now
        );
        String firstGenIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        String downsampleIndex = "downsample-1s-" + firstGenIndex;
        IndexMetadata.Builder downsampleIndexMeta = IndexMetadata.builder(downsampleIndex)
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0);
        builder.put(downsampleIndexMeta);
        // let's remove the first generation index
        dataStream = dataStream.removeBackingIndex(builder.get(firstGenIndex).getIndex());
        // delete the first gen altogether
        builder.remove(firstGenIndex);
        builder.put(dataStream);
        ClusterState previousState = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();

        ClusterState newState = new DeleteSourceAndAddDownsampleToDS(Settings.EMPTY, dataStreamName, firstGenIndex, downsampleIndex, null)
            .execute(previousState);

        IndexAbstraction downsampleIndexAbstraction = newState.metadata().getIndicesLookup().get(downsampleIndex);
        assertThat(downsampleIndexAbstraction, is(notNullValue()));
        assertThat(downsampleIndexAbstraction.getParentDataStream(), is(notNullValue()));
        // the downsample index is part of the data stream
        assertThat(downsampleIndexAbstraction.getParentDataStream().getName(), is(dataStreamName));
    }

    public void testSourceIndexIsWriteIndexThrowsException() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.MAX_VALUE).build(),
            now
        );
        builder.put(dataStream);
        String writeIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 3);
        String downsampleIndex = "downsample-1s-" + writeIndex;
        IndexMetadata.Builder downsampleIndexMeta = IndexMetadata.builder(downsampleIndex)
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0);
        builder.put(downsampleIndexMeta);
        ClusterState previousState = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();

        IllegalStateException illegalStateException = expectThrows(
            IllegalStateException.class,
            () -> new DeleteSourceAndAddDownsampleToDS(Settings.EMPTY, dataStreamName, writeIndex, downsampleIndex, null).execute(
                previousState
            )
        );

        assertThat(
            illegalStateException.getMessage(),
            is("index [" + writeIndex + "] is the write index for data stream [" + dataStreamName + "] and " + "cannot be replaced")
        );
    }

    public void testSourceIsDeleteAndDownsampleOriginationDateIsConfigured() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.MAX_VALUE).build(),
            now
        );
        String firstGenIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        String downsampleIndex = "downsample-1s-" + firstGenIndex;
        IndexMetadata.Builder downsampleIndexMeta = IndexMetadata.builder(downsampleIndex)
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0);
        builder.put(downsampleIndexMeta);
        builder.put(dataStream);
        ClusterState previousState = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();

        // let's add some lifecycle custom metadata to the first generation index
        IndexMetadata indexMetadata = previousState.metadata().index(firstGenIndex);
        RolloverInfo rolloverInfo = indexMetadata.getRolloverInfos().get(dataStreamName);

        IndexMetadata.Builder firstGenBuilder = IndexMetadata.builder(indexMetadata)
            .putCustom(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, Map.of(FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY, String.valueOf(now)));
        Metadata.Builder metaBuilder = Metadata.builder(previousState.metadata()).put(firstGenBuilder);
        previousState = ClusterState.builder(previousState).metadata(metaBuilder).build();
        ClusterState newState = new DeleteSourceAndAddDownsampleToDS(Settings.EMPTY, dataStreamName, firstGenIndex, downsampleIndex, null)
            .execute(previousState);

        IndexAbstraction downsampleIndexAbstraction = newState.metadata().getIndicesLookup().get(downsampleIndex);
        assertThat(downsampleIndexAbstraction, is(notNullValue()));
        assertThat(downsampleIndexAbstraction.getParentDataStream(), is(notNullValue()));
        // the downsample index is part of the data stream
        assertThat(downsampleIndexAbstraction.getParentDataStream().getName(), is(dataStreamName));

        // the source index is deleted
        IndexAbstraction sourceIndexAbstraction = newState.metadata().getIndicesLookup().get(firstGenIndex);
        assertThat(sourceIndexAbstraction, is(nullValue()));

        // let's check the downsample index has the origination date configured to the source index rollover time
        IndexMetadata downsampleMeta = newState.metadata().index(downsampleIndex);
        assertThat(IndexSettings.LIFECYCLE_ORIGINATION_DATE_SETTING.get(downsampleMeta.getSettings()), is(rolloverInfo.getTime()));
        assertThat(downsampleMeta.getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY), notNullValue());
        assertThat(
            downsampleMeta.getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY).get(FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY),
            is(String.valueOf(now))
        );
    }

    public void testSourceWithoutLifecycleMetaAndDestWithOriginationDateAlreadyConfigured() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.MAX_VALUE).build(),
            now
        );
        String firstGenIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        String downsampleIndex = "downsample-1s-" + firstGenIndex;
        long downsampleOriginationDate = now - randomLongBetween(10_000, 12_000);
        IndexMetadata.Builder downsampleIndexMeta = IndexMetadata.builder(downsampleIndex)
            .settings(
                settings(IndexVersion.current()).put(IndexSettings.LIFECYCLE_ORIGINATION_DATE_SETTING.getKey(), downsampleOriginationDate)
            )
            .numberOfShards(1)
            .numberOfReplicas(0);
        builder.put(downsampleIndexMeta);
        builder.put(dataStream);
        ClusterState previousState = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();

        ClusterState newState = new DeleteSourceAndAddDownsampleToDS(Settings.EMPTY, dataStreamName, firstGenIndex, downsampleIndex, null)
            .execute(previousState);

        IndexAbstraction downsampleIndexAbstraction = newState.metadata().getIndicesLookup().get(downsampleIndex);
        assertThat(downsampleIndexAbstraction, is(notNullValue()));
        assertThat(downsampleIndexAbstraction.getParentDataStream(), is(notNullValue()));
        // the downsample index is part of the data stream
        assertThat(downsampleIndexAbstraction.getParentDataStream().getName(), is(dataStreamName));

        // the source index was deleted
        IndexAbstraction sourceIndexAbstraction = newState.metadata().getIndicesLookup().get(firstGenIndex);
        assertThat(sourceIndexAbstraction, is(nullValue()));

        IndexMetadata downsampleMeta = newState.metadata().index(downsampleIndex);
        assertThat(IndexSettings.LIFECYCLE_ORIGINATION_DATE_SETTING.get(downsampleMeta.getSettings()), is(downsampleOriginationDate));
    }

    public void testSourceIndexIsDeleteEvenIfNotPartOfDSAnymore() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.MAX_VALUE).build(),
            now
        );
        String firstGenIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        String downsampleIndex = "downsample-1s-" + firstGenIndex;
        IndexMetadata.Builder downsampleIndexMeta = IndexMetadata.builder(downsampleIndex)
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0);
        builder.put(downsampleIndexMeta);
        // let's remove the first generation index
        dataStream = dataStream.removeBackingIndex(builder.get(firstGenIndex).getIndex());
        builder.put(dataStream);
        ClusterState previousState = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();

        ClusterState newState = new DeleteSourceAndAddDownsampleToDS(Settings.EMPTY, dataStreamName, firstGenIndex, downsampleIndex, null)
            .execute(previousState);

        IndexAbstraction downsampleIndexAbstraction = newState.metadata().getIndicesLookup().get(downsampleIndex);
        assertThat(downsampleIndexAbstraction, is(notNullValue()));
        assertThat(downsampleIndexAbstraction.getParentDataStream(), is(notNullValue()));
        // the downsample index is part of the data stream
        assertThat(downsampleIndexAbstraction.getParentDataStream().getName(), is(dataStreamName));

        assertThat(newState.metadata().getIndicesLookup().get(firstGenIndex), is(nullValue()));
    }
}
