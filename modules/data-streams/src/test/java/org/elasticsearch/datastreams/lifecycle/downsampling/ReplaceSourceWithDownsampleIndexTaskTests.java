/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle.downsampling;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
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

public class ReplaceSourceWithDownsampleIndexTaskTests extends ESTestCase {

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
            DataStreamLifecycle.newBuilder().dataRetention(TimeValue.MAX_VALUE).build(),
            now
        );
        builder.put(dataStream);
        ClusterState previousState = ClusterState.builder(ClusterName.DEFAULT).metadata(builder).build();

        String firstGeneration = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        ClusterState newState = new ReplaceSourceWithDownsampleIndexTask(
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
            DataStreamLifecycle.newBuilder().dataRetention(TimeValue.MAX_VALUE).build(),
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

        ClusterState newState = new ReplaceSourceWithDownsampleIndexTask(dataStreamName, firstGenIndex, downsampleIndex, null).execute(
            previousState
        );

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
            DataStreamLifecycle.newBuilder().dataRetention(TimeValue.MAX_VALUE).build(),
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
            () -> new ReplaceSourceWithDownsampleIndexTask(dataStreamName, writeIndex, downsampleIndex, null).execute(previousState)
        );

        assertThat(
            illegalStateException.getMessage(),
            is("index [" + writeIndex + "] is the write index for data stream [" + dataStreamName + "] and " + "cannot be replaced")
        );
    }

    public void testSourceIsReplacedWithDownsampleAndOriginationDateIsConfigured() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.newBuilder().dataRetention(TimeValue.MAX_VALUE).build(),
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
        IndexMetadata.Builder firstGenBuilder = IndexMetadata.builder(indexMetadata)
            .putCustom(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, Map.of(FORCE_MERGE_COMPLETED_TIMESTAMP_METADATA_KEY, String.valueOf(now)));
        Metadata.Builder metaBuilder = Metadata.builder(previousState.metadata()).put(firstGenBuilder);
        previousState = ClusterState.builder(previousState).metadata(metaBuilder).build();
        ClusterState newState = new ReplaceSourceWithDownsampleIndexTask(dataStreamName, firstGenIndex, downsampleIndex, null).execute(
            previousState
        );

        IndexAbstraction downsampleIndexAbstraction = newState.metadata().getIndicesLookup().get(downsampleIndex);
        assertThat(downsampleIndexAbstraction, is(notNullValue()));
        assertThat(downsampleIndexAbstraction.getParentDataStream(), is(notNullValue()));
        // the downsample index is part of the data stream
        assertThat(downsampleIndexAbstraction.getParentDataStream().getName(), is(dataStreamName));

        // the source index is NOT part of the data stream
        IndexAbstraction sourceIndexAbstraction = newState.metadata().getIndicesLookup().get(firstGenIndex);
        assertThat(sourceIndexAbstraction, is(notNullValue()));
        assertThat(sourceIndexAbstraction.getParentDataStream(), is(nullValue()));

        // let's check the downsample index has the origination date configured to the source index rollover time
        IndexMetadata firstGenMeta = newState.metadata().index(firstGenIndex);
        RolloverInfo rolloverInfo = firstGenMeta.getRolloverInfos().get(dataStreamName);
        assertThat(rolloverInfo, is(notNullValue()));

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
            DataStreamLifecycle.newBuilder().dataRetention(TimeValue.MAX_VALUE).build(),
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

        ClusterState newState = new ReplaceSourceWithDownsampleIndexTask(dataStreamName, firstGenIndex, downsampleIndex, null).execute(
            previousState
        );

        IndexAbstraction downsampleIndexAbstraction = newState.metadata().getIndicesLookup().get(downsampleIndex);
        assertThat(downsampleIndexAbstraction, is(notNullValue()));
        assertThat(downsampleIndexAbstraction.getParentDataStream(), is(notNullValue()));
        // the downsample index is part of the data stream
        assertThat(downsampleIndexAbstraction.getParentDataStream().getName(), is(dataStreamName));

        // the source index is NOT part of the data stream
        IndexAbstraction sourceIndexAbstraction = newState.metadata().getIndicesLookup().get(firstGenIndex);
        assertThat(sourceIndexAbstraction, is(notNullValue()));
        assertThat(sourceIndexAbstraction.getParentDataStream(), is(nullValue()));

        IndexMetadata downsampleMeta = newState.metadata().index(downsampleIndex);
        assertThat(IndexSettings.LIFECYCLE_ORIGINATION_DATE_SETTING.get(downsampleMeta.getSettings()), is(downsampleOriginationDate));
    }

    public void testSourceIndexIsNotPartOfDSAnymore() {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.newBuilder().dataRetention(TimeValue.MAX_VALUE).build(),
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

        ClusterState newState = new ReplaceSourceWithDownsampleIndexTask(dataStreamName, firstGenIndex, downsampleIndex, null).execute(
            previousState
        );

        IndexAbstraction downsampleIndexAbstraction = newState.metadata().getIndicesLookup().get(downsampleIndex);
        assertThat(downsampleIndexAbstraction, is(notNullValue()));
        assertThat(downsampleIndexAbstraction.getParentDataStream(), is(notNullValue()));
        // the downsample index is part of the data stream
        assertThat(downsampleIndexAbstraction.getParentDataStream().getName(), is(dataStreamName));

        // origination date and the lifecycle metadata is configured even if the source index is not part of the data stream anymore
        IndexMetadata firstGenMeta = newState.metadata().index(firstGenIndex);
        RolloverInfo rolloverInfo = firstGenMeta.getRolloverInfos().get(dataStreamName);
        assertThat(rolloverInfo, is(notNullValue()));
    }

    public void testListenersIsNonConsideredInEquals() {
        // the task is used as a key in a result deduplicator ({@link ResultDeduplicator}) map and the listener must not
        // be taken into account

        String dataStreamName = randomAlphaOfLengthBetween(10, 100);
        String sourceBackingIndex = randomAlphaOfLengthBetween(10, 100);
        String downsampleIndex = randomAlphaOfLengthBetween(10, 100);
        ReplaceSourceWithDownsampleIndexTask withoutListener = new ReplaceSourceWithDownsampleIndexTask(
            dataStreamName,
            sourceBackingIndex,
            downsampleIndex,
            null
        );

        ReplaceSourceWithDownsampleIndexTask withListener = new ReplaceSourceWithDownsampleIndexTask(
            dataStreamName,
            sourceBackingIndex,
            downsampleIndex,
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {

                }

                @Override
                public void onFailure(Exception e) {

                }
            }
        );

        assertThat(withoutListener.equals(withListener), is(true));
    }
}
