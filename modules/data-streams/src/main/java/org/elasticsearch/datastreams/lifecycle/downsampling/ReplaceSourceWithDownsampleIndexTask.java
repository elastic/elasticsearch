/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle.downsampling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.datastreams.DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY;

/**
 * Cluster state task that replaces a source index in a data stream with its downsample index.
 * In the process it will configure the origination date for the downsample index (so it can
 * have a correct generation time).
 */
public class ReplaceSourceWithDownsampleIndexTask implements ClusterStateTaskListener {
    private static final Logger LOGGER = LogManager.getLogger(ReplaceSourceWithDownsampleIndexTask.class);
    private ActionListener<Void> listener;
    private final String dataStreamName;
    private final String sourceBackingIndex;
    private final String downsampleIndex;

    public ReplaceSourceWithDownsampleIndexTask(
        String dataStreamName,
        String sourceBackingIndex,
        String downsampleIndex,
        ActionListener<Void> listener
    ) {
        this.dataStreamName = dataStreamName;
        this.sourceBackingIndex = sourceBackingIndex;
        this.downsampleIndex = downsampleIndex;
        this.listener = listener;
    }

    ClusterState execute(ClusterState state) {
        LOGGER.trace(
            "Updating cluster state to replace index [{}] with [{}] in data stream [{}]",
            sourceBackingIndex,
            downsampleIndex,
            dataStreamName
        );
        IndexAbstraction sourceIndexAbstraction = state.metadata().getIndicesLookup().get(sourceBackingIndex);
        IndexMetadata downsampleIndexMeta = state.metadata().index(downsampleIndex);
        if (downsampleIndexMeta == null) {
            // the downsample index doesn't exist anymore so nothing to replace here
            LOGGER.trace(
                "Received request replace index [{}] with [{}] in data stream [{}] but the replacement index [{}] doesn't exist."
                    + "Nothing to do here.",
                sourceBackingIndex,
                downsampleIndex,
                dataStreamName,
                downsampleIndex
            );
            return state;
        }
        IndexMetadata sourceIndexMeta = state.metadata().index(sourceBackingIndex);
        DataStream dataStream = state.metadata().dataStreams().get(dataStreamName);
        if (sourceIndexAbstraction == null) {
            // index was deleted in the meantime, so let's check if we can make sure the downsample index ends up in the
            // data stream (if not already there)
            if (dataStream != null
                && dataStream.getIndices().stream().filter(index -> index.getName().equals(downsampleIndex)).findAny().isEmpty()) {
                // add downsample index to data stream
                LOGGER.trace(
                    "unable find source index [{}] but adding index [{}] to data stream [{}]",
                    sourceBackingIndex,
                    downsampleIndex,
                    dataStreamName
                );
                Metadata.Builder newMetaData = Metadata.builder(state.metadata())
                    .put(dataStream.addBackingIndex(state.metadata(), downsampleIndexMeta.getIndex()));
                return ClusterState.builder(state).metadata(newMetaData).build();
            }
        } else {
            // the source index exists
            DataStream sourceParentDataStream = sourceIndexAbstraction.getParentDataStream();
            if (sourceParentDataStream != null) {
                assert sourceParentDataStream.getName().equals(dataStreamName)
                    : "the backing index must be part of the provided data "
                        + "stream ["
                        + dataStreamName
                        + "] but it is instead part of data stream ["
                        + sourceParentDataStream.getName()
                        + "]";
                if (sourceParentDataStream.getWriteIndex().getName().equals(sourceBackingIndex)) {
                    String errorMessage = String.format(
                        Locale.ROOT,
                        "index [%s] is the write index for data stream [%s] and cannot be replaced",
                        sourceBackingIndex,
                        sourceParentDataStream.getName()
                    );
                    throw new IllegalStateException(errorMessage);
                }
                if (sourceIndexMeta != null) {
                    // both indices exist, let's copy the origination date from the source index to the downsample index
                    Metadata.Builder newMetaData = Metadata.builder(state.getMetadata());
                    TimeValue generationLifecycleDate = dataStream.getGenerationLifecycleDate(sourceIndexMeta);
                    assert generationLifecycleDate != null : "write index must never be downsampled, or replaced";
                    IndexMetadata updatedDownsampleMetadata = copyDataStreamLifecycleState(
                        sourceIndexMeta,
                        downsampleIndexMeta,
                        generationLifecycleDate.millis()
                    );

                    newMetaData.put(updatedDownsampleMetadata, true);
                    // replace source with downsample
                    newMetaData.put(dataStream.replaceBackingIndex(sourceIndexMeta.getIndex(), downsampleIndexMeta.getIndex()));
                    return ClusterState.builder(state).metadata(newMetaData).build();
                }
            } else {
                // the source index is not part of a data stream, so let's check if we can make sure the downsample index ends up in the
                // data stream
                if (dataStream != null
                    && dataStream.getIndices().stream().filter(index -> index.getName().equals(downsampleIndex)).findAny().isEmpty()) {
                    Metadata.Builder newMetaData = Metadata.builder(state.getMetadata());
                    TimeValue generationLifecycleDate = dataStream.getGenerationLifecycleDate(sourceIndexMeta);
                    assert generationLifecycleDate != null : "write index must never be downsampled, or replaced";

                    IndexMetadata updatedDownsampleMetadata = copyDataStreamLifecycleState(
                        sourceIndexMeta,
                        downsampleIndexMeta,
                        generationLifecycleDate.millis()
                    );
                    newMetaData.put(updatedDownsampleMetadata, true);
                    // add downsample index to data stream
                    newMetaData.put(dataStream.addBackingIndex(state.metadata(), downsampleIndexMeta.getIndex()));
                    return ClusterState.builder(state).metadata(newMetaData).build();
                }
            }
        }

        return state;
    }

    /**
     * Copies the data stream lifecycle state information from the source index to the destination.
     * This ensures the destination index will have a generation time by setting the {@link IndexSettings#LIFECYCLE_ORIGINATION_DATE} and
     * that the source index is confingured in the
     * {@link org.elasticsearch.datastreams.DataStreamsPlugin#LIFECYCLE_CUSTOM_INDEX_METADATA_KEY} custom.
     */
    private static IndexMetadata copyDataStreamLifecycleState(
        IndexMetadata source,
        IndexMetadata dest,
        long sourceIndexGenerationTimeMillis
    ) {
        IndexMetadata.Builder downsampleIndexBuilder = IndexMetadata.builder(dest);
        Map<String, String> lifecycleCustomMetadata = source.getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY);
        if (lifecycleCustomMetadata != null) {
            // this will, for now, ensure that DSL tail merging is skipped for the downsample index (and it should be as the downsample
            // transport action forcemerged the downsample index to 1 segment)
            downsampleIndexBuilder.putCustom(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, lifecycleCustomMetadata);
        }
        if (IndexSettings.LIFECYCLE_ORIGINATION_DATE_SETTING.exists(dest.getSettings()) == false) {
            downsampleIndexBuilder.settings(
                Settings.builder()
                    .put(dest.getSettings())
                    .put(IndexSettings.LIFECYCLE_ORIGINATION_DATE, sourceIndexGenerationTimeMillis)
                    .build()
            ).settingsVersion(dest.getSettingsVersion() + 1L);
        }
        return downsampleIndexBuilder.build();
    }

    @Override
    public void onFailure(Exception e) {
        if (listener != null) {
            listener.onFailure(e);
        }
    }

    public String getDataStreamName() {
        return dataStreamName;
    }

    public String getSourceBackingIndex() {
        return sourceBackingIndex;
    }

    public String getDownsampleIndex() {
        return downsampleIndex;
    }

    public ActionListener<Void> getListener() {
        return listener;
    }

    public void setListener(ActionListener<Void> listener) {
        this.listener = listener;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReplaceSourceWithDownsampleIndexTask that = (ReplaceSourceWithDownsampleIndexTask) o;
        return Objects.equals(dataStreamName, that.dataStreamName)
            && Objects.equals(sourceBackingIndex, that.sourceBackingIndex)
            && Objects.equals(downsampleIndex, that.downsampleIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataStreamName, sourceBackingIndex, downsampleIndex);
    }
}
