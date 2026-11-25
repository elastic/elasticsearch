/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class MetadataIsManagedByILMTests extends ESTestCase {

    public void testIsIndexManagedByILM() {
        {
            // index has no ILM policy configured
            IndexMetadata indexMetadata = createIndexMetadataBuilderForIndex("test-no-ilm-policy").build();
            Metadata metadata = Metadata.builder().put(indexMetadata, true).build();

            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata, randomBoolean()), is(false));
        }

        {
            // index has been deleted
            IndexMetadata indexMetadata = createIndexMetadataBuilderForIndex(
                "testindex",
                Settings.builder().put("index.lifecycle.name", "metrics").build()
            ).build();
            Metadata metadata = Metadata.builder().build();

            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata, randomBoolean()), is(false));
        }

        {
            // index has ILM policy configured and doesn't belong to a data stream
            IndexMetadata indexMetadata = createIndexMetadataBuilderForIndex(
                "testindex",
                Settings.builder().put("index.lifecycle.name", "metrics").build()
            ).build();
            Metadata metadata = Metadata.builder().put(indexMetadata, true).build();
            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata, randomBoolean()), is(true));
        }

        {
            // index has ILM policy configured and does belong to a data stream with a data stream lifecycle
            // by default ILM takes precedence
            String dataStreamName = "metrics-prod";

            IndexMetadata indexMetadata = createIndexMetadataBuilderForIndex(
                DataStream.getDefaultBackingIndexName(dataStreamName, 1),
                Settings.builder().put("index.lifecycle.name", "metrics").build()
            ).build();

            DataStream dataStream = DataStreamTestHelper.newInstance(
                dataStreamName,
                List.of(indexMetadata.getIndex()),
                1,
                null,
                false,
                DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE
            );
            Metadata metadata = Metadata.builder().put(indexMetadata, true).put(dataStream).build();

            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata, randomBoolean()), is(true));
        }

        {
            // index has ILM policy configured and does belong to a data stream with a data stream lifecycle, but
            // the PREFER_ILM_SETTING is configured to false
            String dataStreamName = "metrics-prod";

            IndexMetadata indexMetadata = createIndexMetadataBuilderForIndex(
                DataStream.getDefaultBackingIndexName(dataStreamName, 1),
                Settings.builder().put("index.lifecycle.name", "metrics").put(IndexSettings.PREFER_ILM, false).build()
            ).build();

            DataStream dataStream = DataStreamTestHelper.newInstance(
                dataStreamName,
                List.of(indexMetadata.getIndex()),
                1,
                null,
                false,
                DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE
            );
            Metadata metadata = Metadata.builder().put(indexMetadata, true).put(dataStream).build();

            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata, randomBoolean()), is(false));
        }
    }

    public void testTimeSeriesDataStreamWithoutLifecycle() {
        String dataStreamName = "metrics-prod";
        {
            // ILM policy configured and ILM preferred, ILM manages the index regardless of the flag
            IndexMetadata indexMetadata = createIndexMetadataBuilderForIndex(
                DataStream.getDefaultBackingIndexName(dataStreamName, 1),
                Settings.builder().put("index.lifecycle.name", "metrics").build()
            ).build();
            Metadata metadata = Metadata.builder()
                .put(indexMetadata, true)
                .put(createDataStream(dataStreamName, indexMetadata, IndexMode.TIME_SERIES, null))
                .build();

            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata, randomBoolean()), is(true));
        }

        {
            // ILM policy configured but ILM not preferred, the flag enables the minimum lifecycle which takes over
            IndexMetadata indexMetadata = createIndexMetadataBuilderForIndex(
                DataStream.getDefaultBackingIndexName(dataStreamName, 1),
                Settings.builder().put("index.lifecycle.name", "metrics").put(IndexSettings.PREFER_ILM, false).build()
            ).build();
            Metadata metadata = Metadata.builder()
                .put(indexMetadata, true)
                .put(createDataStream(dataStreamName, indexMetadata, IndexMode.TIME_SERIES, null))
                .build();

            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata, false), is(true));
            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata, true), is(false));
        }
    }

    /**
     * An explicitly configured lifecycle on a time series data stream is never overridden by the default lifecycle for time series flag.
     */
    public void testTimeSeriesDataStreamWithExplicitLifecycle() {
        String dataStreamName = "metrics-prod";
        IndexMetadata indexMetadata = createIndexMetadataBuilderForIndex(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            Settings.builder().put("index.lifecycle.name", "metrics").put(IndexSettings.PREFER_ILM, false).build()
        ).build();
        {
            // disabled lifecycle, ILM manages the index
            DataStreamLifecycle disabled = DataStreamLifecycle.dataLifecycleBuilder().enabled(false).build();
            Metadata metadata = Metadata.builder()
                .put(indexMetadata, true)
                .put(createDataStream(dataStreamName, indexMetadata, IndexMode.TIME_SERIES, disabled))
                .build();

            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata, randomBoolean()), is(true));
        }

        {
            // enabled lifecycle and ILM not preferred, data stream lifecycle manages the index
            Metadata metadata = Metadata.builder()
                .put(indexMetadata, true)
                .put(createDataStream(dataStreamName, indexMetadata, IndexMode.TIME_SERIES, DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE))
                .build();

            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata, randomBoolean()), is(false));
        }
    }

    /**
     * The default lifecycle for time series flag only applies to time series data streams.
     */
    public void testNonTimeSeriesDataStreamWithoutLifecycleIgnoresFlag() {
        String dataStreamName = "logs-prod";
        IndexMetadata indexMetadata = createIndexMetadataBuilderForIndex(
            DataStream.getDefaultBackingIndexName(dataStreamName, 1),
            Settings.builder().put("index.lifecycle.name", "logs").put(IndexSettings.PREFER_ILM, false).build()
        ).build();
        IndexMode indexMode = randomBoolean() ? null : randomFrom(IndexMode.STANDARD, IndexMode.LOGSDB);
        Metadata metadata = Metadata.builder()
            .put(indexMetadata, true)
            .put(createDataStream(dataStreamName, indexMetadata, indexMode, null))
            .build();

        assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata, true), is(true));
        assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata, false), is(true));
    }

    public void testLookupIndexIsNeverManagedByILM() {
        IndexMetadata indexMetadata = createIndexMetadataBuilderForIndex(
            "lookup-index",
            Settings.builder().put("index.lifecycle.name", "metrics").put(IndexSettings.MODE.getKey(), IndexMode.LOOKUP.getName()).build()
        ).build();
        Metadata metadata = Metadata.builder().put(indexMetadata, true).build();

        assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata, randomBoolean()), is(false));
    }

    private static DataStream createDataStream(
        String dataStreamName,
        IndexMetadata backingIndex,
        @Nullable IndexMode indexMode,
        @Nullable DataStreamLifecycle lifecycle
    ) {
        return DataStream.builder(dataStreamName, List.of(backingIndex.getIndex()))
            .setGeneration(1)
            .setIndexMode(indexMode)
            .setLifecycle(lifecycle)
            .build();
    }

    public static IndexMetadata.Builder createIndexMetadataBuilderForIndex(String index) {
        return createIndexMetadataBuilderForIndex(index, Settings.EMPTY);
    }

    public static IndexMetadata.Builder createIndexMetadataBuilderForIndex(String index, Settings settings) {
        return IndexMetadata.builder(index)
            .settings(Settings.builder().put(settings).put(settings(IndexVersion.current()).build()))
            .numberOfShards(1)
            .numberOfReplicas(1);
    }

}
