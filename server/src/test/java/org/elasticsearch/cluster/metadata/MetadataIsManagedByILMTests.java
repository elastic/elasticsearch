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

            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata), is(false));
        }

        {
            // index has been deleted
            IndexMetadata indexMetadata = createIndexMetadataBuilderForIndex(
                "testindex",
                Settings.builder().put("index.lifecycle.name", "metrics").build()
            ).build();
            Metadata metadata = Metadata.builder().build();

            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata), is(false));
        }

        {
            // index has ILM policy configured and doesn't belong to a data stream
            IndexMetadata indexMetadata = createIndexMetadataBuilderForIndex(
                "testindex",
                Settings.builder().put("index.lifecycle.name", "metrics").build()
            ).build();
            Metadata metadata = Metadata.builder().put(indexMetadata, true).build();
            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata), is(true));
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

            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata), is(true));
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

            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata), is(false));
        }
    }

    public void testIsIndexManagedByIlmWithDefaultLifecycleForTimeSeries() {
        {
            // Backing index without ILM in a TSDB data stream: always false regardless of flag
            String dataStreamName = "metrics-tsdb";
            IndexMetadata indexMetadata = createIndexMetadataBuilderForIndex(DataStream.getDefaultBackingIndexName(dataStreamName, 1))
                .build();
            DataStream dataStream = DataStream.builder(dataStreamName, List.of(indexMetadata.getIndex()))
                .setIndexMode(IndexMode.TIME_SERIES)
                .build();
            Metadata metadata = Metadata.builder().put(indexMetadata, true).put(dataStream).build();

            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata, false), is(false));
            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata, true), is(false));
        }

        {
            // Backing index with ILM in a TSDB data stream with no lifecycle configured:
            // flag=false → no DSL lifecycle active → ILM manages it
            // flag=true → DEFAULT_DATA_LIFECYCLE applies → PREFER_ILM defaults to true → ILM still manages it
            String dataStreamName = "metrics-tsdb";
            IndexMetadata indexMetadata = createIndexMetadataBuilderForIndex(
                DataStream.getDefaultBackingIndexName(dataStreamName, 1),
                Settings.builder().put("index.lifecycle.name", "metrics").build()
            ).build();
            DataStream dataStream = DataStream.builder(dataStreamName, List.of(indexMetadata.getIndex()))
                .setIndexMode(IndexMode.TIME_SERIES)
                .build();
            Metadata metadata = Metadata.builder().put(indexMetadata, true).put(dataStream).build();

            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata, false), is(true));
            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata, true), is(true));
        }

        {
            // Backing index with ILM and PREFER_ILM=false in a TSDB data stream with no lifecycle configured:
            // flag=false → no DSL lifecycle active → ILM manages it (PREFER_ILM is irrelevant when no DSL)
            // flag=true → DEFAULT_DATA_LIFECYCLE applies → PREFER_ILM=false → DSL takes precedence → not managed by ILM
            String dataStreamName = "metrics-tsdb";
            IndexMetadata indexMetadata = createIndexMetadataBuilderForIndex(
                DataStream.getDefaultBackingIndexName(dataStreamName, 1),
                Settings.builder().put("index.lifecycle.name", "metrics").put(IndexSettings.PREFER_ILM, false).build()
            ).build();
            DataStream dataStream = DataStream.builder(dataStreamName, List.of(indexMetadata.getIndex()))
                .setIndexMode(IndexMode.TIME_SERIES)
                .build();
            Metadata metadata = Metadata.builder().put(indexMetadata, true).put(dataStream).build();

            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata, false), is(true));
            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata, true), is(false));
        }

        {
            // Backing index with ILM in a TSDB data stream with an explicit lifecycle: flag is irrelevant, DSL is active
            // PREFER_ILM defaults to true → ILM manages it
            String dataStreamName = "metrics-tsdb";
            IndexMetadata indexMetadata = createIndexMetadataBuilderForIndex(
                DataStream.getDefaultBackingIndexName(dataStreamName, 1),
                Settings.builder().put("index.lifecycle.name", "metrics").build()
            ).build();
            DataStream dataStream = DataStream.builder(dataStreamName, List.of(indexMetadata.getIndex()))
                .setIndexMode(IndexMode.TIME_SERIES)
                .setLifecycle(DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE)
                .build();
            Metadata metadata = Metadata.builder().put(indexMetadata, true).put(dataStream).build();

            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata, false), is(true));
            assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata, true), is(true));
        }
    }

    public void testLookupIndexIsNeverManagedByILM() {
        IndexMetadata indexMetadata = createIndexMetadataBuilderForIndex(
            "lookup-index",
            Settings.builder().put("index.lifecycle.name", "metrics").put(IndexSettings.MODE.getKey(), IndexMode.LOOKUP.getName()).build()
        ).build();
        Metadata metadata = Metadata.builder().put(indexMetadata, true).build();

        assertThat(metadata.getProject().isIndexManagedByILM(indexMetadata), is(false));
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
