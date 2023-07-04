/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class MetadataIsManagedByILMTests extends ESTestCase {

    public void testIsIndexManagedByILM() {
        {
            // index has no ILM policy configured
            IndexMetadata indexMetadata = createIndexMetadataBuilderForIndex("test-no-ilm-policy").build();
            Metadata metadata = Metadata.builder().put(indexMetadata, true).build();

            assertThat(metadata.isIndexManagedByILM(indexMetadata), is(false));
        }

        {
            // index has been deleted
            IndexMetadata indexMetadata = createIndexMetadataBuilderForIndex(
                "testindex",
                Settings.builder().put("index.lifecycle.name", "metrics").build()
            ).build();
            Metadata metadata = Metadata.builder().build();

            assertThat(metadata.isIndexManagedByILM(indexMetadata), is(false));
        }

        {
            // index has ILM policy configured and doesn't belong to a data stream
            IndexMetadata indexMetadata = createIndexMetadataBuilderForIndex(
                "testindex",
                Settings.builder().put("index.lifecycle.name", "metrics").build()
            ).build();
            Metadata metadata = Metadata.builder().put(indexMetadata, true).build();
            assertThat(metadata.isIndexManagedByILM(indexMetadata), is(true));
        }

        {
            // index has ILM policy configured and does belong to a data stream with a DLM lifecycle
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
                new DataLifecycle()
            );
            Metadata metadata = Metadata.builder().put(indexMetadata, true).put(dataStream).build();

            assertThat(metadata.isIndexManagedByILM(indexMetadata), is(true));
        }

        {
            // index has ILM policy configured and does belong to a data stream with a DLM lifecycle, but
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
                new DataLifecycle()
            );
            Metadata metadata = Metadata.builder().put(indexMetadata, true).put(dataStream).build();

            assertThat(metadata.isIndexManagedByILM(indexMetadata), is(false));
        }
    }

    public static IndexMetadata.Builder createIndexMetadataBuilderForIndex(String index) {
        return createIndexMetadataBuilderForIndex(index, Settings.EMPTY);
    }

    public static IndexMetadata.Builder createIndexMetadataBuilderForIndex(String index, Settings settings) {
        return IndexMetadata.builder(index)
            .settings(Settings.builder().put(settings).put(ESTestCase.settings(Version.CURRENT).build()))
            .numberOfShards(1)
            .numberOfReplicas(1);
    }

}
