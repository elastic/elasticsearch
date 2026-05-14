/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;

public class VectordbDocumentIndexModeTests extends ESTestCase {

    public void testFromString() {
        assumeTrue("vectordb_document index mode requires snapshot build", IndexMode.VECTORDB_FEATURE_FLAG.isEnabled());
        assertThat(IndexMode.fromString("vectordb_document"), equalTo(IndexMode.VECTORDB_DOCUMENT));
        assertThat(IndexMode.fromString("VECTORDB_DOCUMENT"), equalTo(IndexMode.VECTORDB_DOCUMENT));
    }

    public void testProviderSetsDefaultSettings() {
        assumeTrue("vectordb_document index mode requires snapshot build", IndexMode.VECTORDB_FEATURE_FLAG.isEnabled());
        // _source mode defaults to STORED for vectordb_document index mode.
        assertThat(IndexMode.VECTORDB_DOCUMENT.defaultSourceMode().name(), equalTo("STORED"));

        Settings userSettings = Settings.builder().put(IndexSettings.MODE.getKey(), "vectordb_document").build();
        Settings.Builder additional = Settings.builder();
        runProvider(userSettings, additional);
        Settings resolved = additional.build();

        // Source vectors are excluded from _source by default for vectordb_document mode.
        assertEquals("true", resolved.get(IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey()));

        // The default preload list matches VECTORDB_DOCUMENT_MODE_PRELOAD_EXTENSIONS.
        List<String> preload = resolved.getAsList(IndexModule.INDEX_STORE_PRE_LOAD_SETTING.getKey());
        assertEquals(IndexMode.IndexModeSettingsProvider.VECTORDB_DOCUMENT_MODE_PRELOAD_EXTENSIONS, preload);
        // HNSW graph, quantized data and IVF centroids are preloaded.
        assertThat(preload, hasItems("vex", "veq", "veb", "cenivf"));
        // Raw vectors and IVF cluster postings are intentionally excluded: large and streamed on demand.
        assertThat(preload, not(hasItems("vec", "clivf")));
        // Vector metadata files are intentionally excluded: tiny and fully read by Lucene at directory open.
        assertThat(preload, not(hasItems("vem", "vemf", "vemq", "vemb", "vfi", "mivf")));

        // Intra-merge parallelism is enabled by default.
        assertEquals("true", resolved.get(IndexSettings.INTRA_MERGE_PARALLELISM_ENABLED_SETTING.getKey()));

        // Merge IO auto-throttling is disabled by default.
        assertEquals("false", resolved.get(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey()));
    }

    public void testProviderRejectsExplicitFalseExcludeSourceVectors() {
        assumeTrue("vectordb_document index mode requires snapshot build", IndexMode.VECTORDB_FEATURE_FLAG.isEnabled());
        Settings userSettings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "vectordb_document")
            .put(IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey(), false)
            .build();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> runProvider(userSettings, Settings.builder()));
        assertThat(ex.getMessage(), containsString("index.mapping.exclude_source_vectors"));
        assertThat(ex.getMessage(), containsString("vectordb_document"));
    }

    public void testProviderAcceptsExplicitTrueExcludeSourceVectors() {
        assumeTrue("vectordb_document index mode requires snapshot build", IndexMode.VECTORDB_FEATURE_FLAG.isEnabled());
        Settings userSettings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "vectordb_document")
            .put(IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey(), true)
            .build();
        Settings.Builder additional = Settings.builder();
        runProvider(userSettings, additional);
        assertEquals("true", additional.get(IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey()));
    }

    public void testProviderDoesNotOverrideExplicitSettings() {
        assumeTrue("vectordb_document index mode requires snapshot build", IndexMode.VECTORDB_FEATURE_FLAG.isEnabled());
        Settings userSettings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "vectordb_document")
            .putList(IndexModule.INDEX_STORE_PRE_LOAD_SETTING.getKey(), "vex")
            .put(IndexSettings.INTRA_MERGE_PARALLELISM_ENABLED_SETTING.getKey(), false)
            .put(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey(), true)
            .build();
        Settings.Builder additional = Settings.builder();
        runProvider(userSettings, additional);
        Settings resolved = additional.build();

        assertNull(
            "should not override user-provided preload list",
            resolved.getAsList(IndexModule.INDEX_STORE_PRE_LOAD_SETTING.getKey(), null)
        );
        assertNull(
            "should not override user-provided intra-merge parallelism setting",
            resolved.get(IndexSettings.INTRA_MERGE_PARALLELISM_ENABLED_SETTING.getKey())
        );
        assertNull(
            "should not override user-provided merge auto-throttle setting",
            resolved.get(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey())
        );
    }

    private static void runProvider(Settings userSettings, Settings.Builder additional) {
        new IndexMode.IndexModeSettingsProvider().provideAdditionalSettings(
            "test_index",
            null,
            null,
            ProjectMetadata.builder(ProjectId.fromId("test_project")).build(),
            Instant.now(),
            userSettings,
            List.of(),
            IndexVersion.current(),
            additional
        );
    }
}
