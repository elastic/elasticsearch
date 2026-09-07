/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class VectordbColumnarIndexModeTests extends ESTestCase {

    @Before
    public void assumeFeatureFlagEnabled() {
        assumeTrue("vectordb_columnar index mode requires snapshot build", IndexMode.VECTORDB_COLUMNAR_FEATURE_FLAG.isEnabled());
    }

    public void testFromString() {
        assertThat(IndexMode.fromString("vectordb_columnar"), equalTo(IndexMode.VECTORDB_COLUMNAR));
        assertThat(IndexMode.fromString("VECTORDB_COLUMNAR"), equalTo(IndexMode.VECTORDB_COLUMNAR));
    }

    public void testSerialization() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            IndexMode.writeTo(IndexMode.VECTORDB_COLUMNAR, out);
            try (var in = out.bytes().streamInput()) {
                assertThat(IndexMode.readFrom(in), equalTo(IndexMode.VECTORDB_COLUMNAR));
            }
        }
    }

    public void testSerializationFailsOnOlderTransportVersion() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersionUtils.getPreviousVersion(IndexMode.VECTORDB_COLUMNAR_INDEX_MODE));
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> IndexMode.writeTo(IndexMode.VECTORDB_COLUMNAR, out));
            assertThat(e.getMessage(), containsString("[vectordb_columnar] doesn't support serialization with transport version"));
        }
    }

    public void testColumnarDefaults() {
        assertThat(IndexMode.VECTORDB_COLUMNAR.defaultSourceMode(), equalTo(SourceFieldMapper.Mode.SYNTHETIC));
        // Unlike the other columnar modes, columnar_stored source is not supported yet.
        assertThat(IndexMode.VECTORDB_COLUMNAR.supportedSourceModes(), equalTo(List.of(SourceFieldMapper.Mode.SYNTHETIC)));
        assertThat(IndexMode.VECTORDB_COLUMNAR.getDefaultCodec(), equalTo(CodecService.BEST_COMPRESSION_CODEC));
        assertTrue(IndexMode.VECTORDB_COLUMNAR.isColumnar());
        assertTrue(IndexMode.VECTORDB_COLUMNAR.isStrictColumnar());
        assertTrue(IndexMode.VECTORDB_COLUMNAR.isVectorDb());
    }

    public void testStrictColumnarIndexSettings() {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.VECTORDB_COLUMNAR.getName()).build();
        IndexMetadata metadata = IndexSettingsTests.newIndexMeta("test", settings);
        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);

        assertTrue(indexSettings.isIndexDisabledByDefault());
        assertTrue(indexSettings.isUseColumnarIdByDefault());
        assertThat(indexSettings.seqNoIndexOptions(), equalTo(SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY));
        assertFalse(indexSettings.sequenceNumbersDisabled());
        // Dynamic strings become text fields for relevance scoring, but without the keyword multi-field that would duplicate them.
        assertTrue(indexSettings.getDynamicStringsAutoText());
        assertFalse(indexSettings.getDynamicStringsAutoKeywordSubfield());
    }

    public void testProviderSetsVectorDefaults() {
        Settings userSettings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.VECTORDB_COLUMNAR.getName()).build();
        Settings.Builder additional = Settings.builder();
        runProvider(userSettings, additional);
        Settings resolved = additional.build();

        // Synthetic source never stores vectors, so this only keeps them out of fetched _source unless a request asks for
        // them, matching vectordb_document.
        assertEquals("true", resolved.get(IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey()));
        assertEquals(
            IndexMode.IndexModeSettingsProvider.VECTORDB_MODE_PRELOAD_EXTENSIONS,
            resolved.getAsList(IndexModule.INDEX_STORE_PRE_LOAD_SETTING.getKey())
        );
        assertEquals("true", resolved.get(IndexSettings.INTRA_MERGE_PARALLELISM_ENABLED_SETTING.getKey()));
        assertEquals("false", resolved.get(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey()));
    }

    public void testProviderPreservesExplicitVectorPerformanceSettings() {
        Settings userSettings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.VECTORDB_COLUMNAR.getName())
            .put(IndexSettings.INTRA_MERGE_PARALLELISM_ENABLED_SETTING.getKey(), false)
            .put(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey(), true)
            .putList(IndexModule.INDEX_STORE_PRE_LOAD_SETTING.getKey(), "custom")
            .build();
        Settings.Builder additional = Settings.builder();
        runProvider(userSettings, additional);
        Settings resolved = additional.build();

        assertFalse(resolved.hasValue(IndexSettings.INTRA_MERGE_PARALLELISM_ENABLED_SETTING.getKey()));
        assertFalse(resolved.hasValue(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey()));
        assertFalse(resolved.hasValue(IndexModule.INDEX_STORE_PRE_LOAD_SETTING.getKey()));
        assertEquals("true", resolved.get(IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey()));
    }

    public void testProviderRejectsExplicitFalseExcludeSourceVectors() {
        Settings userSettings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.VECTORDB_COLUMNAR.getName())
            .put(IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey(), false)
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> runProvider(userSettings, Settings.builder()));
        assertThat(e.getMessage(), containsString("index.mapping.exclude_source_vectors"));
        assertThat(e.getMessage(), containsString("vectordb_columnar"));
    }

    public void testProviderDisablesSequenceNumbersForDataStream() {
        Settings userSettings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.VECTORDB_COLUMNAR.getName()).build();
        Settings.Builder additional = Settings.builder();
        runProvider(userSettings, "vectors", additional);
        assertEquals("true", additional.get(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey()));
    }

    private static void runProvider(Settings userSettings, Settings.Builder additional) {
        runProvider(userSettings, null, additional);
    }

    private static void runProvider(Settings userSettings, String dataStreamName, Settings.Builder additional) {
        new IndexMode.IndexModeSettingsProvider().provideAdditionalSettings(
            "test_index",
            dataStreamName,
            null,
            false,
            ProjectMetadata.builder(ProjectId.fromId("test_project")).build(),
            Instant.now(),
            userSettings,
            List.of(),
            IndexVersion.current(),
            additional
        );
    }
}
