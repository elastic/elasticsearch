/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.columnar;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.util.List;

public class ColumnarDocValuesFormatSelectorTests extends ESTestCase {

    public void testColumnarCodecSettingIsRegistered() {
        assertNotNull(
            "index.columnar_codec.enabled must be a registered index setting",
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.get(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey())
        );
    }

    public void testUsesColumnarForStrictColumnarModesWhenEnabled() {
        for (IndexMode mode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            assertTrue(
                "mode=" + mode,
                ColumnarDocValuesFormatSelector.useColumnarCodec(indexSettings(mode, randomEligibleVersion(), true))
            );
        }
    }

    public void testNotUsedForNonStrictColumnarModes() {
        for (IndexMode mode : List.of(IndexMode.STANDARD, IndexMode.TIME_SERIES, IndexMode.LOGSDB)) {
            assertFalse(
                "mode=" + mode,
                ColumnarDocValuesFormatSelector.useColumnarCodec(indexSettings(mode, IndexVersion.current(), true))
            );
        }
    }

    public void testSettingDisabledNeverSelects() {
        assertFalse(ColumnarDocValuesFormatSelector.useColumnarCodec(indexSettings(IndexMode.COLUMNAR, randomEligibleVersion(), false)));
    }

    public void testVersionBoundary() {
        final IndexVersion justBefore = IndexVersionUtils.getPreviousVersion(IndexVersions.COLUMNAR_CODEC_NO_FEATURE_FLAG);
        assertFalse(ColumnarDocValuesFormatSelector.useColumnarCodec(indexSettings(IndexMode.COLUMNAR, justBefore, true)));
        assertTrue(
            ColumnarDocValuesFormatSelector.useColumnarCodec(
                indexSettings(IndexMode.COLUMNAR, IndexVersions.COLUMNAR_CODEC_NO_FEATURE_FLAG, true)
            )
        );
    }

    /**
     * Eligible indices default to {@code index.columnar_codec.enabled=true}, so they select the ColumNAR codec without an explicit opt-in.
     */
    public void testDefaultEnabledForEligibleIndices() {
        final IndexSettings settings = indexSettingsWithDefaultCodecEnabled(IndexMode.COLUMNAR, randomEligibleVersion());
        assertTrue(settings.isColumnarCodecEnabled());
        assertTrue(ColumnarDocValuesFormatSelector.useColumnarCodec(settings));
    }

    /**
     * Indices created before {@link IndexVersions#COLUMNAR_CODEC_NO_FEATURE_FLAG} default to
     * {@code index.columnar_codec.enabled=false}, matching the codec they are read with.
     */
    public void testDefaultDisabledForIndicesBeforeVersionGate() {
        final IndexVersion before = IndexVersionUtils.randomVersionBetween(
            IndexVersions.COLUMNAR_DOC_VALUES_CODEC_FEATURE_FLAG,
            IndexVersionUtils.getPreviousVersion(IndexVersions.COLUMNAR_CODEC_NO_FEATURE_FLAG)
        );
        final IndexSettings settings = indexSettingsWithDefaultCodecEnabled(IndexMode.COLUMNAR, before);
        assertFalse(settings.isColumnarCodecEnabled());
        assertFalse(ColumnarDocValuesFormatSelector.useColumnarCodec(settings));
    }

    private static IndexVersion randomEligibleVersion() {
        return IndexVersionUtils.randomVersionBetween(IndexVersions.COLUMNAR_CODEC_NO_FEATURE_FLAG, IndexVersion.current());
    }

    private static IndexSettings indexSettings(final IndexMode mode, final IndexVersion version, boolean columnarEnabled) {
        final Settings.Builder builder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        if (mode != IndexMode.STANDARD) {
            builder.put("index.mode", mode.getName());
        }
        if (mode == IndexMode.TIME_SERIES) {
            builder.put("index.routing_path", "dimension");
        }
        builder.put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), columnarEnabled);
        final IndexMetadata metadata = IndexMetadata.builder("test").settings(builder).build();
        return new IndexSettings(metadata, Settings.EMPTY);
    }

    /**
     * Builds index settings without explicitly setting {@link IndexSettings#COLUMNAR_CODEC_ENABLED_SETTING},
     * so tests can exercise the version-gated default value.
     */
    private static IndexSettings indexSettingsWithDefaultCodecEnabled(final IndexMode mode, final IndexVersion version) {
        final Settings.Builder builder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        if (mode != IndexMode.STANDARD) {
            builder.put("index.mode", mode.getName());
        }
        if (mode == IndexMode.TIME_SERIES) {
            builder.put("index.routing_path", "dimension");
        }
        final IndexMetadata metadata = IndexMetadata.builder("test").settings(builder).build();
        return new IndexSettings(metadata, Settings.EMPTY);
    }
}
