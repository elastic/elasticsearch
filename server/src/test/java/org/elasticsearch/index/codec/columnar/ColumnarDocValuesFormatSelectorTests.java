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

    public void testUsesColumnarForStrictColumnarModes() {
        for (IndexMode mode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            assertTrue("mode=" + mode, ColumnarDocValuesFormatSelector.useColumnarCodec(indexSettings(mode, randomEligibleVersion())));
        }
    }

    public void testNotUsedForNonStrictColumnarModes() {
        for (IndexMode mode : List.of(IndexMode.STANDARD, IndexMode.TIME_SERIES, IndexMode.LOGSDB)) {
            assertFalse("mode=" + mode, ColumnarDocValuesFormatSelector.useColumnarCodec(indexSettings(mode, IndexVersion.current())));
        }
    }

    public void testVersionBoundary() {
        final IndexVersion justBefore = IndexVersionUtils.getPreviousVersion(IndexVersions.COLUMNAR_CODEC_NO_FEATURE_FLAG);
        assertFalse(ColumnarDocValuesFormatSelector.useColumnarCodec(indexSettings(IndexMode.COLUMNAR, justBefore)));
        assertTrue(
            ColumnarDocValuesFormatSelector.useColumnarCodec(
                indexSettings(IndexMode.COLUMNAR, IndexVersions.COLUMNAR_CODEC_NO_FEATURE_FLAG)
            )
        );
    }

    public void testNumericSettingIsRegisteredWhenFlagEnabled() {
        assumeTrue("columnar_codec_numeric feature flag must be enabled", numericFlagEnabled());
        assertNotNull(
            "index.columnar_codec.numeric.enabled must be a registered index setting",
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.get(IndexSettings.COLUMNAR_CODEC_NUMERIC_ENABLED_SETTING.getKey())
        );
    }

    public void testNumericSettingIsNotRegisteredWhenFlagDisabled() {
        assumeFalse("columnar_codec_numeric feature flag must be disabled", numericFlagEnabled());
        assertNull(
            "index.columnar_codec.numeric.enabled must not be registered when the columnar_codec_numeric feature flag is disabled",
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.get(IndexSettings.COLUMNAR_CODEC_NUMERIC_ENABLED_SETTING.getKey())
        );
    }

    public void testNumericUsesColumnarForStrictColumnarModesByDefault() {
        assumeTrue("columnar_codec_numeric feature flag must be enabled", numericFlagEnabled());
        for (IndexMode mode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            assertTrue(
                "mode=" + mode,
                ColumnarDocValuesFormatSelector.useColumnarNumericCodec(indexSettings(mode, randomNumericEligibleVersion()))
            );
        }
    }

    public void testNumericNotUsedForNonStrictColumnarModes() {
        for (IndexMode mode : List.of(IndexMode.STANDARD, IndexMode.TIME_SERIES, IndexMode.LOGSDB)) {
            assertFalse(
                "mode=" + mode,
                ColumnarDocValuesFormatSelector.useColumnarNumericCodec(indexSettings(mode, IndexVersion.current()))
            );
        }
    }

    public void testNumericSettingDisabledNeverSelects() {
        assumeTrue("columnar_codec_numeric feature flag must be enabled", numericFlagEnabled());
        final IndexSettings indexSettings = indexSettings(
            IndexMode.COLUMNAR,
            randomNumericEligibleVersion(),
            Settings.builder().put(IndexSettings.COLUMNAR_CODEC_NUMERIC_ENABLED_SETTING.getKey(), false).build()
        );
        assertFalse(ColumnarDocValuesFormatSelector.useColumnarNumericCodec(indexSettings));
        // Strings use the codec whatever numerics do.
        assertTrue(ColumnarDocValuesFormatSelector.useColumnarCodec(indexSettings));
    }

    /**
     * Indices created before {@link IndexVersions#COLUMNAR_CODEC_NUMERIC_FF} keep writing numeric fields as they always have, so
     * no existing index changes format on upgrade. An explicit opt-in can't override that either.
     */
    public void testNumericVersionBoundary() {
        assumeTrue("columnar_codec_numeric feature flag must be enabled", numericFlagEnabled());
        final IndexVersion justBefore = IndexVersionUtils.getPreviousVersion(IndexVersions.COLUMNAR_CODEC_NUMERIC_FF);
        assertFalse(ColumnarDocValuesFormatSelector.useColumnarNumericCodec(indexSettings(IndexMode.COLUMNAR, justBefore)));
        assertFalse(
            ColumnarDocValuesFormatSelector.useColumnarNumericCodec(
                indexSettings(
                    IndexMode.COLUMNAR,
                    justBefore,
                    Settings.builder().put(IndexSettings.COLUMNAR_CODEC_NUMERIC_ENABLED_SETTING.getKey(), true).build()
                )
            )
        );
        assertTrue(
            ColumnarDocValuesFormatSelector.useColumnarNumericCodec(
                indexSettings(IndexMode.COLUMNAR, IndexVersions.COLUMNAR_CODEC_NUMERIC_FF)
            )
        );
    }

    public void testNumericFlagDisabledNeverSelects() {
        assumeFalse("columnar_codec_numeric feature flag must be disabled", numericFlagEnabled());
        assertFalse(
            ColumnarDocValuesFormatSelector.useColumnarNumericCodec(indexSettings(IndexMode.COLUMNAR, randomNumericEligibleVersion()))
        );
    }

    private static boolean numericFlagEnabled() {
        return ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_NUMERIC_FEATURE_FLAG.isEnabled();
    }

    private static IndexVersion randomEligibleVersion() {
        return IndexVersionUtils.randomVersionBetween(IndexVersions.COLUMNAR_CODEC_NO_FEATURE_FLAG, IndexVersion.current());
    }

    private static IndexVersion randomNumericEligibleVersion() {
        return IndexVersionUtils.randomVersionBetween(IndexVersions.COLUMNAR_CODEC_NUMERIC_FF, IndexVersion.current());
    }

    private static IndexSettings indexSettings(final IndexMode mode, final IndexVersion version) {
        return indexSettings(mode, version, Settings.EMPTY);
    }

    private static IndexSettings indexSettings(final IndexMode mode, final IndexVersion version, final Settings extra) {
        final Settings.Builder builder = Settings.builder()
            .put(extra)
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
