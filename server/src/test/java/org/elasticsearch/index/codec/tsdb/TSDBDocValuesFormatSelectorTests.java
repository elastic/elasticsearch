/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.DocValuesFormat;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class TSDBDocValuesFormatSelectorTests extends ESTestCase {

    private static final String ES95_CODEC_NAME = "ES95TSDB";

    private static List<IndexMode> indexModesUnderTest() {
        List<IndexMode> modes = new ArrayList<>(List.of(IndexMode.TIME_SERIES, IndexMode.STANDARD, IndexMode.LOGSDB));
        if (IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled()) {
            modes.add(IndexMode.COLUMNAR);
            modes.add(IndexMode.LOGSDB_COLUMNAR);
        }
        return modes;
    }

    public void testES95SettingRegistrationMatchesFeatureFlag() {
        assertEquals(
            "index.time_series.es95_codec.enabled registration must match the es95_codec feature flag state",
            IndexSettings.ES95_CODEC_FEATURE_FLAG.isEnabled(),
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS.contains(IndexSettings.TIME_SERIES_ES95_CODEC_ENABLED_SETTING)
        );
    }

    public void testES95SelectedAcrossModesWhenSettingEnabled() {
        assumeTrue("es95_codec feature flag must be enabled", IndexSettings.ES95_CODEC_FEATURE_FLAG.isEnabled());
        for (IndexMode mode : indexModesUnderTest()) {
            final DocValuesFormat format = TSDBDocValuesFormatSelector.select(indexSettings(mode, IndexVersion.current(), true));
            assertThat("mode=" + mode, format.getName(), equalTo(ES95_CODEC_NAME));
        }
    }

    public void testES819SelectedAcrossModesWhenSettingDisabled() {
        assumeTrue("es95_codec feature flag must be enabled", IndexSettings.ES95_CODEC_FEATURE_FLAG.isEnabled());
        for (IndexMode mode : indexModesUnderTest()) {
            final DocValuesFormat format = TSDBDocValuesFormatSelector.select(indexSettings(mode, IndexVersion.current(), false));
            assertThat("mode=" + mode, format.getName(), startsWith("ES819"));
        }
    }

    public void testVersionBoundary() {
        assumeTrue("es95_codec feature flag must be enabled", IndexSettings.ES95_CODEC_FEATURE_FLAG.isEnabled());
        final IndexVersion justBefore = IndexVersionUtils.getPreviousVersion(IndexVersions.ES95_TSDB_CODEC_FEATURE_FLAG);
        final IndexVersion exact = IndexVersions.ES95_TSDB_CODEC_FEATURE_FLAG;

        assertThat(
            TSDBDocValuesFormatSelector.select(indexSettings(IndexMode.TIME_SERIES, justBefore, true)).getName(),
            startsWith("ES819")
        );
        assertThat(
            TSDBDocValuesFormatSelector.select(indexSettings(IndexMode.TIME_SERIES, exact, true)).getName(),
            equalTo(ES95_CODEC_NAME)
        );
    }

    public void testES819AlwaysSelectedForTSDBWithOldVersion() {
        final IndexVersion oldVersion = IndexVersionUtils.getPreviousVersion(IndexVersions.ES95_TSDB_CODEC_FEATURE_FLAG);
        final DocValuesFormat format = TSDBDocValuesFormatSelector.select(indexSettings(IndexMode.TIME_SERIES, oldVersion, true));
        assertThat(format.getName(), startsWith("ES819"));
    }

    private static IndexSettings indexSettings(final IndexMode mode, final IndexVersion version, final boolean es95Enabled) {
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
        if (es95Enabled && IndexSettings.ES95_CODEC_FEATURE_FLAG.isEnabled()) {
            builder.put(IndexSettings.TIME_SERIES_ES95_CODEC_ENABLED_SETTING.getKey(), true);
        }
        final IndexMetadata metadata = IndexMetadata.builder("test").settings(builder).build();
        return new IndexSettings(metadata, Settings.EMPTY);
    }
}
