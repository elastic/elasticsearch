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
import org.elasticsearch.cluster.node.DiscoveryNode;
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
    private static final String ES95RT_CODEC_NAME = "ES95RTTSDB";

    private static List<IndexMode> indexModesUnderTest() {
        List<IndexMode> modes = new ArrayList<>(List.of(IndexMode.TIME_SERIES, IndexMode.STANDARD, IndexMode.LOGSDB));
        modes.add(IndexMode.COLUMNAR);
        modes.add(IndexMode.LOGSDB_COLUMNAR);
        return modes;
    }

    public void testES95SettingAlwaysRegistered() {
        assertTrue(
            "index.time_series.es95_codec.enabled must always be registered",
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS.contains(IndexSettings.TIME_SERIES_ES95_CODEC_ENABLED_SETTING)
        );
    }

    public void testRunTableOrdinalSettingRegisteredOnlyWhenFeatureFlagEnabled() {
        final boolean registered = IndexScopedSettings.BUILT_IN_INDEX_SETTINGS.contains(
            IndexSettings.TIME_SERIES_RUN_TABLE_ORDINAL_ENABLED_SETTING
        );
        assertEquals(
            "run-table ordinal setting registration must match feature flag state",
            IndexSettings.ES95_RUNTABLE_ENCODING_FEATURE_FLAG.isEnabled(),
            registered
        );
    }

    public void testES95OnlySelectedForTimeSeriesWhenSettingEnabled() {
        final IndexVersion version = IndexVersionUtils.randomVersionBetween(
            IndexVersions.ES95_TSDB_CODEC_FEATURE_FLAG,
            IndexVersion.current()
        );
        for (IndexMode mode : indexModesUnderTest()) {
            final DocValuesFormat format = TSDBDocValuesFormatSelector.select(indexSettings(mode, version, true), null);
            if (mode == IndexMode.TIME_SERIES) {
                final boolean runTable = IndexSettings.ES95_RUNTABLE_ENCODING_FEATURE_FLAG.isEnabled()
                    && version.onOrAfter(IndexVersions.TIME_SERIES_RUN_TABLE_ORDINAL_DEFAULT);
                final String expectedName = runTable ? ES95RT_CODEC_NAME : ES95_CODEC_NAME;
                assertThat("mode=" + mode + " version=" + version, format.getName(), equalTo(expectedName));
            } else {
                assertThat("mode=" + mode + " version=" + version, format.getName(), startsWith("ES819"));
            }
        }
    }

    public void testES819SelectedAcrossModesWhenSettingDisabled() {
        for (IndexMode mode : indexModesUnderTest()) {
            final DocValuesFormat format = TSDBDocValuesFormatSelector.select(indexSettings(mode, IndexVersion.current(), false), null);
            assertThat("mode=" + mode, format.getName(), startsWith("ES819"));
        }
    }

    public void testVersionBoundary() {
        final IndexVersion justBefore = IndexVersionUtils.getPreviousVersion(IndexVersions.ES95_TSDB_CODEC_FEATURE_FLAG);
        final IndexVersion es95Exact = IndexVersions.ES95_TSDB_CODEC_FEATURE_FLAG;
        final IndexVersion rtJustBefore = IndexVersionUtils.getPreviousVersion(IndexVersions.TIME_SERIES_RUN_TABLE_ORDINAL_DEFAULT);
        final IndexVersion rtExact = IndexVersions.TIME_SERIES_RUN_TABLE_ORDINAL_DEFAULT;

        assertThat(
            TSDBDocValuesFormatSelector.select(indexSettings(IndexMode.TIME_SERIES, justBefore, true), null).getName(),
            startsWith("ES819")
        );
        assertThat(
            TSDBDocValuesFormatSelector.select(indexSettings(IndexMode.TIME_SERIES, es95Exact, true), null).getName(),
            equalTo(ES95_CODEC_NAME)
        );
        assertThat(
            TSDBDocValuesFormatSelector.select(indexSettings(IndexMode.TIME_SERIES, rtJustBefore, true), null).getName(),
            equalTo(ES95_CODEC_NAME)
        );
        final String expectedAtRt = IndexSettings.ES95_RUNTABLE_ENCODING_FEATURE_FLAG.isEnabled() ? ES95RT_CODEC_NAME : ES95_CODEC_NAME;
        assertThat(
            TSDBDocValuesFormatSelector.select(indexSettings(IndexMode.TIME_SERIES, rtExact, true), null).getName(),
            equalTo(expectedAtRt)
        );
    }

    public void testUseES95RunTableRequiresTimeSeriesMode() {
        assumeTrue("run-table ordinal feature flag must be enabled", IndexSettings.ES95_RUNTABLE_ENCODING_FEATURE_FLAG.isEnabled());
        final IndexVersion version = IndexVersionUtils.randomVersionBetween(
            IndexVersions.TIME_SERIES_RUN_TABLE_ORDINAL_DEFAULT,
            IndexVersion.current()
        );
        assertTrue(TSDBDocValuesFormatSelector.useES95RunTable(indexSettings(IndexMode.TIME_SERIES, version, true)));
        for (IndexMode mode : indexModesUnderTest()) {
            if (mode != IndexMode.TIME_SERIES) {
                assertFalse(
                    "useES95RunTable must return false for mode=" + mode,
                    TSDBDocValuesFormatSelector.useES95RunTable(indexSettings(mode, version, true))
                );
            }
        }
    }

    public void testUseES95RunTableReturnsFalseWhenFeatureFlagDisabled() {
        assumeFalse("run-table ordinal feature flag must be disabled", IndexSettings.ES95_RUNTABLE_ENCODING_FEATURE_FLAG.isEnabled());
        final IndexVersion version = IndexVersionUtils.randomVersionBetween(
            IndexVersions.TIME_SERIES_RUN_TABLE_ORDINAL_DEFAULT,
            IndexVersion.current()
        );
        assertFalse(TSDBDocValuesFormatSelector.useES95RunTable(indexSettings(IndexMode.TIME_SERIES, version, true)));
    }

    private static IndexSettings indexSettings(final IndexMode mode, final IndexVersion version, boolean es95Enabled) {
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
        builder.put(IndexSettings.TIME_SERIES_ES95_CODEC_ENABLED_SETTING.getKey(), es95Enabled);
        final IndexMetadata metadata = IndexMetadata.builder("test").settings(builder).build();
        return new IndexSettings(metadata, Settings.EMPTY);
    }

    public void testDefaultEnabledForTimeSeriesOnOrAfterDefaultVersion() {
        final IndexVersion version = IndexVersionUtils.randomVersionBetween(
            IndexVersions.TIME_SERIES_ES95_CODEC_DEFAULT,
            IndexVersion.current()
        );
        assertTrue(defaultEs95Enabled(IndexMode.TIME_SERIES, version, false));
    }

    public void testDefaultDisabledForTimeSeriesBeforeDefaultVersion() {
        final IndexVersion version = IndexVersionUtils.getPreviousVersion(IndexVersions.TIME_SERIES_ES95_CODEC_DEFAULT);
        assertFalse(defaultEs95Enabled(IndexMode.TIME_SERIES, version, false));
    }

    public void testDefaultEnabledForStatelessTimeSeries() {
        final IndexVersion version = IndexVersionUtils.randomVersionBetween(
            IndexVersions.TIME_SERIES_ES95_CODEC_DEFAULT,
            IndexVersion.current()
        );
        assertTrue(defaultEs95Enabled(IndexMode.TIME_SERIES, version, true));

        final IndexVersion before = IndexVersionUtils.getPreviousVersion(IndexVersions.TIME_SERIES_ES95_CODEC_DEFAULT);
        assertFalse(defaultEs95Enabled(IndexMode.TIME_SERIES, before, true));
    }

    public void testDefaultDisabledForNonTimeSeries() {
        final IndexVersion version = IndexVersion.current();
        assertFalse(defaultEs95Enabled(IndexMode.STANDARD, version, false));
        assertFalse(defaultEs95Enabled(IndexMode.LOGSDB, version, false));
    }

    public void testRunTableOrdinalDefaultEnabledForTimeSeriesWhenFeatureFlagEnabled() {
        assumeTrue("run-table ordinal feature flag must be enabled", IndexSettings.ES95_RUNTABLE_ENCODING_FEATURE_FLAG.isEnabled());
        final IndexVersion version = IndexVersionUtils.randomVersionBetween(
            IndexVersions.ES95_TSDB_CODEC_FEATURE_FLAG,
            IndexVersion.current()
        );
        assertTrue(defaultRunTableOrdinalEnabled(IndexMode.TIME_SERIES, version));
    }

    public void testRunTableOrdinalDefaultDisabledForNonTimeSeries() {
        final IndexVersion version = IndexVersion.current();
        assertFalse(defaultRunTableOrdinalEnabled(IndexMode.STANDARD, version));
        assertFalse(defaultRunTableOrdinalEnabled(IndexMode.LOGSDB, version));
    }

    public void testRunTableOrdinalDefaultDisabledWhenFeatureFlagDisabled() {
        assumeFalse("run-table ordinal feature flag must be disabled", IndexSettings.ES95_RUNTABLE_ENCODING_FEATURE_FLAG.isEnabled());
        final IndexVersion version = IndexVersion.current();
        assertFalse(defaultRunTableOrdinalEnabled(IndexMode.TIME_SERIES, version));
    }

    private static boolean defaultEs95Enabled(final IndexMode mode, final IndexVersion version, boolean stateless) {
        final Settings.Builder indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        if (mode != IndexMode.STANDARD) {
            indexSettings.put("index.mode", mode.getName());
        }
        if (mode == IndexMode.TIME_SERIES) {
            indexSettings.put("index.routing_path", "dimension");
        }
        final Settings nodeSettings = stateless
            ? Settings.builder().put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true).build()
            : Settings.EMPTY;
        final IndexMetadata metadata = IndexMetadata.builder("test").settings(indexSettings).build();
        return new IndexSettings(metadata, nodeSettings).isTimeSeriesEs95CodecEnabled();
    }

    private static boolean defaultRunTableOrdinalEnabled(final IndexMode mode, final IndexVersion version) {
        final Settings.Builder indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        if (mode != IndexMode.STANDARD) {
            indexSettings.put("index.mode", mode.getName());
        }
        if (mode == IndexMode.TIME_SERIES) {
            indexSettings.put("index.routing_path", "dimension");
            indexSettings.put(IndexSettings.TIME_SERIES_ES95_CODEC_ENABLED_SETTING.getKey(), true);
        }
        final IndexMetadata metadata = IndexMetadata.builder("test").settings(indexSettings).build();
        return new IndexSettings(metadata, Settings.EMPTY).isTimeSeriesRunTableOrdinalEnabled();
    }
}
