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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.codec.columnar.ColumnarDocValuesFormatSelector;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ColumnarCodecClusterSettingProviderTests extends ESTestCase {

    private static final String CLUSTER_KEY = ColumnarCodecClusterSettingProvider.COLUMNAR_CODEC_CLUSTER_ENABLED_SETTING.getKey();
    private static final String INDEX_KEY = IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey();

    private static final List<IndexMode> STRICT_COLUMNAR_MODES = List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR);

    public void testEnabledByDefaultInjectsNothing() {
        final ColumnarCodecClusterSettingProvider provider = provider(Settings.EMPTY);
        assertThat(inject(provider, randomStrictColumnarMode(), Settings.EMPTY).isEmpty(), is(true));
    }

    public void testDisabledForcesColumnarSettingOff() {
        final ColumnarCodecClusterSettingProvider provider = provider(clusterSetting(false));
        assertThat(inject(provider, randomStrictColumnarMode(), Settings.EMPTY).get(INDEX_KEY), equalTo("false"));
    }

    public void testDisabledForcesOffEvenWhenIndexOptsIn() {
        final ColumnarCodecClusterSettingProvider provider = provider(clusterSetting(false));
        final Settings indexOptIn = Settings.builder().put(INDEX_KEY, true).build();
        assertThat(inject(provider, randomStrictColumnarMode(), indexOptIn).get(INDEX_KEY), equalTo("false"));
    }

    public void testDisabledIgnoresNonStrictColumnarModes() {
        final ColumnarCodecClusterSettingProvider provider = provider(clusterSetting(false));
        for (final IndexMode mode : IndexMode.values()) {
            if (mode.isStrictColumnar()) {
                continue;
            }
            assertThat("mode=" + mode, inject(provider, mode, Settings.EMPTY).isEmpty(), is(true));
        }
    }

    public void testModeResolvedFromIndexSettingsWhenTemplateModeMissing() {
        final ColumnarCodecClusterSettingProvider provider = provider(clusterSetting(false));
        final IndexMode mode = randomStrictColumnarMode();
        final Settings columnarBySetting = Settings.builder().put(IndexSettings.MODE.getKey(), mode.getName()).build();
        assertThat(inject(provider, null, columnarBySetting).get(INDEX_KEY), equalTo("false"));
    }

    public void testDynamicUpdateThroughClusterSettings() {
        final ClusterSettings clusterSettings = clusterSettings(Settings.EMPTY);
        final ColumnarCodecClusterSettingProvider provider = new ColumnarCodecClusterSettingProvider(clusterSettings);
        final IndexMode mode = randomStrictColumnarMode();
        assertThat(inject(provider, mode, Settings.EMPTY).isEmpty(), is(true));

        clusterSettings.applySettings(clusterSetting(false));
        assertThat(inject(provider, mode, Settings.EMPTY).get(INDEX_KEY), equalTo("false"));

        clusterSettings.applySettings(clusterSetting(true));
        assertThat(inject(provider, mode, Settings.EMPTY).isEmpty(), is(true));
    }

    public void testExistingColumnarIndexStillSelectsColumnarWhenSwitchOff() {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarCodecClusterSettingProvider.isFeatureFlagEnabled());
        final IndexMode mode = randomStrictColumnarMode();
        assertTrue(ColumnarDocValuesFormatSelector.useColumnarCodec(indexSettings(mode, randomEligibleVersion(), true)));
    }

    public void testForcedOffSettingMakesNewIndexIneligible() {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarCodecClusterSettingProvider.isFeatureFlagEnabled());
        final IndexMode mode = randomStrictColumnarMode();
        final ColumnarCodecClusterSettingProvider provider = provider(clusterSetting(false));
        final Settings forced = inject(provider, mode, Settings.builder().put(INDEX_KEY, true).build());
        assertThat(forced.getAsBoolean(INDEX_KEY, true), is(false));
        assertFalse(ColumnarDocValuesFormatSelector.useColumnarCodec(indexSettings(mode, randomEligibleVersion(), false)));
    }

    private IndexMode randomStrictColumnarMode() {
        return randomFrom(STRICT_COLUMNAR_MODES);
    }

    private static IndexVersion randomEligibleVersion() {
        return IndexVersionUtils.randomVersionBetween(IndexVersions.COLUMNAR_DOC_VALUES_CODEC_FEATURE_FLAG, IndexVersion.current());
    }

    private static ClusterSettings clusterSettings(final Settings nodeSettings) {
        return new ClusterSettings(nodeSettings, Set.of(ColumnarCodecClusterSettingProvider.COLUMNAR_CODEC_CLUSTER_ENABLED_SETTING));
    }

    private static ColumnarCodecClusterSettingProvider provider(final Settings nodeSettings) {
        return new ColumnarCodecClusterSettingProvider(clusterSettings(nodeSettings));
    }

    private static Settings clusterSetting(final boolean enabled) {
        return Settings.builder().put(CLUSTER_KEY, enabled).build();
    }

    private static IndexSettings indexSettings(final IndexMode mode, final IndexVersion version, final boolean columnarEnabled) {
        final Settings.Builder builder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.MODE.getKey(), mode.getName())
            .put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), columnarEnabled);
        final IndexMetadata metadata = IndexMetadata.builder("test").settings(builder).build();
        return new IndexSettings(metadata, Settings.EMPTY);
    }

    private static Settings inject(
        final ColumnarCodecClusterSettingProvider provider,
        final IndexMode templateMode,
        final Settings indexSettings
    ) {
        final Settings.Builder additionalSettings = Settings.builder();
        provider.provideAdditionalSettings(
            "test-index",
            null,
            templateMode,
            false,
            null,
            Instant.ofEpochMilli(0L),
            indexSettings,
            List.of(),
            IndexVersion.current(),
            additionalSettings
        );
        return additionalSettings.build();
    }
}
