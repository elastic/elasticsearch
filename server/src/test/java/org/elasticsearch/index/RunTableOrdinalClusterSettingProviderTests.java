/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RunTableOrdinalClusterSettingProviderTests extends ESTestCase {

    private static final String CLUSTER_KEY = RunTableOrdinalClusterSettingProvider.TIME_SERIES_RUN_TABLE_ORDINAL_CLUSTER_ENABLED_SETTING
        .getKey();
    private static final String INDEX_KEY = IndexSettings.TIME_SERIES_RUN_TABLE_ORDINAL_ENABLED_SETTING.getKey();

    public void testEnabledByDefaultInjectsNothing() {
        final RunTableOrdinalClusterSettingProvider provider = provider(Settings.EMPTY);
        assertThat(inject(provider, IndexMode.TIME_SERIES, Settings.EMPTY).isEmpty(), is(true));
    }

    public void testDisabledForcesTimeSeriesSettingOff() {
        assumeTrue("run-table ordinal feature flag must be enabled", IndexSettings.ES95_RUNTABLE_ENCODING_FEATURE_FLAG.isEnabled());
        final RunTableOrdinalClusterSettingProvider provider = provider(clusterSetting(false));
        assertThat(inject(provider, IndexMode.TIME_SERIES, Settings.EMPTY).get(INDEX_KEY), equalTo("false"));
    }

    public void testDisabledForcesOffEvenWhenIndexOptsIn() {
        assumeTrue("run-table ordinal feature flag must be enabled", IndexSettings.ES95_RUNTABLE_ENCODING_FEATURE_FLAG.isEnabled());
        final RunTableOrdinalClusterSettingProvider provider = provider(clusterSetting(false));
        final Settings indexOptIn = Settings.builder().put(INDEX_KEY, true).build();
        assertThat(inject(provider, IndexMode.TIME_SERIES, indexOptIn).get(INDEX_KEY), equalTo("false"));
    }

    public void testDisabledIgnoresNonTimeSeries() {
        final RunTableOrdinalClusterSettingProvider provider = provider(clusterSetting(false));
        for (final IndexMode mode : IndexMode.values()) {
            if (mode == IndexMode.TIME_SERIES) {
                continue;
            }
            assertThat("mode=" + mode, inject(provider, mode, Settings.EMPTY).isEmpty(), is(true));
        }
    }

    public void testModeResolvedFromIndexSettingsWhenTemplateModeMissing() {
        assumeTrue("run-table ordinal feature flag must be enabled", IndexSettings.ES95_RUNTABLE_ENCODING_FEATURE_FLAG.isEnabled());
        final RunTableOrdinalClusterSettingProvider provider = provider(clusterSetting(false));
        final Settings timeSeriesBySetting = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName()).build();
        assertThat(inject(provider, null, timeSeriesBySetting).get(INDEX_KEY), equalTo("false"));
    }

    public void testDynamicUpdateThroughClusterSettings() {
        assumeTrue("run-table ordinal feature flag must be enabled", IndexSettings.ES95_RUNTABLE_ENCODING_FEATURE_FLAG.isEnabled());
        final ClusterSettings clusterSettings = clusterSettings(Settings.EMPTY);
        final RunTableOrdinalClusterSettingProvider provider = new RunTableOrdinalClusterSettingProvider(clusterSettings);
        assertThat(inject(provider, IndexMode.TIME_SERIES, Settings.EMPTY).isEmpty(), is(true));

        clusterSettings.applySettings(clusterSetting(false));
        assertThat(inject(provider, IndexMode.TIME_SERIES, Settings.EMPTY).get(INDEX_KEY), equalTo("false"));

        clusterSettings.applySettings(clusterSetting(true));
        assertThat(inject(provider, IndexMode.TIME_SERIES, Settings.EMPTY).isEmpty(), is(true));
    }

    private static ClusterSettings clusterSettings(final Settings nodeSettings) {
        return new ClusterSettings(
            nodeSettings,
            Set.of(RunTableOrdinalClusterSettingProvider.TIME_SERIES_RUN_TABLE_ORDINAL_CLUSTER_ENABLED_SETTING)
        );
    }

    private static RunTableOrdinalClusterSettingProvider provider(final Settings nodeSettings) {
        return new RunTableOrdinalClusterSettingProvider(clusterSettings(nodeSettings));
    }

    private static Settings clusterSetting(boolean enabled) {
        return Settings.builder().put(CLUSTER_KEY, enabled).build();
    }

    private static Settings inject(
        final RunTableOrdinalClusterSettingProvider provider,
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
