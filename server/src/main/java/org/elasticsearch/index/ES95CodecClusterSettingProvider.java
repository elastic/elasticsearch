/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.time.Instant;
import java.util.List;
import java.util.Locale;

/**
 * Cluster wide opt-out for the ES95 TSDB doc values codec, complementing the per-index
 * {@link IndexSettings#TIME_SERIES_ES95_CODEC_ENABLED_SETTING}. Setting
 * {@link #TIME_SERIES_ES95_CODEC_CLUSTER_ENABLED_SETTING} to {@code false} lets an operator disable the codec
 * across a whole cluster without editing individual indices, for example to react to a problem.
 *
 * <p>Because a doc values codec is fixed per segment when data is written, the opt-out is applied at index
 * creation rather than evaluated live: newly created time series indices inherit {@code false} for their
 * per-index setting, so the change takes effect on rollover while existing indices keep the codec they were
 * created with. The setting is dynamic, so a change is picked up by the next index creation without a node
 * restart.
 *
 * <p>When disabled, the provider overrules any template or create request value and forces
 * {@link IndexSettings#TIME_SERIES_ES95_CODEC_ENABLED_SETTING} to {@code false}, so the cluster wide opt-out
 * wins over a per-index opt-in.
 */
public final class ES95CodecClusterSettingProvider implements IndexSettingProvider {

    public static final Setting<Boolean> TIME_SERIES_ES95_CODEC_CLUSTER_ENABLED_SETTING = Setting.boolSetting(
        "cluster.time_series.es95_codec.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile boolean enabled;

    public ES95CodecClusterSettingProvider(final ClusterSettings clusterSettings) {
        this.enabled = clusterSettings.get(TIME_SERIES_ES95_CODEC_CLUSTER_ENABLED_SETTING);
        clusterSettings.addSettingsUpdateConsumer(TIME_SERIES_ES95_CODEC_CLUSTER_ENABLED_SETTING, value -> this.enabled = value);
    }

    @Override
    public void provideAdditionalSettings(
        final String indexName,
        final String dataStreamName,
        final IndexMode templateIndexMode,
        final ProjectMetadata projectMetadata,
        final Instant resolvedAt,
        final Settings indexTemplateAndCreateRequestSettings,
        final List<CompressedXContent> combinedTemplateMappings,
        final IndexVersion indexVersion,
        final Settings.Builder additionalSettings
    ) {
        if (enabled) {
            return;
        }
        if (indexMode(templateIndexMode, indexTemplateAndCreateRequestSettings) == IndexMode.TIME_SERIES) {
            additionalSettings.put(IndexSettings.TIME_SERIES_ES95_CODEC_ENABLED_SETTING.getKey(), false);
        }
    }

    private static IndexMode indexMode(final IndexMode templateIndexMode, final Settings settings) {
        if (templateIndexMode != null) {
            return templateIndexMode;
        }
        final String modeName = settings.get(IndexSettings.MODE.getKey());
        return modeName == null ? null : IndexMode.valueOf(modeName.toUpperCase(Locale.ROOT));
    }

    @Override
    public boolean overrulesTemplateAndRequestSettings() {
        return true;
    }
}
