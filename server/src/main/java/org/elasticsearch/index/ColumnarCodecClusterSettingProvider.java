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
import org.elasticsearch.index.codec.columnar.ColumnarDocValuesFormatSelector;

import java.time.Instant;
import java.util.List;

/**
 * Cluster wide kill switch for ColumNAR keyword doc values adoption, complementing the per-index
 * {@link IndexSettings#COLUMNAR_CODEC_ENABLED_SETTING}. Setting {@link #COLUMNAR_CODEC_CLUSTER_ENABLED_SETTING}
 * to {@code false} lets an operator stop new indices from adopting the ColumNAR codec across a whole cluster
 * without editing individual indices or templates, for example to react to a problem.
 *
 * <p>Because a doc values codec is fixed per segment when data is written, the kill switch is applied at index
 * creation rather than evaluated live: newly created strict columnar indices inherit {@code false} for their
 * per-index setting, so the change takes effect on the next index creation while existing indices keep the
 * codec they were created with and stay readable. The setting is dynamic, so a change is picked up by the next
 * index creation without a node restart.
 *
 * <p>When disabled, the provider overrules any template or create request value and forces
 * {@link IndexSettings#COLUMNAR_CODEC_ENABLED_SETTING} to {@code false}, so the cluster wide kill switch wins
 * over a per-index opt-in.
 *
 * <p>Like the per-index opt-in, the kill switch is only registered while the {@code columnar_codec} feature flag
 * is enabled, so a release build without the flag does not expose it. Callers gate registration on
 * {@link #isFeatureFlagEnabled()}.
 */
public final class ColumnarCodecClusterSettingProvider implements IndexSettingProvider {

    public static final Setting<Boolean> COLUMNAR_CODEC_CLUSTER_ENABLED_SETTING = Setting.boolSetting(
        "cluster.columnar_codec.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile boolean enabled;

    public ColumnarCodecClusterSettingProvider(final ClusterSettings clusterSettings) {
        this.enabled = clusterSettings.get(COLUMNAR_CODEC_CLUSTER_ENABLED_SETTING);
        clusterSettings.addSettingsUpdateConsumer(COLUMNAR_CODEC_CLUSTER_ENABLED_SETTING, value -> this.enabled = value);
    }

    /**
     * @return {@code true} if the {@code columnar_codec} feature flag is enabled. The kill switch and this provider
     *         are only registered while the flag is enabled, mirroring the gating of the per-index
     *         {@link IndexSettings#COLUMNAR_CODEC_ENABLED_SETTING}.
     */
    public static boolean isFeatureFlagEnabled() {
        return ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled();
    }

    @Override
    public void provideAdditionalSettings(
        final String indexName,
        final String dataStreamName,
        final IndexMode templateIndexMode,
        final boolean registryInstalledTemplate,
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
        final IndexMode indexMode = indexMode(templateIndexMode, indexTemplateAndCreateRequestSettings);
        if (indexMode != null && indexMode.isStrictColumnar()) {
            additionalSettings.put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), false);
        }
    }

    private static IndexMode indexMode(final IndexMode templateIndexMode, final Settings settings) {
        if (templateIndexMode != null) {
            return templateIndexMode;
        }
        final String modeName = settings.get(IndexSettings.MODE.getKey());
        return modeName == null ? null : IndexMode.fromString(modeName);
    }

    @Override
    public boolean overrulesTemplateAndRequestSettings() {
        return true;
    }
}
