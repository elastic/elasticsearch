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
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.time.Instant;
import java.util.List;
import java.util.Set;

public final class HashInIdRoutingSettingProvider implements IndexSettingProvider {

    private static final Set<String> AFFECTED_SETTINGS = Set.of(IndexMetadata.INDEX_ROUTING_HASH_IN_ID.getKey());

    public static final Setting<Boolean> HASH_IN_ID_ROUTING_ENABLED_SETTING = Setting.boolSetting(
        "cluster.routing.hash_in_id.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile boolean enabled;

    public HashInIdRoutingSettingProvider(final ClusterSettings clusterSettings) {
        this.enabled = clusterSettings.get(HASH_IN_ID_ROUTING_ENABLED_SETTING);
        clusterSettings.addSettingsUpdateConsumer(HASH_IN_ID_ROUTING_ENABLED_SETTING, value -> this.enabled = value);
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
        if (enabled
            && (dataStreamName != null || indexName.startsWith(".") == false)
            && IndexMetadata.INDEX_ROUTING_HASH_IN_ID.exists(indexTemplateAndCreateRequestSettings) == false) {
            additionalSettings.put(IndexMetadata.INDEX_ROUTING_HASH_IN_ID.getKey(), true);
        }
    }

    @Override
    public Set<String> affectedSettings() {
        return AFFECTED_SETTINGS;
    }
}
