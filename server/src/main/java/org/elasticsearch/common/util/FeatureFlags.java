/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

public class FeatureFlags {
    /**
     * Gates the visibility of the remote store to docrep migration.
     */
    public static final String REMOTE_STORE_MIGRATION_EXPERIMENTAL = "opensearch.experimental.feature.remote_store.migration.enabled";

    /**
     * Gates the ability for Searchable Snapshots to read snapshots that are older than the
     * guaranteed backward compatibility for OpenSearch (one prior major version) on a best effort basis.
     */
    public static final String SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY =
        "opensearch.experimental.feature.searchable_snapshot.extended_compatibility.enabled";

    /**
     * Gates the functionality of extensions.
     * Once the feature is ready for production release, this feature flag can be removed.
     */
    public static final String EXTENSIONS = "opensearch.experimental.feature.extensions.enabled";

    /**
     * Gates the functionality of identity.
     */
    public static final String IDENTITY = "opensearch.experimental.feature.identity.enabled";

    /**
     * Gates the functionality of telemetry framework.
     */
    public static final String TELEMETRY = "opensearch.experimental.feature.telemetry.enabled";

    /**
     * Gates the optimization of datetime formatters caching along with change in default datetime formatter.
     */
    public static final String DATETIME_FORMATTER_CACHING = "opensearch.experimental.optimization.datetime_formatter_caching.enabled";

    /**
     * Gates the functionality of remote index having the capability to move across different tiers
     * Once the feature is ready for release, this feature flag can be removed.
     */
    public static final String TIERED_REMOTE_INDEX = "opensearch.experimental.feature.tiered_remote_index.enabled";

    /**
     * Gates the functionality of pluggable cache.
     * Enables OpenSearch to use pluggable caches with respective store names via setting.
     */
    public static final String PLUGGABLE_CACHE = "opensearch.experimental.feature.pluggable.caching.enabled";

    /**
     * Gates the functionality of remote routing table.
     */
    public static final String REMOTE_PUBLICATION_EXPERIMENTAL = "opensearch.experimental.feature.remote_store.publication.enabled";

    public static final Setting<Boolean> REMOTE_STORE_MIGRATION_EXPERIMENTAL_SETTING = Setting.boolSetting(
        REMOTE_STORE_MIGRATION_EXPERIMENTAL,
        false,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> EXTENSIONS_SETTING = Setting.boolSetting(EXTENSIONS, false, Setting.Property.NodeScope);

    public static final Setting<Boolean> IDENTITY_SETTING = Setting.boolSetting(IDENTITY, false, Setting.Property.NodeScope);

    public static final Setting<Boolean> TELEMETRY_SETTING = Setting.boolSetting(TELEMETRY, false, Setting.Property.NodeScope);

    public static final Setting<Boolean> DATETIME_FORMATTER_CACHING_SETTING = Setting.boolSetting(
        DATETIME_FORMATTER_CACHING,
        false,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> TIERED_REMOTE_INDEX_SETTING = Setting.boolSetting(TIERED_REMOTE_INDEX, false, Setting.Property.NodeScope);

    public static final Setting<Boolean> PLUGGABLE_CACHE_SETTING = Setting.boolSetting(PLUGGABLE_CACHE, false, Setting.Property.NodeScope);

    public static final Setting<Boolean> REMOTE_PUBLICATION_EXPERIMENTAL_SETTING = Setting.boolSetting(
        REMOTE_PUBLICATION_EXPERIMENTAL,
        false,
        Setting.Property.NodeScope
    );

    private static final List<Setting<Boolean>> ALL_FEATURE_FLAG_SETTINGS = List.of(
        REMOTE_STORE_MIGRATION_EXPERIMENTAL_SETTING,
        EXTENSIONS_SETTING,
        IDENTITY_SETTING,
        TELEMETRY_SETTING,
        DATETIME_FORMATTER_CACHING_SETTING,
        TIERED_REMOTE_INDEX_SETTING,
        PLUGGABLE_CACHE_SETTING,
        REMOTE_PUBLICATION_EXPERIMENTAL_SETTING
    );
    /**
     * Should store the settings from opensearch.yml.
     */
    private static Settings settings;

    static {
        Settings.Builder settingsBuilder = Settings.builder();
        for (Setting<Boolean> ffSetting : ALL_FEATURE_FLAG_SETTINGS) {
            settingsBuilder = settingsBuilder.put(ffSetting.getKey(), ffSetting.getDefault(Settings.EMPTY));
        }
        settings = settingsBuilder.build();
    }

    /**
     * This method is responsible to map settings from opensearch.yml to local stored
     * settings value. That is used for the existing isEnabled method.
     *
     * @param openSearchSettings The settings stored in opensearch.yml.
     */
    public static void initializeFeatureFlags(Settings openSearchSettings) {
        Settings.Builder settingsBuilder = Settings.builder();
        for (Setting<Boolean> ffSetting : ALL_FEATURE_FLAG_SETTINGS) {
            settingsBuilder = settingsBuilder.put(
                ffSetting.getKey(),
                openSearchSettings.getAsBoolean(ffSetting.getKey(), ffSetting.getDefault(openSearchSettings))
            );
        }
        settings = settingsBuilder.build();
    }

    /**
     * Used to test feature flags whose values are expected to be booleans.
     * This method returns true if the value is "true" (case-insensitive),
     * and false otherwise.
     */
    public static boolean isEnabled(String featureFlagName) {
        if ("true".equalsIgnoreCase(System.getProperty(featureFlagName))) {
            // TODO: Remove the if condition once FeatureFlags are only supported via opensearch.yml
            return true;
        }
        return settings != null && settings.getAsBoolean(featureFlagName, false);
    }

    public static boolean isEnabled(Setting<Boolean> featureFlag) {
        if ("true".equalsIgnoreCase(System.getProperty(featureFlag.getKey()))) {
            // TODO: Remove the if condition once FeatureFlags are only supported via opensearch.yml
            return true;
        } else if (settings != null) {
            return featureFlag.get(settings);
        } else {
            return featureFlag.getDefault(Settings.EMPTY);
        }
    }
}
