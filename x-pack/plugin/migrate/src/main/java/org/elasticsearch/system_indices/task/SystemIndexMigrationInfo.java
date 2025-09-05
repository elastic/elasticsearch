/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.plugins.SystemIndexPlugin;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.IndexMetadata.State.CLOSE;

/**
 * Holds the data required to migrate a single system index, including metadata from the current index. If necessary, computes the settings
 * and mappings for the "next" index based off of the current one.
 */
final class SystemIndexMigrationInfo extends SystemResourceMigrationInfo {
    private static final Logger logger = LogManager.getLogger(SystemIndexMigrationInfo.class);

    private final IndexMetadata currentIndex;
    private final String featureName;
    private final Settings settings;
    private final String mapping;
    private final String origin;
    private final String migrationScript;
    private final SystemIndices.Feature owningFeature;
    private final boolean allowsTemplates;

    private SystemIndexMigrationInfo(
        IndexMetadata currentIndex,
        String featureName,
        Settings settings,
        String mapping,
        String origin,
        String migrationScript,
        SystemIndices.Feature owningFeature,
        boolean allowsTemplates
    ) {
        super(featureName, origin, owningFeature);
        this.currentIndex = currentIndex;
        this.featureName = featureName;
        this.settings = settings;
        this.mapping = mapping;
        this.origin = origin;
        this.migrationScript = migrationScript;
        this.owningFeature = owningFeature;
        this.allowsTemplates = allowsTemplates;
    }

    /**
     * Gets the name of the index to be migrated.
     */
    String getCurrentIndexName() {
        return currentIndex.getIndex().getName();
    }

    @Override
    protected String getCurrentResourceName() {
        return getCurrentIndexName();
    }

    @Override
    Stream<IndexMetadata> getIndices(ProjectMetadata metadata) {
        return Stream.of(currentIndex);
    }

    /**
     * Indicates if the index to be migrated is closed.
     */
    @Override
    boolean isCurrentIndexClosed() {
        return CLOSE.equals(currentIndex.getState());
    }

    /**
     * Gets the name to be used for the post-migration index.
     */
    String getNextIndexName() {
        return currentIndex.getIndex().getName() + SystemIndices.UPGRADED_INDEX_SUFFIX;
    }

    /**
     * Gets the name of the feature which owns the index to be migrated.
     */
    String getFeatureName() {
        return featureName;
    }

    /**
     * Gets the mappings to be used for the post-migration index.
     */
    String getMappings() {
        return mapping;
    }

    /**
     * Gets the settings to be used for the post-migration index.
     */
    Settings getSettings() {
        return settings;
    }

    /**
     * Gets the origin that should be used when interacting with this index.
     */
    String getOrigin() {
        return origin;
    }

    String getMigrationScript() {
        return migrationScript;
    }

    /**
     * By default, system indices should not be affected by user defined templates, so this
     * method should return false in almost all cases. At the moment certain Kibana indices use
     * templates, therefore we allow templates to be used on Kibana created system indices until
     * Kibana removes the template use on system index creation.
     */
    boolean allowsTemplates() {
        return allowsTemplates;
    }

    /**
     * Invokes the pre-migration hook for the feature that owns this index.
     * See {@link SystemIndexPlugin#prepareForIndicesMigration(ProjectMetadata, Client, ActionListener)}.
     * @param project The project metadata
     * @param client For performing any update operations necessary to prepare for the upgrade.
     * @param listener Call {@link ActionListener#onResponse(Object)} when preparation for migration is complete.
     */
    void prepareForIndicesMigration(ProjectMetadata project, Client client, ActionListener<Map<String, Object>> listener) {
        owningFeature.getPreMigrationFunction().prepareForIndicesMigration(project, client, listener);
    }

    /**
     * Invokes the post-migration hooks for the feature that owns this index.
     * See {@link SystemIndexPlugin#indicesMigrationComplete(Map, Client, ActionListener)}.
     *
     * @param metadata The metadata that was passed into the listener by the pre-migration hook.
     * @param client For performing any update operations necessary to prepare for the upgrade.
     * @param listener Call {@link ActionListener#onResponse(Object)} when the hook is finished.
     */
    void indicesMigrationComplete(Map<String, Object> metadata, Client client, ActionListener<Boolean> listener) {
        owningFeature.getPostMigrationFunction().indicesMigrationComplete(metadata, client, listener);
    }

    /**
     * Creates a client that's been configured to be able to properly access the system index to be migrated.
     * @param baseClient The base client to wrap.
     * @return An {@link OriginSettingClient} which uses the origin provided by {@link SystemIndexMigrationInfo#getOrigin()}.
     */
    Client createClient(Client baseClient) {
        return new OriginSettingClient(baseClient, this.getOrigin());
    }

    @Override
    public String toString() {
        return "IndexUpgradeInfo["
            + "currentIndex='"
            + currentIndex.getIndex().getName()
            + "\'"
            + ", featureName='"
            + featureName
            + '\''
            + ", settings="
            + settings
            + ", mapping='"
            + mapping
            + '\''
            + ", origin='"
            + origin
            + '\'';
    }

    static SystemIndexMigrationInfo build(
        IndexMetadata currentIndex,
        SystemIndexDescriptor descriptor,
        SystemIndices.Feature feature,
        IndexScopedSettings indexScopedSettings
    ) {
        final Settings settings;
        final String mapping;
        if (descriptor.isAutomaticallyManaged()) {
            Settings.Builder settingsBuilder = Settings.builder();
            settingsBuilder.put(descriptor.getSettings());
            settingsBuilder.remove(IndexMetadata.SETTING_VERSION_CREATED); // Simplifies testing, should never impact real uses.
            settings = settingsBuilder.build();

            mapping = descriptor.getMappings();
        } else {
            // Get Settings from old index
            settings = copySettingsForNewIndex(currentIndex.getSettings(), indexScopedSettings);

            // Copy mapping from the old index
            mapping = currentIndex.mapping().source().string();
        }
        return new SystemIndexMigrationInfo(
            currentIndex,
            feature.getName(),
            settings,
            mapping,
            descriptor.getOrigin(),
            descriptor.getMigrationScript(),
            feature,
            descriptor.allowsTemplates()
        );
    }

    private static Settings copySettingsForNewIndex(Settings currentIndexSettings, IndexScopedSettings indexScopedSettings) {
        Settings.Builder newIndexSettings = Settings.builder();
        currentIndexSettings.keySet()
            .stream()
            .filter(settingKey -> indexScopedSettings.isPrivateSetting(settingKey) == false)
            .map(indexScopedSettings::get)
            .filter(Objects::nonNull)
            .filter(setting -> setting.getProperties().contains(Setting.Property.NotCopyableOnResize) == false)
            .filter(setting -> setting.getProperties().contains(Setting.Property.PrivateIndex) == false)
            .forEach(setting -> {
                newIndexSettings.put(setting.getKey(), currentIndexSettings.get(setting.getKey()));
            });
        return newIndexSettings.build();
    }
}
