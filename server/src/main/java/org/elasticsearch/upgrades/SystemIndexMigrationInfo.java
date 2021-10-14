/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;

import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.IndexMetadata.State.CLOSE;

/**
 * Holds the data required to migrate a single system index, including metadata from the current index. If necessary, computes the settings
 * and mappings for the "next" index based off of the current one.
 */
class SystemIndexMigrationInfo implements Comparable<SystemIndexMigrationInfo> {
    private static final Logger logger = LogManager.getLogger(SystemIndexMigrationInfo.class);

    private final IndexMetadata currentIndex;
    private final String featureName;
    private final Settings settings;
    private final String mapping;
    private final String origin;

    private static final Comparator<SystemIndexMigrationInfo> SAME_CLASS_COMPARATOR = Comparator.comparing(
        SystemIndexMigrationInfo::getFeatureName
    ).thenComparing(SystemIndexMigrationInfo::getCurrentIndexName);

    private SystemIndexMigrationInfo(IndexMetadata currentIndex, String featureName, Settings settings, String mapping, String origin) {
        this.currentIndex = currentIndex;
        this.featureName = featureName;
        this.settings = settings;
        this.mapping = mapping;
        this.origin = origin;
    }

    /**
     * Gets the name of the index to be migrated.
     */
    String getCurrentIndexName() {
        return currentIndex.getIndex().getName();
    }

    /**
     * Indicates if the index to be migrated is closed.
     */
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

    /**
     * Creates a client that's been configured to be able to properly access the system index to be migrated.
     * @param baseClient The base client to wrap.
     * @return An {@link OriginSettingClient} which uses the origin provided by {@link SystemIndexMigrationInfo#getOrigin()}.
     */
    Client createClient(Client baseClient) {
        return new OriginSettingClient(baseClient, this.getOrigin());
    }

    @Override
    public int compareTo(SystemIndexMigrationInfo o) {
        return SAME_CLASS_COMPARATOR.compare(this, o);
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
        Settings settings = descriptor.getSettings();
        String mapping = descriptor.getMappings();
        if (descriptor.isAutomaticallyManaged() == false) {
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
            descriptor.getOrigin());
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
            .forEach(setting -> { newIndexSettings.put(setting.getKey(), currentIndexSettings.get(setting.getKey())); });
        return newIndexSettings.build();
    }

    /**
     * Convenience factory method holding the logic for creating instances from a Feature object.
     * @param feature The feature that
     * @param metadata The current metadata, as index migration depends on the current state of the clsuter.
     * @param indexScopedSettings
     * @return
     */
    static Stream<SystemIndexMigrationInfo> fromFeature(
        SystemIndices.Feature feature,
        Metadata metadata,
        IndexScopedSettings indexScopedSettings
    ) {
        return feature.getIndexDescriptors()
            .stream()
            .flatMap(descriptor -> descriptor.getMatchingIndices(metadata).stream().map(metadata::index).filter(imd -> {
                assert imd != null : "got null IndexMetadata for index in system index descriptor [" + descriptor.getIndexPattern() + "]";
                return Objects.nonNull(imd);
            }).map(imd -> SystemIndexMigrationInfo.build(imd, descriptor, feature, indexScopedSettings)));
    }

    static SystemIndexMigrationInfo fromTaskState(
        SystemIndexMigrationTaskState taskState,
        SystemIndices systemIndices,
        Metadata metadata,
        IndexScopedSettings indexScopedSettings
    ) {
        SystemIndexDescriptor descriptor = systemIndices.findMatchingDescriptor(taskState.getCurrentIndex());
        SystemIndices.Feature feature = systemIndices.getFeatures().get(taskState.getCurrentFeature());
        IndexMetadata imd = metadata.index(taskState.getCurrentIndex());

        // It's possible for one or both of these to happen if the executing node fails during execution and:
        // 1. The task gets assigned to a node with a different set of plugins installed.
        // 2. The index in question is somehow deleted before we got to it.
        // The first case shouldn't happen, master nodes must have all `SystemIndexPlugins` installed.
        // In the second case, we should just start over.
        if (descriptor == null) {
            String errorMsg = new ParameterizedMessage(
                "couldn't find system index descriptor for index [{}] from feature [{}], which likely means this node is missing a plugin",
                taskState.getCurrentIndex(),
                taskState.getCurrentFeature()
            ).toString();
            logger.warn(errorMsg);
            assert false : errorMsg;
            throw new IllegalStateException(errorMsg);
        }

        if (imd == null) {
            String errorMsg = new ParameterizedMessage(
                "couldn't find index [{}] from feature [{}] with descriptor pattern [{}]",
                taskState.getCurrentIndex(),
                taskState.getCurrentFeature(),
                descriptor.getIndexPattern()
            ).toString();
            logger.warn(errorMsg);
            assert false : errorMsg;
            throw new IllegalStateException(errorMsg);
        }

        return build(imd, descriptor, feature, indexScopedSettings);
    }
}
