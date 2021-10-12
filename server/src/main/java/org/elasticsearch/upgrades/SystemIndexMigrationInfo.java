/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

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

class SystemIndexMigrationInfo implements Comparable<SystemIndexMigrationInfo> {

    private final IndexMetadata currentIndex;
    private final String featureName;
    private final Settings settings;
    private final String mapping;
    private final String origin;
    private final boolean isPrimaryIndex;

    private static final Comparator<SystemIndexMigrationInfo> SAME_CLASS_COMPARATOR = Comparator.comparing(
        SystemIndexMigrationInfo::getFeatureName
    ).thenComparing(SystemIndexMigrationInfo::getCurrentIndexName);

    private SystemIndexMigrationInfo(
        IndexMetadata currentIndex,
        String featureName,
        Settings settings,
        String mapping,
        String origin,
        boolean isPrimaryIndex,
        SystemIndices.Feature owningFeature
    ) {
        this.currentIndex = currentIndex;
        this.featureName = featureName;
        this.settings = settings;
        this.mapping = mapping;
        this.origin = origin;
        this.isPrimaryIndex = isPrimaryIndex;
    }

    String getCurrentIndexName() {
        return currentIndex.getIndex().getName();
    }

    boolean isCurrentIndexClosed() {
        return CLOSE.equals(currentIndex.getState());
    }

    String getNextIndexName() {
        return currentIndex.getIndex().getName() + SystemIndices.UPGRADED_INDEX_SUFFIX;
    }

    String getFeatureName() {
        return featureName;
    }

    String getMappings() {
        return mapping;
    }

    Settings getSettings() {
        return settings;
    }

    String getOrigin() {
        return origin;
    }

    boolean isPrimaryIndex() {
        return isPrimaryIndex;
    }

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
            + '\''
            + ", isPrimaryIndex="
            + isPrimaryIndex
            + ']';
    }

    static boolean isPrimaryIndex(IndexMetadata index, SystemIndexDescriptor descriptor) {
        final String currentIndexName = index.getIndex().getName();
        final String primaryIndexName = descriptor.getPrimaryIndex();
        return primaryIndexName.equals(currentIndexName) || index.getAliases().containsKey(primaryIndexName);
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
        boolean isPrimaryIndex = isPrimaryIndex(currentIndex, descriptor);
        return new SystemIndexMigrationInfo(
            currentIndex,
            feature.getName(),
            settings,
            mapping,
            descriptor.getOrigin(),
            isPrimaryIndex,
            feature
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
            .forEach(setting -> { newIndexSettings.put(setting.getKey(), currentIndexSettings.get(setting.getKey())); });
        return newIndexSettings.build();
    }

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
        assert descriptor != null : "got null descriptor for index ["
            + taskState.getCurrentIndex()
            + "] owned by feature ["
            + taskState.getCurrentFeature()
            + "]";
        // GWB> Probably don't assert this, this is fine - just need to start from scratch
        assert imd != null : "got null index metadata for index ["
            + taskState.getCurrentIndex()
            + "] owned by feature ["
            + feature.getName()
            + "] via descriptor pattern ["
            + descriptor.getIndexPattern()
            + "]";

        return build(imd, descriptor, feature, indexScopedSettings);
    }
}
