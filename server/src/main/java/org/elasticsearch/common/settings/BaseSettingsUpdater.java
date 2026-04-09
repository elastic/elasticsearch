/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.settings;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.core.Tuple;

import java.util.Map;

import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;

public abstract class BaseSettingsUpdater {
    protected final AbstractScopedSettings scopedSettings;

    public BaseSettingsUpdater(AbstractScopedSettings scopedSettings) {
        this.scopedSettings = scopedSettings;
    }

    private static void logUnknownSetting(final String settingType, final Map.Entry<String, String> e, final Logger logger) {
        logger.warn("ignoring existing unknown {} setting: [{}] with value [{}]; archiving", settingType, e.getKey(), e.getValue());
    }

    private static void logInvalidSetting(
        final String settingType,
        final Map.Entry<String, String> e,
        final IllegalArgumentException ex,
        final Logger logger
    ) {
        logger.warn(
            (Supplier<?>) () -> "ignoring existing invalid "
                + settingType
                + " setting: ["
                + e.getKey()
                + "] with value ["
                + e.getValue()
                + "]; archiving",
            ex
        );
    }

    /**
     * Partitions the settings into those that are known and valid versus those that are unknown or invalid. The resulting tuple contains
     * the known and valid settings in the first component and the unknown or invalid settings in the second component. Note that archived
     * settings contained in the settings to partition are included in the first component.
     *
     * @param settings     the settings to partition
     * @param settingsType a string to identify the settings (for logging)
     * @param logger       a logger to sending warnings to
     * @return the partitioned settings
     */
    protected final Tuple<Settings, Settings> partitionKnownAndValidSettings(
        final Settings settings,
        final String settingsType,
        final Logger logger
    ) {
        final Settings existingArchivedSettings = settings.filter(k -> k.startsWith(ARCHIVED_SETTINGS_PREFIX));
        final Settings settingsExcludingExistingArchivedSettings = settings.filter(k -> k.startsWith(ARCHIVED_SETTINGS_PREFIX) == false);
        final Settings settingsWithUnknownOrInvalidArchived = scopedSettings.archiveUnknownOrInvalidSettings(
            settingsExcludingExistingArchivedSettings,
            e -> BaseSettingsUpdater.logUnknownSetting(settingsType, e, logger),
            (e, ex) -> BaseSettingsUpdater.logInvalidSetting(settingsType, e, ex, logger)
        );
        return Tuple.tuple(
            Settings.builder()
                .put(settingsWithUnknownOrInvalidArchived.filter(k -> k.startsWith(ARCHIVED_SETTINGS_PREFIX) == false))
                .put(existingArchivedSettings)
                .build(),
            settingsWithUnknownOrInvalidArchived.filter(k -> k.startsWith(ARCHIVED_SETTINGS_PREFIX))
        );
    }
}
