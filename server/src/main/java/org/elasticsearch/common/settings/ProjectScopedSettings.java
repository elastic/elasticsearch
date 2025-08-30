/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.cluster.metadata.ProjectId;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Encapsulates project level settings and its update listeners.
 */
public class ProjectScopedSettings extends AbstractScopedSettings<ProjectId> {
    private final Map<ProjectId, Settings> projectSettings = new ConcurrentHashMap<>();

    public ProjectScopedSettings(Set<Setting<?>> settingsSet) {
        super(settingsSet, Setting.Property.ProjectScope);
    }

    public ProjectScopedSettings() {
        this(Collections.emptySet());
    }

    /**
     * Retrieves a setting value for a specific project.
     *
     * @param projectId id of the project for which the setting value is to be retrieved
     * @param setting the setting whose value needs to be fetched
     * @return the value of the specified setting for the given project
     */
    public <T> T get(ProjectId projectId, Setting<T> setting) {
        return setting.get(projectSettings.getOrDefault(projectId, Settings.EMPTY));
    }

    /**
     * Validates the given settings for the given project by running it through all update listeners without applying it. This
     * method will not change any settings but will fail if any of the settings can't be applied.
     */
    public void validateUpdate(ProjectId projectId, Settings newSettings) {
        Settings lastSettingsApplied = projectSettings.getOrDefault(projectId, Settings.EMPTY);
        validateUpdate(newSettings, lastSettingsApplied);
    }

    /**
     * Applies the given settings of the project to all the settings consumers or to none of them.
     *
     * @param projectId id of the project
     * @param newSettings the settings to apply
     * @return the unmerged applied settings
     */
    public Settings applySettings(ProjectId projectId, Settings newSettings) {
        return projectSettings.compute(projectId, (id, lastSettingsApplied) -> {
            if (lastSettingsApplied == null) {
                lastSettingsApplied = Settings.EMPTY;
            }
            executeSettingsUpdaters(projectId, newSettings, lastSettingsApplied);
            return newSettings;
        });
    }
}
