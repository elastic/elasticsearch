/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.core.FixForMultiProject;

import java.util.Set;

public class ProjectScopedSettings extends AbstractScopedSettings {
    public ProjectScopedSettings(Settings settings, Set<Setting<?>> settingsSet) {
        super(settings, settingsSet, Setting.Property.ProjectScope);
    }

    @FixForMultiProject
    @Override
    public <T> T get(Setting<T> setting) {
        throw new UnsupportedOperationException("Not implemented for project scoped settings");
    }
}
