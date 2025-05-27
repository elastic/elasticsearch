/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.plugins.Plugin;

import java.util.List;
import java.util.ServiceLoader;

/**
 * An SPI interface for registering Settings.
 *
 * This is an alternative to {@link Plugin#getSettings()} allowing
 * any Java module to include registered settings.
 */
public interface SettingsExtension {
    List<Setting<?>> getSettings();

    /**
     * Loads a single SettingsExtension, or returns {@code null} if none are found.
     */
    static List<SettingsExtension> load() {
        var loader = ServiceLoader.load(SettingsExtension.class);
        return loader.stream().map(p -> p.get()).toList();
    }
}
