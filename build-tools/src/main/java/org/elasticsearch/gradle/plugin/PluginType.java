/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.plugin;

import java.util.Locale;

/**
 * This mirrors org.elasticsearch.plugins.PluginType, which is not
 * available to the Gradle plugin that actually builds plugins. See that
 * class for further information.
 */
public enum PluginType {
    ISOLATED,
    BOOTSTRAP;

    @Override
    public String toString() {
        return this.name().toLowerCase(Locale.ROOT);
    }
}
