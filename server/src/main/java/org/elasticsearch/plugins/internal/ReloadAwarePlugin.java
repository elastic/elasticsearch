/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ReloadablePlugin;

import java.io.IOException;
import java.util.List;

/**
 * A plugin that may receive a list of {@link ReloadablePlugin}s in order to
 * call their {@link ReloadablePlugin#reload(Settings)} method.
 */
public interface ReloadAwarePlugin {

    /**
     * Provide a list of reloadable plugins.
     * @param reloadablePlugin A plugin that this plugin may be able to reload
     */
    void setReloadablePlugin(ReloadablePlugin reloadablePlugin);

    /**
     * Wrap a group of reloadable plugins into a single reloadable plugin interface
     * @param reloadablePlugins A list of reloadable plugins
     * @return A single ReloadablePlugin that, upon reload, reloads the plugins it wraps
     */
    static ReloadablePlugin wrapPlugins(List<ReloadablePlugin> reloadablePlugins) {
        return settings -> {
            for (ReloadablePlugin plugin : reloadablePlugins) {
                try {
                    plugin.reload(settings);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
