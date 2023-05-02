/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.stateless;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ReloadablePlugin;

import java.util.List;

/**
 * A plugin that may receive a list of {@link ReloadablePlugin}s in order to
 * call their {@link ReloadablePlugin#reload(Settings)} method.
 */
public interface ReloadingPlugin {

    /**
     * Provide a list of reloadable plugins.
     * @param reloadablePlugins A list of plugins that this plugin may be able to reload
     */
    void setReloadablePlugins(List<ReloadablePlugin> reloadablePlugins);
}
