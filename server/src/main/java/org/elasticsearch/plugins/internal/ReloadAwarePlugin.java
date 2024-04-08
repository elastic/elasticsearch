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

/**
 * A plugin that may receive a {@link ReloadablePlugin} in order to
 * call its {@link ReloadablePlugin#reload(Settings)} method.
 */
public interface ReloadAwarePlugin {

    /**
     * Provide a callback for reloading plugins
     *
     * <p>This callback is in the form of an implementation of {@link ReloadablePlugin},
     * but the implementation does not need to be a {@link org.elasticsearch.plugins.Plugin},
     * or be registered with {@link org.elasticsearch.plugins.PluginsService}.
     *
     * @param reloadablePlugin A plugin that this plugin may be able to reload
     */
    void setReloadCallback(ReloadablePlugin reloadablePlugin);

}
