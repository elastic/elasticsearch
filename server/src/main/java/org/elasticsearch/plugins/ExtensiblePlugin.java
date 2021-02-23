/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import java.util.List;

/**
 * An extension point for {@link Plugin} implementations to be themselves extensible.
 *
 * This class provides a callback for extensible plugins to be informed of other plugins
 * which extend them.
 */
public interface ExtensiblePlugin {

    interface ExtensionLoader {
        /**
         * Load extensions of the type from all extending plugins. The concrete extensions must have either a no-arg constructor
         * or a single-arg constructor accepting the specific plugin class.
         * @param extensionPointType the extension point type
         * @param <T> extension point type
         * @return all implementing extensions.
         */
        <T> List<T> loadExtensions(Class<T> extensionPointType);
    }

    /**
     * Allow this plugin to load extensions from other plugins.
     *
     * This method is called once only, after initializing this plugin and all plugins extending this plugin. It is called before
     * any other methods on this Plugin instance are called.
     */
    default void loadExtensions(ExtensionLoader loader) {}
}
