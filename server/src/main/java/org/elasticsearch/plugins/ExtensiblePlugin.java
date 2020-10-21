/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
