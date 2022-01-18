/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.plugins.loader.ExtendedPluginsClassLoader;

import java.util.List;

// TODO: remove this indirection now that transport client is gone
/**
 * This class exists solely as an intermediate layer to avoid causing PluginsService
 * to load ExtendedPluginsClassLoader when used in the transport client.
 */
class PluginLoaderIndirection {

    static ClassLoader createLoader(ClassLoader parent, List<ClassLoader> extendedLoaders) {
        return ExtendedPluginsClassLoader.create(parent, extendedLoaders);
    }
}
