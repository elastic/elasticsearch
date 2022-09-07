/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.scanner.impl;

import org.elasticsearch.plugin.scanner.StablePluginRegistry;
import org.elasticsearch.plugin.scanner.spi.StablePluginRegistryProvider;

import java.nio.file.Path;
import java.util.stream.Stream;

public class StablePluginRegistryProviderImpl implements StablePluginRegistryProvider {

    @Override
    public StablePluginRegistry getInstance(Stream<Path> moduleOrClassPath) {
        ExtensiblesRegistry extensiblesRegistry = new ExtensiblesRegistry(moduleOrClassPath);
        return new StablePluginsRegistryImpl(extensiblesRegistry);
    }
}
