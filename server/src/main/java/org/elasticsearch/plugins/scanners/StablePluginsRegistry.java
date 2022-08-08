/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.scanners;


import org.elasticsearch.plugins.PluginBundle;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class StablePluginsRegistry {

    private Map<String /*Extensible */, NamedPlugins> namedComponents = new HashMap<>();

    private final ExtensibleScanner extensibleScanner = new ExtensibleScanner();
    private final NamedComponentScanner namedComponentScanner = new NamedComponentScanner();

    public Map<String /*Extensible */, NamedPlugins> scanBundleForStablePlugins(PluginBundle bundle, ClassLoader pluginClassLoader) {
        Map<String,String> extensibleInterfaces = extensibleScanner.getExtensibles();// todo do this in build time, read from file, possibly static?

        return namedComponentScanner.findNamedComponents(bundle, pluginClassLoader, extensibleInterfaces);
    }


    public Map<String, NamedPlugins> getNamedComponents() {
        return namedComponents;
    }

}
