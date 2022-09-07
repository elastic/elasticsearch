/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.scanner.impl;

import org.elasticsearch.plugin.scanner.NameToPluginInfo;
import org.elasticsearch.plugin.scanner.StablePluginRegistry;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class StablePluginsRegistryImpl implements StablePluginRegistry {

    /*
    A map of an interface/class name marked (effectively) with @Extensible to NameToPluginInfo map
    effectively means that an interface which extends another interface marked with @Extensible is also extensible
    NameToPluginInfo map is a map of Name to PluginInfo
    i.e.
    org.elasticsearch.plugin.analysis.api.TokenFilterFactory ->
        {"nori" -> {nori, org.elasticserach.plugin.analysis.new_nori.NoriReadingFormFilterFactory, classloaderInstance}
     */
    private final Map<String /*Extensible */, NameToPluginInfo> namedComponents;
    private final NamedComponentScanner namedComponentsScanner;

    public StablePluginsRegistryImpl(ExtensiblesRegistry extensiblesRegistry) {
        this(new NamedComponentScanner(extensiblesRegistry), new HashMap<>());
    }

    // for testing
    StablePluginsRegistryImpl(NamedComponentScanner namedComponentScanner, HashMap<String /*Extensible */, NameToPluginInfo> namedComponents) {
        this.namedComponentsScanner = namedComponentScanner;
        this.namedComponents = namedComponents;
    }

    @Override
    public void scanBundleForStablePlugins(Set<URL> pluginsUrls, ClassLoader pluginClassLoader) {
        Map<String, NameToPluginInfo> namedComponentsFromPlugin =
            namedComponentsScanner.findNamedComponents(pluginsUrls, pluginClassLoader);
        for (Map.Entry<String, NameToPluginInfo> entry : namedComponentsFromPlugin.entrySet()) {
            if (namedComponents.containsKey(entry.getKey())) {
                NameToPluginInfo nameToPluginInfo = namedComponents.get(entry.getKey());
                nameToPluginInfo.put(entry.getValue());
            } else {
                namedComponents.put(entry.getKey(), entry.getValue());
            }
        }
    }

    // TODO this will be removed. getPluginForName or similar wil be created
    @Override
    public Map<String, NameToPluginInfo> getNamedComponents() {
        return namedComponents;
    }

}
